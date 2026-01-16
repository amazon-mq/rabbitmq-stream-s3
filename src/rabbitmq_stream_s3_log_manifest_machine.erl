%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_manifest_machine).
-moduledoc """
The "functional core" of the log manifest.

This module contains purely functional logic which handles events like
a new fragment becoming available, or the commit offset moving forward. These
events are applied to the state record and `apply/3` returns a list of effects
for the log manifest server to execute.
""".

-compile({no_auto_import, [apply/2, apply/3]}).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

%% NOTE: Pending is reversed.
-type upload_status() ::
    {uploading, Pending :: [#fragment_info{}]}
    | {last_uploaded, non_neg_integer() | infinity}.

-record(stream, {
    %% PID of the osiris_writer process. Used to attach offset listeners.
    writer_pid :: pid() | undefined,
    replica_nodes = [] :: [node()],
    %% Directory of the local copy of the log.
    %% This could be undefined for a stream until a writer or acceptor starts
    %% on this node. This field is used to add information to effects like
    %% `upload_fragment{}' which act on local files.
    dir :: directory() | undefined,
    %% Most up-to-date known copy of the manifest, if there is one, or a list
    %% of callers waiting for it to be resolved.
    manifest = {pending, []} :: #manifest{} | {pending, [gen_server:from()]},
    upload_status = {last_uploaded, 0} :: upload_status(),
    %% Current commit offset (updated by offset listener notifications) known
    %% to the manifest - this can lag behind the actual commit offset.
    commit_offset = -1 :: osiris:offset() | -1,
    %% List of segments in ascending offset order which have been rolled and
    %% are awaiting upload.
    available_fragments = [] :: [#fragment{}],
    %% List of fragments in ascending offset order which have been uploaded
    %% successfully but have not yet been applied to the manifest.
    uploaded_fragments = [] :: [#fragment_info{}]
}).

-record(?MODULE, {
    streams = #{} :: #{stream_id() => #stream{}}
}).

-type metadata() :: #{
    %% Warp-safe native time, i.e. `erlang:monotonic_time/0`
    time := integer()
}.

-opaque state() :: #?MODULE{}.

-export_type([metadata/0, state/0]).

-export([new/0, get_manifest/2, apply/3]).

-doc """
Create a default, empty machine state.
""".
-spec new() -> state().
new() -> #?MODULE{}.

-spec get_manifest(StreamId :: stream_id(), state()) -> #manifest{} | undefined.
get_manifest(StreamId, #?MODULE{streams = Streams}) ->
    case Streams of
        #{StreamId := #stream{manifest = #manifest{} = M}} ->
            M;
        _ ->
            undefined
    end.

-doc """
Apply an event to the state, evolving the state and returning a list of events
to execute.
""".
-spec apply(metadata(), event(), state()) -> {state(), [effect()]}.

apply(
    _Meta,
    #fragment_available{stream = StreamId, fragment = Fragment},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #stream{available_fragments = Fragments0} = Stream0} ->
            Fragments = add_available_fragment(Fragment, Fragments0),
            Stream = Stream0#stream{available_fragments = Fragments},
            State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
            {State, []};
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #commit_offset_increased{stream = StreamId, offset = Offset},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{
            StreamId := #stream{
                writer_pid = Pid,
                dir = Dir,
                available_fragments = Available,
                manifest = Manifest
            } = Stream0
        } ->
            Effects0 = [#register_offset_listener{writer_pid = Pid, offset = Offset + 1}],
            case Manifest of
                {pending, _Requesters} ->
                    %% Wait until the manifest is resolved to upload fragments
                    %% so that we avoid uploading something which already
                    %% exists in the remote tier.
                    Stream = Stream0#stream{commit_offset = Offset},
                    {State0#?MODULE{streams = Streams0#{StreamId := Stream}}, Effects0};
                _ ->
                    {Uncommitted, Committed} = lists:splitwith(
                        fun(#fragment{last_offset = LastOffset}) -> LastOffset > Offset end,
                        Available
                    ),
                    Effects = lists:foldl(
                        fun(Fragment, Acc) ->
                            [
                                #upload_fragment{
                                    stream = StreamId,
                                    dir = Dir,
                                    fragment = Fragment
                                }
                                | Acc
                            ]
                        end,
                        Effects0,
                        Committed
                    ),
                    Stream = Stream0#stream{
                        commit_offset = Offset,
                        available_fragments = Uncommitted
                    },
                    Streams = Streams0#{StreamId := Stream},
                    State = State0#?MODULE{streams = Streams},
                    {State, Effects}
            end;
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #fragment_uploaded{stream = StreamId, info = #fragment_info{} = Info},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{
            StreamId := #stream{
                manifest = #manifest{next_offset = NTO0} = Manifest0,
                uploaded_fragments = Uploaded0
            } = Stream0
        } ->
            Uploaded1 = insert_info(Info, Uploaded0),
            {NTO, Pending, Finished} = split_uploaded_infos(NTO0, Uploaded1),
            %% assertion: the manifest can't be pending download here.
            #manifest{} = Manifest0,
            Stream1 = Stream0#stream{uploaded_fragments = Pending},
            {Stream, Effects0} = apply_infos_to_manifest(Finished, StreamId, Stream1),
            %% assertion: the finished fragments were applied up to the
            %% next-tiered-offset we expected from split_uploaded_infos/2.
            #stream{manifest = #manifest{next_offset = NTO}} = Stream,
            Effects =
                case NTO of
                    NTO0 ->
                        Effects0;
                    _ ->
                        [
                            #set_last_tiered_offset{
                                writer_pid = self(),
                                stream = StreamId,
                                offset = NTO - 1
                            }
                            | Effects0
                        ]
                end,
            State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(_Meta, #manifest_uploaded{stream = StreamId}, #?MODULE{streams = Streams0} = State0) ->
    case Streams0 of
        #{StreamId := #stream{upload_status = UploadStatus0} = Stream0} ->
            %% assertion
            {uploading, Pending0} = UploadStatus0,
            %% Pending is stored reversed for quick prepends.
            Pending = lists:reverse(Pending0),
            Stream1 = Stream0#stream{upload_status = {last_uploaded, 0}},
            {Stream, Effects} = apply_infos_to_manifest(Pending, StreamId, Stream1),
            State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #manifest_rebalanced{stream = StreamId, manifest = Manifest0},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #stream{upload_status = UploadStatus0} = Stream0} ->
            %% assertion
            {uploading, Pending0} = UploadStatus0,
            %% Pending is stored reversed for quick prepends.
            Pending = lists:reverse(Pending0),
            Stream1 = Stream0#stream{
                manifest = Manifest0,
                upload_status = {last_uploaded, infinity}
            },
            {Stream, Effects} = apply_infos_to_manifest(Pending, StreamId, Stream1),
            State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #writer_spawned{pid = Pid, stream = StreamId, dir = Dir, replica_nodes = ReplicaNodes},
    #?MODULE{streams = Streams0} = State0
) ->
    Effects0 = [#register_offset_listener{writer_pid = Pid, offset = -1}],
    case Streams0 of
        #{StreamId := #stream{} = Stream0} ->
            Stream = Stream0#stream{writer_pid = Pid, dir = Dir, replica_nodes = ReplicaNodes},
            Streams = Streams0#{StreamId := Stream},
            {State0#?MODULE{streams = Streams}, Effects0};
        _ ->
            Stream = #stream{writer_pid = Pid, dir = Dir, replica_nodes = ReplicaNodes},
            Streams = Streams0#{StreamId => Stream},
            Effects = [#resolve_manifest{stream = StreamId} | Effects0],
            {State0#?MODULE{streams = Streams}, Effects}
    end;
apply(
    _Meta,
    #manifest_requested{stream = StreamId, requester = Requester},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #stream{manifest = #manifest{} = Manifest}} ->
            {State0, [#reply{to = Requester, response = Manifest}]};
        #{StreamId := #stream{manifest = {pending, Requesters0}} = Stream0} ->
            Stream = Stream0#stream{manifest = {pending, [Requester | Requesters0]}},
            State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
            {State, []};
        _ ->
            Stream = #stream{manifest = {pending, [Requester]}},
            State = State0#?MODULE{streams = Streams0#{StreamId => Stream}},
            {State, [#resolve_manifest{stream = StreamId}]}
    end;
apply(
    _Meta,
    #manifest_resolved{
        stream = StreamId,
        manifest = #manifest{entries = Entries} = Manifest
    },
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{
            StreamId := #stream{
                dir = Dir,
                manifest = {pending, Requesters},
                commit_offset = CommitOffset,
                available_fragments = Available0
            } = Stream0
        } ->
            %% NOTE: `Requesters` is in reverse order.
            Effects0 = lists:foldl(
                fun(R, Acc) -> [#reply{to = R, response = Manifest} | Acc] end,
                [],
                Requesters
            ),
            LastOffsetInManifest =
                case rabbitmq_stream_s3_binary_array:last(?ENTRY_B, Entries) of
                    ?ENTRY(O, _, _, _, _, _) ->
                        O;
                    undefined ->
                        -1
                end,
            Available = lists:filter(
                fun(#fragment{first_offset = Offset}) ->
                    Offset > LastOffsetInManifest
                end,
                Available0
            ),
            {Uncommitted, Committed} = lists:splitwith(
                fun(#fragment{last_offset = Offset}) ->
                    Offset > CommitOffset
                end,
                Available
            ),
            Effects = lists:foldl(
                fun(Fragment, Acc) ->
                    [
                        #upload_fragment{
                            stream = StreamId,
                            dir = Dir,
                            fragment = Fragment
                        }
                        | Acc
                    ]
                end,
                Effects0,
                Committed
            ),
            Stream = Stream0#stream{
                manifest = Manifest,
                upload_status = {last_uploaded, 0},
                available_fragments = Uncommitted
            },
            State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(_Meta, Event, State) ->
    ?LOG_WARNING(?MODULE_STRING " dropped unknown event ~W", [Event, 15]),
    {State, []}.

%%----------------------------------------------------------------------------

-doc """
Insert the new fragment `Info` into the list of existing `Infos`.
`Infos` is sorted by offset ascending. This insertion preserves the ordering.
""".
-spec insert_info(#fragment_info{}, [#fragment_info{}]) -> [#fragment_info{}].
insert_info(Info, Infos) ->
    insert_info(Info, Infos, []).

insert_info(
    #fragment_info{offset = InfoOffset} = Info,
    [#fragment_info{offset = HeadOffset} = Head | Rest],
    Acc
) when InfoOffset > HeadOffset ->
    insert_info(Info, Rest, [Head | Acc]);
insert_info(Info, Infos, Acc) ->
    lists:reverse(Acc, [Info | Infos]).

-doc """
Splits the list of uploaded infos into 'pending' and 'finished'.

Uploads may complete out of order. Infos are queued up until a sequential run
of infos has been fully uploaded. Those finished infos can then be applied to
the manifest, and the pending infos should be saved so they can be reconsidered
when the next fragment is uploaded.
""".
-spec split_uploaded_infos(NextTieredOffset, UploadedInfos) ->
    {NewNextTieredOffset, PendingInfos, FinishedInfos}
when
    NextTieredOffset :: osiris:offset(),
    NewNextTieredOffset :: osiris:offset(),
    UploadedInfos :: [#fragment_info{}],
    PendingInfos :: [#fragment_info{}],
    FinishedInfos :: [#fragment_info{}].
split_uploaded_infos(NextTieredOffset, UploadedInfos) ->
    split_uploaded_infos(NextTieredOffset, UploadedInfos, []).

split_uploaded_infos(
    NextTieredOffset,
    [#fragment_info{offset = FirstOffset, next_offset = NextOffset} = Info | Rest],
    Acc
) when NextTieredOffset =:= FirstOffset ->
    split_uploaded_infos(NextOffset, Rest, [Info | Acc]);
split_uploaded_infos(NextTieredOffset, PendingUploaded, Acc) ->
    {NextTieredOffset, PendingUploaded, lists:reverse(Acc)}.

-doc """
Apply successfully uploaded fragments to their stream's manifest.

This function also evaluates whether the manifest should be rebalanced and/or
uploaded to the remote tier.
""".
-spec apply_infos_to_manifest([#fragment_info{}], stream_id(), #stream{}) ->
    {#stream{}, [effect()]}.
apply_infos_to_manifest(Infos, StreamId, Stream) ->
    apply_infos_to_manifest(Infos, StreamId, Stream, []).

apply_infos_to_manifest(
    [],
    StreamId,
    #stream{
        manifest = #manifest{entries = Entries} = Manifest,
        upload_status = {last_uploaded, _} = UploadStatus0
    } = Stream0,
    Effects0
) when
    ?ENTRIES_LEN(Entries) >= 2 * ?MANIFEST_BRANCHING_FACTOR
->
    %% The manifest is loaded. Try to rebalance away a group. TODO see if we
    %% can improve this "load factor." It's pretty simple at the moment.
    case rabbitmq_stream_s3_log_manifest_entry:rebalance(Entries) of
        undefined ->
            ?LOG_DEBUG("Manifest is loaded but rebalancing is not possible.", []),
            {Manifest, UploadStatus0, Effects0};
        {GroupKind, GroupSize, Group, Rebalanced} ->
            ?LOG_DEBUG("Compacting away ~b kind ~b's from entries of byte size ~b", [
                ?MANIFEST_BRANCHING_FACTOR, GroupKind, byte_size(Entries)
            ]),
            Rebalance = #rebalance_manifest{
                stream = StreamId,
                kind = GroupKind,
                size = GroupSize,
                new_group = Group,
                rebalanced = Rebalanced,
                manifest = Manifest
            },
            Stream = Stream0#stream{
                manifest = Manifest,
                upload_status = {uploading, []}
            },
            {Stream, [Rebalance | Effects0]}
    end;
apply_infos_to_manifest(
    [],
    StreamId,
    #stream{
        manifest = Manifest,
        upload_status = {last_uploaded, NumUpdates}
    } = Stream0,
    Effects0
) when
    NumUpdates >= ?FRAGMENT_UPLOADS_PER_MANIFEST_UPDATE
->
    %% Updates have been debounced but there have been enough that now it is
    %% time to perform the upload.
    case NumUpdates of
        infinity ->
            ?LOG_DEBUG("Forcing upload of manifest");
        _ when is_integer(NumUpdates) ->
            ?LOG_DEBUG("Uploading manifest because there have been ~b updates since last upload", [
                NumUpdates
            ])
    end,
    Stream = Stream0#stream{upload_status = {uploading, []}},
    Upload = #upload_manifest{stream = StreamId, manifest = Manifest},
    {Stream, [Upload | Effects0]};
apply_infos_to_manifest([], _StreamId, #stream{upload_status = UploadStatus} = Stream, Effects) ->
    %% The manifest is currently being uploaded, or there are no updates
    %% necessary. Skip the upload.
    ?LOG_DEBUG("Skipping upload of manifest with status ~w", [UploadStatus]),
    {Stream, Effects};
apply_infos_to_manifest(
    [
        #fragment_info{
            offset = Offset,
            timestamp = Ts,
            next_offset = NextOffset,
            seq_no = SeqNo,
            size = Size
        }
        | Rest
    ],
    StreamId,
    #stream{manifest = #manifest{next_offset = 0}, upload_status = UploadStatus0} = Stream0,
    Effects
) ->
    ?assertEqual({last_uploaded, 0}, UploadStatus0),
    %% The very first fragment in the manifest. Create a new manifest.
    Manifest = #manifest{
        first_offset = Offset,
        first_timestamp = Ts,
        next_offset = NextOffset,
        total_size = Size,
        entries = ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>)
    },
    %% And force its upload.
    Stream = Stream0#stream{
        manifest = Manifest,
        upload_status = {last_uploaded, infinity}
    },
    apply_infos_to_manifest(Rest, StreamId, Stream, Effects);
apply_infos_to_manifest(
    [Fragment | Rest],
    StreamId,
    #stream{upload_status = {uploading, Pending0}} = Stream0,
    Effects
) ->
    %% The manifest is currently being uploaded. Queue the fragment for later
    %% application once the current upload completes.
    Stream = Stream0#stream{upload_status = {uploading, [Fragment | Pending0]}},
    apply_infos_to_manifest(Rest, StreamId, Stream, Effects);
apply_infos_to_manifest(
    [
        #fragment_info{
            offset = Offset,
            next_offset = NextOffset,
            timestamp = Ts,
            seq_no = SeqNo,
            size = Size
        }
        | Rest
    ],
    StreamId,
    #stream{
        manifest = #manifest{total_size = TotalSize0, entries = Entries0} = Manifest0,
        upload_status = {last_uploaded, NumUpdates0}
    } = Stream0,
    Effects
) ->
    %% Common case: the manifest exists. Append the fragment to the entries.
    NumUpdates =
        case NumUpdates0 of
            infinity ->
                infinity;
            N when is_integer(N) ->
                NumUpdates0 + 1
        end,
    Manifest = Manifest0#manifest{
        next_offset = NextOffset,
        total_size = TotalSize0 + Size,
        entries =
            <<Entries0/binary,
                ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>)/binary>>
    },
    Stream = Stream0#stream{
        manifest = Manifest,
        upload_status = {last_uploaded, NumUpdates}
    },
    apply_infos_to_manifest(Rest, StreamId, Stream, Effects).

-doc """
Add a fragment to the list of available fragments.

Available fragments are stored in sorted order, descending.
""".
-spec add_available_fragment(#fragment{}, [#fragment{}]) -> [#fragment{}].
add_available_fragment(F, []) ->
    [F];
add_available_fragment(
    #fragment{first_offset = O1} = F, [#fragment{first_offset = O2} | _] = Fs
) when O1 > O2 ->
    [F | Fs];
add_available_fragment(F, [Head | Rest]) ->
    %% non-fast-lane: the fragment needs to be inserted in sorted order within
    %% the list rather than prepended.
    add_available_fragment(F, Rest, [Head]).

add_available_fragment(
    #fragment{first_offset = O1} = F, [#fragment{first_offset = O2} = Head | Rest], Acc
) when O1 < O2 ->
    add_available_fragment(F, Rest, [Head | Acc]);
add_available_fragment(F, Fs, Acc) ->
    lists:reverse(Acc, [F | Fs]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

add_available_fragment_test() ->
    %% `add_available_fragment/2` keeps the fragments ordered descending by
    %% first offset.
    Fragments = [#fragment{first_offset = N} || N <- lists:seq(1, 5)],
    Expected = lists:reverse(Fragments),
    ?assertEqual(Expected, lists:foldl(fun add_available_fragment/2, [], Fragments)),
    ?assertEqual(Expected, lists:foldr(fun add_available_fragment/2, [], Fragments)),
    ok.

-endif.
