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

-record(writer, {
    %% Pid of the osiris_writer process. Used to attach offset listeners.
    pid :: pid(),
    %% Local dir of the log.
    dir :: file:filename_all(),
    %% Current commit offset (updated by offset listener notifications) known
    %% to the manifest - this can lag behind the actual commit offset.
    commit_offset = -1 :: osiris:offset() | -1,
    %% List of segments in ascending offset order which have been rolled and
    %% are awaiting upload.
    available_fragments = [] :: [#fragment{}],
    %% List of fragments in ascending offset order which have been uploaded
    %% successfully but have not yet been applied to the manifest.
    uploaded_fragments = [] :: [#fragment_info{}],
    %% The next offset that should be uploaded.
    %% All offsets under this have been tiered without any "holes" in the
    %% remote log. TODO: is zero the correct default here? What if the entire
    %% local log is truncated away?
    next_tiered_offset = 0 :: osiris:offset()
}).

%% NOTE: Pending is reversed.
-type upload_status() ::
    {uploading, Pending :: [#fragment_info{}]}
    | {last_uploaded, non_neg_integer() | infinity}.

-record(?MODULE, {
    writers = #{} :: #{writer_ref() => #writer{}},
    manifests = #{} :: #{
        file:filename_all() =>
            {#manifest{} | undefined, upload_status()}
            | {pending, [gen_server:from()]}
    }
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

-spec get_manifest(Dir :: file:filename_all(), state()) -> #manifest{} | undefined.
get_manifest(Dir, #?MODULE{manifests = Manifests}) ->
    maps:get(Dir, Manifests, undefined).

-doc """
Apply an event to the state, evolving the state and returning a list of events
to execute.
""".
-spec apply(metadata(), event(), state()) -> {state(), [effect()]}.

apply(
    _Meta,
    #fragment_available{writer_ref = WriterRef, fragment = Fragment},
    #?MODULE{writers = Writers0} = State0
) ->
    case Writers0 of
        #{WriterRef := #writer{available_fragments = Fragments0} = Writer0} ->
            Fragments = add_available_fragment(Fragment, Fragments0),
            Writer = Writer0#writer{available_fragments = Fragments},
            State = State0#?MODULE{writers = Writers0#{WriterRef := Writer}},
            {State, []};
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #commit_offset_increased{writer_ref = WriterRef, offset = Offset},
    #?MODULE{writers = Writers0, manifests = Manifests} = State0
) ->
    case Writers0 of
        #{
            WriterRef := #writer{
                pid = Pid,
                dir = Dir,
                available_fragments = Available
            } = Writer0
        } ->
            Effects0 = [#register_offset_listener{writer_pid = Pid, offset = Offset + 1}],
            case Manifests of
                #{Dir := {pending, _Requesters}} ->
                    %% Wait until the manifest is resolved to upload fragments
                    %% so that we avoid uploading something which already
                    %% exists in the remote tier.
                    Writer = Writer0#writer{commit_offset = Offset},
                    {State0#?MODULE{writers = Writers0#{WriterRef := Writer}}, Effects0};
                #{Dir := _} ->
                    {Uncommitted, Committed} = lists:splitwith(
                        fun(#fragment{last_offset = LastOffset}) -> LastOffset > Offset end,
                        Available
                    ),
                    Effects = lists:foldl(
                        fun(Fragment, Acc) ->
                            [
                                #upload_fragment{
                                    writer_ref = WriterRef, dir = Dir, fragment = Fragment
                                }
                                | Acc
                            ]
                        end,
                        Effects0,
                        Committed
                    ),
                    Writer = Writer0#writer{
                        commit_offset = Offset,
                        available_fragments = Uncommitted
                    },
                    Writers = Writers0#{WriterRef := Writer},
                    State = State0#?MODULE{writers = Writers},
                    {State, Effects}
            end;
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #fragment_uploaded{writer_ref = WriterRef, info = #fragment_info{} = Info},
    #?MODULE{writers = Writers0, manifests = Manifests0} = State0
) ->
    case Writers0 of
        #{
            WriterRef := #writer{
                dir = Dir,
                uploaded_fragments = Uploaded0,
                next_tiered_offset = NTO0
            } = Writer0
        } ->
            Uploaded1 = insert_info(Info, Uploaded0),
            {NTO, Pending, Finished} = split_uploaded_infos(NTO0, Uploaded1),
            #{Dir := {Manifest0, UploadStatus0}} = Manifests0,
            %% assertion: the manifest can't be pending download here.
            case Manifest0 of
                #manifest{} -> ok;
                undefined -> ok
            end,
            {Manifest, UploadStatus, Effects} = apply_infos_to_manifest(
                Finished, Manifest0, UploadStatus0, Dir
            ),
            Writer = Writer0#writer{
                uploaded_fragments = Pending,
                next_tiered_offset = NTO
            },
            State = State0#?MODULE{
                writers = Writers0#{WriterRef := Writer},
                manifests = Manifests0#{Dir := {Manifest, UploadStatus}}
            },
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(_Meta, #manifest_uploaded{dir = Dir}, #?MODULE{manifests = Manifests0} = State0) ->
    case Manifests0 of
        #{Dir := {Manifest0, UploadStatus0}} ->
            %% assertion
            {uploading, Pending0} = UploadStatus0,
            %% Pending is stored reversed for quick prepends.
            Pending = lists:reverse(Pending0),
            {Manifest, UploadStatus, Effects} = apply_infos_to_manifest(
                Pending, Manifest0, {last_uploaded, 0}, Dir
            ),
            State = State0#?MODULE{
                manifests = Manifests0#{Dir := {Manifest, UploadStatus}}
            },
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #manifest_rebalanced{dir = Dir, manifest = Manifest0},
    #?MODULE{manifests = Manifests0} = State0
) ->
    case Manifests0 of
        #{Dir := {_Manifest0, UploadStatus0}} ->
            %% assertion
            {uploading, Pending0} = UploadStatus0,
            %% Pending is stored reversed for quick prepends.
            Pending = lists:reverse(Pending0),
            {Manifest, UploadStatus, Effects} = apply_infos_to_manifest(
                Pending, Manifest0, {last_uploaded, infinity}, Dir
            ),
            State = State0#?MODULE{
                manifests = Manifests0#{Dir := {Manifest, UploadStatus}}
            },
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #writer_spawned{pid = Pid, writer_ref = WriterRef, dir = Dir},
    #?MODULE{writers = Writers0, manifests = Manifests0} = State0
) ->
    Writers = Writers0#{WriterRef => #writer{pid = Pid, dir = Dir}},
    Manifests = Manifests0#{Dir => {pending, []}},
    State = State0#?MODULE{writers = Writers, manifests = Manifests},
    Effects = [
        #register_offset_listener{writer_pid = Pid, offset = -1},
        #resolve_manifest{dir = Dir}
    ],
    {State, Effects};
apply(
    _Meta,
    #manifest_requested{dir = Dir, requester = Requester},
    #?MODULE{manifests = Manifests0} = State0
) ->
    case Manifests0 of
        #{Dir := {#manifest{} = Manifest, _UploadStatus}} ->
            {State0, [#reply{to = Requester, response = Manifest}]};
        #{Dir := {pending, Requesters}} ->
            Manifests = Manifests0#{Dir := {pending, [Requester | Requesters]}},
            {State0#?MODULE{manifests = Manifests}, []};
        _ ->
            ResolveManifest = #resolve_manifest{dir = Dir},
            Manifests = Manifests0#{Dir => {pending, [Requester]}},
            {State0#?MODULE{manifests = Manifests}, [ResolveManifest]}
    end;
apply(
    _Meta,
    #manifest_resolved{dir = Dir, manifest = Manifest},
    #?MODULE{writers = Writers0, manifests = Manifests0} = State0
) ->
    case Manifests0 of
        #{Dir := {pending, Requesters}} ->
            %% NOTE: `Requesters` is in reverse order.
            Effects0 = lists:foldl(
                fun(R, Acc) -> [#reply{to = R, response = Manifest} | Acc] end,
                [],
                Requesters
            ),
            State1 = State0#?MODULE{
                manifests = Manifests0#{Dir := {Manifest, {last_uploaded, 0}}}
            },
            %% THOUGHT: keep a lookup map for `Dir => WriterRef`?
            case [{Ref, W} || Ref := #writer{dir = D} = W <- Writers0, D =:= Dir] of
                [
                    {WriterRef,
                        #writer{commit_offset = CommitOffset, available_fragments = Available0} =
                            Writer0}
                ] when Manifest =/= undefined ->
                    LastOffsetInManifest =
                        case
                            rabbitmq_stream_s3_binary_array:last(
                                ?ENTRY_B, Manifest#manifest.entries
                            )
                        of
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
                                    writer_ref = WriterRef,
                                    dir = Dir,
                                    fragment = Fragment
                                }
                                | Acc
                            ]
                        end,
                        Effects0,
                        Committed
                    ),
                    Writer = Writer0#writer{available_fragments = Uncommitted},
                    State = State1#?MODULE{writers = Writers0#{WriterRef := Writer}},
                    {State, Effects};
                _ ->
                    {State1, Effects0}
            end;
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
-spec apply_infos_to_manifest(
    [#fragment_info{}], #manifest{} | undefined, upload_status(), file:filename_all()
) ->
    {#manifest{}, upload_status(), [effect()]}.
apply_infos_to_manifest(Infos, Manifest, UploadStatus, Dir) ->
    apply_infos_to_manifest(Infos, Manifest, UploadStatus, Dir, []).

apply_infos_to_manifest(
    [], #manifest{entries = Entries} = Manifest, {last_uploaded, _} = UploadStatus0, Dir, Effects0
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
                dir = Dir,
                kind = GroupKind,
                size = GroupSize,
                new_group = Group,
                rebalanced = Rebalanced,
                manifest = Manifest
            },
            {Manifest, {uploading, []}, [Rebalance | Effects0]}
    end;
apply_infos_to_manifest([], Manifest, {last_uploaded, NumUpdates}, Dir, Effects0) when
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
    Upload = #upload_manifest{dir = Dir, manifest = Manifest},
    {Manifest, {uploading, []}, [Upload | Effects0]};
apply_infos_to_manifest([], Manifest, UploadStatus, _Dir, Effects) ->
    %% The manifest is currently being uploaded, or there are no updates
    %% necessary. Skip the upload.
    ?LOG_DEBUG("Skipping upload of manifest with status ~w", [UploadStatus]),
    {Manifest, UploadStatus, Effects};
apply_infos_to_manifest(
    [#fragment_info{offset = Offset, timestamp = Ts, seq_no = SeqNo, size = Size} | Rest],
    undefined,
    UploadStatus0,
    Dir,
    Effects
) ->
    ?assertEqual({last_uploaded, 0}, UploadStatus0),
    %% The very first fragment in the manifest. Create a new manifest.
    Manifest = #manifest{
        first_offset = Offset,
        first_timestamp = Ts,
        total_size = Size,
        entries = ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>)
    },
    %% And force its upload.
    UploadStatus = {last_uploaded, infinity},
    apply_infos_to_manifest(Rest, Manifest, UploadStatus, Dir, Effects);
apply_infos_to_manifest([Fragment | Rest], Manifest, {uploading, Pending0}, Dir, Effects) ->
    %% The manifest is currently being uploaded. Queue the fragment for later
    %% application once the current upload completes.
    apply_infos_to_manifest(Rest, Manifest, {uploading, [Fragment | Pending0]}, Dir, Effects);
apply_infos_to_manifest(
    [#fragment_info{offset = Offset, timestamp = Ts, seq_no = SeqNo, size = Size} | Rest],
    #manifest{total_size = TotalSize0, entries = Entries0} = Manifest0,
    {last_uploaded, NumUpdates0},
    Dir,
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
        total_size = TotalSize0 + Size,
        entries =
            <<Entries0/binary,
                ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>)/binary>>
    },
    apply_infos_to_manifest(Rest, Manifest, {last_uploaded, NumUpdates}, Dir, Effects).

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
