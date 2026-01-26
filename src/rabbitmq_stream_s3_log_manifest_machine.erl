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

-define(SERVER, rabbitmq_stream_s3_log_manifest).

-type cfg() :: #{
    debounce_modifications => non_neg_integer(),
    debounce_milliseconds => non_neg_integer()
}.

-type milliseconds() :: non_neg_integer().

-type manifest() :: #manifest{} | {pending, [gen_server:from() | pid()]}.

%% Subset of osiris:retention_spec(), as a map.
-type retention_spec() :: #{
    max_bytes := non_neg_integer(),
    max_age := milliseconds()
}.

-type writer() :: #{
    kind := writer,
    %% PID of the `osiris_writer` process. Used to attach offset listeners.
    pid := pid(),
    %% A mapping of replica node to the last range sent to it.
    %% The writer is included in this map. Used to send `#fragments_applied{}`
    %% notifications to replicas and emit the `#set_range{}` effect on this node.
    ranges := #{node() := {osiris:offset(), osiris:offset()}},
    retention := retention_spec(),
    %% Local log's directory.
    dir := directory(),
    manifest := manifest(),
    %% Number of fragments applied to the manifest since the last upload.
    modifications := non_neg_integer(),
    %% The current active modification to the remote tier. We set this to
    %% prevent ourselves from performing retention at the same time as
    %% rebalancing, for example.
    pending_change := none | upload | retention | rebalance,
    %% Timestamp when the manifest was last uploaded. Used to debounce uploads.
    last_uploaded := osiris:timestamp(),
    %% Current commit offset (updated by offset listener notifications) known
    %% to the manifest - this can lag behind the actual commit offset.
    commit_offset := osiris:offset() | -1,
    %% List of segments in ascending offset order which have been rolled and
    %% are awaiting upload.
    available_fragments := [#fragment{}],
    %% List of fragments in ascending offset order which have been uploaded
    %% successfully but have not yet been applied to the manifest.
    uploaded_fragments := [#fragment_info{}]
}.

-type replica() :: #{kind := replica, manifest := manifest()}.

-type stream() :: writer() | replica().

-record(?MODULE, {
    cfg :: cfg(),
    streams = #{} :: #{stream_id() => stream()}
}).

-type metadata() :: #{
    %% Time the same way Osiris computes it: erlang:system_time(millisecond).
    time := osiris:timestamp()
}.

-opaque state() :: #?MODULE{}.

-export_type([metadata/0, state/0]).

-export([new/0, new/1, get_manifest/2, apply/3]).

-spec writer(pid(), [node()], directory(), retention_spec(), [#fragment{}]) -> writer().
writer(Pid, ReplicaNodes, Dir, Retention, Available) ->
    ?assertEqual(node(Pid), node()),
    #{
        kind => writer,
        pid => Pid,
        ranges => #{Node => {0, 0} || Node <- [node() | ReplicaNodes]},
        retention => Retention,
        dir => Dir,
        manifest => {pending, []},
        pending_change => none,
        modifications => 0,
        last_uploaded => -1,
        commit_offset => -1,
        available_fragments => Available,
        uploaded_fragments => []
    }.

-spec replica() -> replica().
replica() ->
    #{
        kind => replica,
        manifest => {pending, []}
    }.

-doc """
Create a default, empty machine state.
""".
-spec new() -> state().
new() ->
    new(#{
        debounce_modifications => application:get_env(
            rabbitmq_stream_s3, manifest_debounce_modifications, 10
        ),
        debounce_milliseconds => application:get_env(
            rabbitmq_stream_s3, manifest_debounce_milliseconds, 5000
        )
    }).

-spec new(cfg()) -> state().
new(Cfg) ->
    #?MODULE{cfg = Cfg}.

-spec get_manifest(StreamId :: stream_id(), state()) -> #manifest{} | undefined.
get_manifest(StreamId, #?MODULE{streams = Streams}) ->
    case Streams of
        #{StreamId := #{manifest := #manifest{} = M}} ->
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
        #{StreamId := #{available_fragments := Fragments0} = Writer0} ->
            Fragments = add_available_fragment(Fragment, Fragments0),
            Writer1 = Writer0#{available_fragments := Fragments},
            {Writer, Effects} = upload_available_fragments(StreamId, Writer1, []),
            State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #commit_offset_increased{stream = StreamId, offset = Offset},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #{pid := Pid, manifest := Manifest} = Writer0} ->
            Effects0 = [#register_offset_listener{writer_pid = Pid, offset = Offset + 1}],
            Writer1 = Writer0#{commit_offset := Offset},
            case Manifest of
                {pending, _Requesters} ->
                    %% Wait until the manifest is resolved to upload fragments
                    %% so that we avoid uploading something which already
                    %% exists in the remote tier.
                    {State0#?MODULE{streams = Streams0#{StreamId := Writer1}}, Effects0};
                _ ->
                    {Writer, Effects} = upload_available_fragments(StreamId, Writer1, Effects0),
                    State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
                    {State, Effects}
            end;
        _ ->
            {State0, []}
    end;
apply(
    Meta,
    #fragment_uploaded{stream = StreamId, info = #fragment_info{} = Info},
    #?MODULE{cfg = Cfg, streams = Streams0} = State0
) ->
    case Streams0 of
        #{
            StreamId := #{
                manifest := #manifest{next_offset = NextOffset0} = Manifest0,
                modifications := Modifications0,
                uploaded_fragments := Uploaded0
            } = Writer0
        } ->
            Uploaded1 = insert_info(Info, Uploaded0),
            {NextOffset, Pending, Finished} = split_uploaded_infos(NextOffset0, Uploaded1),
            {ok, Manifest} = apply_infos(Finished, Manifest0),
            %% assertion: the finished fragments were applied up to the
            %% next-tiered-offset we expected from split_uploaded_infos/2.
            #manifest{next_offset = NextOffset} = Manifest,
            Writer1 = Writer0#{
                manifest := Manifest,
                modifications := Modifications0 + length(Finished),
                uploaded_fragments := Pending
            },
            {Writer2, Effects0} = evaluate_writer(Cfg, Meta, StreamId, Writer1, []),
            {Writer, Effects} = notify_fragments_applied(Finished, StreamId, Writer2, Effects0),
            State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #fragments_applied{
        stream = StreamId,
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        fragments = Fragments
    },
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #{kind := replica, manifest := #manifest{} = Manifest0} = Replica0} ->
            case apply_infos(Fragments, Manifest0) of
                {ok, #manifest{next_offset = NextOffset} = Manifest1} ->
                    Manifest = Manifest1#manifest{
                        first_offset = FirstOffset,
                        first_timestamp = FirstTs
                    },
                    Streams = Streams0#{StreamId := Replica0#{manifest := Manifest}},
                    SetRange = #set_range{
                        stream = StreamId,
                        first = FirstOffset,
                        next = NextOffset
                    },
                    {State0#?MODULE{streams = Streams}, [SetRange]};
                {error, OutOfSequenceInfo} ->
                    ?LOG_DEBUG(
                        "Replica received an out-of-sequence fragment info. Refreshing manifest from writer... ~0p",
                        [OutOfSequenceInfo]
                    ),
                    Effect = #manifest_requested{stream = StreamId, requester = self()},
                    Replica = Replica0#{manifest := {pending, []}},
                    {Replica, [Effect]}
            end;
        _ ->
            {State0, []}
    end;
apply(
    #{time := Ts},
    #manifest_uploaded{stream = StreamId},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #{kind := writer, pending_change := Pending} = Writer0} when
            Pending /= none
        ->
            Writer = Writer0#{modifications := 0, last_uploaded := Ts, pending_change := none},
            State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
            {State, []};
        _ ->
            {State0, []}
    end;
apply(_Meta, #manifest_rebalanced{}, _State) ->
    erlang:error(unimplemented);
apply(
    _Meta,
    #writer_spawned{
        pid = Pid,
        stream = StreamId,
        dir = Dir,
        replica_nodes = ReplicaNodes,
        retention = Retention0,
        available_fragments = Available
    },
    #?MODULE{streams = Streams0} = State0
) ->
    Retention = #{K => V || {K, V} <- Retention0, K =:= max_age orelse K =:= max_bytes},
    Writer0 = writer(Pid, ReplicaNodes, Dir, Retention, Available),
    Effects0 = [#register_offset_listener{writer_pid = Pid, offset = -1}],
    case Streams0 of
        #{StreamId := #{manifest := {pending, Pending0}}} ->
            Writer = Writer0#{manifest := {pending, Pending0}},
            State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
            {State, Effects0};
        _ ->
            State = State0#?MODULE{streams = Streams0#{StreamId => Writer0}},
            Effects = [#resolve_manifest{stream = StreamId} | Effects0],
            {State, Effects}
    end;
apply(_Meta, #acceptor_spawned{stream = StreamId}, #?MODULE{streams = Streams0} = State0) ->
    Stream0 = replica(),
    case Streams0 of
        #{StreamId := #{manifest := {pending, Pending0}}} ->
            Stream = Stream0#{manifest := {pending, Pending0}},
            State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
            {State, []};
        _ ->
            State = State0#?MODULE{streams = Streams0#{StreamId => Stream0}},
            {State, []}
    end;
apply(
    _Meta,
    #manifest_requested{stream = StreamId, requester = Requester},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #{manifest := #manifest{} = Manifest}} ->
            Reply =
                case Requester of
                    {_, _} ->
                        #reply{to = Requester, response = Manifest};
                    _ when is_pid(Requester) ->
                        Message = #manifest_resolved{stream = StreamId, manifest = Manifest},
                        #send{to = Requester, message = Message}
                end,
            {State0, [Reply]};
        #{StreamId := #{manifest := {pending, Requesters0}} = Stream0} ->
            Stream = Stream0#{manifest := {pending, [Requester | Requesters0]}},
            State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
            {State, []};
        _ ->
            Stream = (replica())#{manifest := {pending, [Requester]}},
            State = State0#?MODULE{streams = Streams0#{StreamId => Stream}},
            {State, [#resolve_manifest{stream = StreamId}]}
    end;
apply(
    #{time := Ts},
    #manifest_resolved{
        stream = StreamId,
        manifest =
            #manifest{
                first_offset = FirstOffset,
                next_offset = NextOffset
            } = Manifest
    } = Event,
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #{manifest := {pending, Requesters}} = Stream0} ->
            Stream1 = Stream0#{manifest := Manifest},
            SetRange = #set_range{stream = StreamId, first = FirstOffset, next = NextOffset},
            %% NOTE: `Requesters` is in reverse order.
            Effects0 = lists:foldl(
                fun
                    ({_, _} = R, Acc) ->
                        [#reply{to = R, response = Manifest} | Acc];
                    (Pid, Acc) when is_pid(Pid) ->
                        [#send{to = Pid, message = Event} | Acc]
                end,
                [SetRange],
                Requesters
            ),
            case Stream1 of
                #{kind := writer, ranges := Ranges0, available_fragments := Available0} ->
                    %% If there is a hole between the last fragments in the
                    %% manifest and the first available fragment, backfill
                    %% fragments from local stream data.
                    Effects1 = maybe_find_fragments(StreamId, Stream1, Effects0),
                    %% Drop all available fragments which are older than what
                    %% has already been uploaded to the remote tier.
                    Available = lists:filter(
                        fun(#fragment{first_offset = Offset}) ->
                            Offset >= NextOffset
                        end,
                        Available0
                    ),
                    Ranges = #{Node => {FirstOffset, NextOffset} || Node := _ <- Ranges0},
                    Stream2 = Stream1#{
                        last_uploaded := Ts,
                        available_fragments := Available,
                        ranges := Ranges
                    },
                    {Stream, Effects} = upload_available_fragments(StreamId, Stream2, Effects1),
                    State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
                    {State, Effects};
                _ ->
                    State = State0#?MODULE{streams = Streams0#{StreamId := Stream1}},
                    {State, Effects0}
            end;
        _ ->
            {State0, []}
    end;
apply(Meta, #tick{}, #?MODULE{cfg = Cfg, streams = Streams0} = State0) ->
    {Streams, Effects} =
        maps:fold(
            fun
                (
                    StreamId,
                    #{kind := writer, manifest := #manifest{}, pending_change := none} = Writer0,
                    {Streams1, Effs0}
                ) ->
                    {Writer1, Effs1} = evaluate_retention(Meta, StreamId, Writer0, Effs0),
                    {Writer2, Effs2} = evaluate_upload(Cfg, Meta, StreamId, Writer1, Effs1),
                    {Writer, Effs} = notify_fragments_applied([], StreamId, Writer2, Effs2),
                    {Streams1#{StreamId := Writer}, Effs};
                (_StreamId, _Stream, Acc) ->
                    Acc
            end,
            {Streams0, []},
            Streams0
        ),
    {State0#?MODULE{streams = Streams}, Effects};
apply(
    Meta,
    #retention_updated{stream = StreamId, retention = Retention0},
    #?MODULE{streams = Streams0} = State0
) ->
    Retention = #{K => V || {K, V} <- Retention0, K =:= max_age orelse K =:= max_bytes},
    case Streams0 of
        #{StreamId := #{kind := writer} = Writer0} ->
            Writer1 = Writer0#{retention := Retention},
            {Writer2, Effects0} = evaluate_retention(Meta, StreamId, Writer1, []),
            {Writer, Effects} = notify_fragments_applied([], StreamId, Writer2, Effects0),
            State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
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

`Infos` is expected to be sorted by offset ascending.
""".
-spec apply_infos([#fragment_info{}], #manifest{}) ->
    {ok, #manifest{}} | {error, #fragment_info{}}.
apply_infos([], Manifest) ->
    {ok, Manifest};
apply_infos(
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
    #manifest{
        next_offset = Offset,
        total_size = TotalSize0,
        entries = Entries0
    } = Manifest0
) ->
    Manifest1 =
        case Offset of
            0 ->
                %% For the very first fragment, also set the offset and timestamp.
                Manifest0#manifest{first_offset = Offset, first_timestamp = Ts};
            _ ->
                Manifest0
        end,
    Manifest = Manifest1#manifest{
        next_offset = NextOffset,
        total_size = TotalSize0 + Size,
        entries =
            <<Entries0/binary,
                ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, Size, SeqNo, <<>>)/binary>>
    },
    apply_infos(Rest, Manifest);
apply_infos([Info | _], #manifest{}) ->
    {error, Info}.

-spec notify_fragments_applied([#fragment_info{}], stream_id(), writer(), [effect()]) ->
    {writer(), [effect()]}.
notify_fragments_applied(
    Fragments,
    StreamId,
    #{
        ranges := Ranges0,
        manifest := #manifest{
            first_offset = FirstOffset,
            first_timestamp = FirstTs,
            next_offset = NextOffset
        }
    } = Writer0,
    Effects0
) ->
    Effects =
        maps:fold(
            fun
                (_Node, {F, N}, Acc) when F =:= FirstOffset andalso N =:= NextOffset ->
                    Acc;
                (ReplicaNode, {_, _}, Acc) when ReplicaNode /= node() ->
                    Event = #fragments_applied{
                        stream = StreamId,
                        first_offset = FirstOffset,
                        first_timestamp = FirstTs,
                        fragments = Fragments
                    },
                    Effect = #send{
                        to = {?SERVER, ReplicaNode},
                        message = Event,
                        options = [noconnect]
                    },
                    [Effect | Acc];
                (Node, {_, _}, Acc) when Node == node() ->
                    Effect = #set_range{stream = StreamId, first = FirstOffset, next = NextOffset},
                    [Effect | Acc]
            end,
            Effects0,
            Ranges0
        ),
    Ranges = #{Node => {FirstOffset, NextOffset} || Node := _ <- Ranges0},
    Writer = Writer0#{ranges := Ranges},
    {Writer, Effects}.

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

-doc """
Create effects to upload available fragments.

Fragments may be uploaded when their last offset has been fully committed.
Fragments can be uploaded in any order: handling for out-of-order uploads is
done when handling `#fragment_uploaded{}` events rather than before upload.
Fragments are stored in descending order in the `available_fragments` field
to make this function quick.
""".
-spec upload_available_fragments(stream_id(), writer(), [effect()]) -> {writer(), [effect()]}.
upload_available_fragments(
    StreamId,
    #{
        dir := Dir,
        commit_offset := CommitOffset,
        available_fragments := Available0
    } = Writer0,
    Effects0
) ->
    {Available, Committed} = lists:splitwith(
        fun(#fragment{last_offset = LastOffset}) ->
            LastOffset > CommitOffset
        end,
        Available0
    ),
    Effects = lists:foldl(
        fun(Fragment, Acc) ->
            Eff = #upload_fragment{stream = StreamId, dir = Dir, fragment = Fragment},
            [Eff | Acc]
        end,
        Effects0,
        Committed
    ),
    Writer = Writer0#{available_fragments := Available},
    {Writer, Effects}.

-spec evaluate_writer(cfg(), metadata(), stream_id(), writer(), [effect()]) ->
    {writer(), [effect()]}.
evaluate_writer(Cfg, Meta, StreamId, Writer0, Effects0) ->
    {Writer1, Effects1} = evaluate_retention(Meta, StreamId, Writer0, Effects0),
    {Writer2, Effects2} = evaluate_rebalance(StreamId, Writer1, Effects1),
    evaluate_upload(Cfg, Meta, StreamId, Writer2, Effects2).

evaluate_retention(
    #{time := Now},
    StreamId,
    #{
        pending_change := none,
        retention := RetentionSpec,
        manifest := #manifest{
            total_size = TotalSize0,
            first_timestamp = FirstTs,
            entries = Entries0
        } = Manifest0
    } = Writer0,
    Effects0
) ->
    ExceedsRetention =
        case RetentionSpec of
            #{max_bytes := MaxBytes} when TotalSize0 > MaxBytes ->
                true;
            #{max_age := MaxAge} when FirstTs < Now - MaxAge ->
                true;
            _ when Entries0 =:= <<>> ->
                %% Nothing to reclaim!
                false;
            _ ->
                false
        end,
    case ExceedsRetention of
        true ->
            case Entries0 of
                ?ENTRY(_Offset, _Timestamp, ?MANIFEST_KIND_FRAGMENT, _Size, _SeqNo, _Rest) ->
                    %% In the common case a stream will not be so long that it
                    %% needs groups. Luckily, this means we can determine
                    %% which fragments can be deleted very efficiently by
                    %% looking just at manifest's entries array.
                    {Manifest, Offsets} = evaluate_retention1(Manifest0, Now, RetentionSpec),
                    Writer = Writer0#{manifest := Manifest, pending_change := retention},
                    UploadManifest = #upload_manifest{stream = StreamId, manifest = Manifest},
                    DeleteFragments = #delete_fragments{stream = StreamId, offsets = Offsets},
                    {Writer, [UploadManifest, DeleteFragments | Effects0]};
                _ ->
                    %% TODO: if the first entry is not a fragment then it is
                    %% a group (or kilo-group or mega-group). Create a new
                    %% effect which downloads the necessary group(s) and
                    %% carries on retention from there.
                    erlang:error(unimplemented)
            end;
        false ->
            {Writer0, Effects0}
    end;
evaluate_retention(_Meta, _StreamId, Writer, Effects) ->
    {Writer, Effects}.

evaluate_retention1(Manifest, Now, RetentionSpec) ->
    evaluate_retention1(Manifest, Now, RetentionSpec, []).

%% NOTE: keep at least one entry so that we can set the `first_offset` and
%% `first_timestamp`.
evaluate_retention1(
    #manifest{
        total_size = TotalSize0,
        entries = ?ENTRY(Offset, _Ts, Kind, Size, _SeqNo, Rest)
    } = Manifest0,
    Now,
    #{max_bytes := MaxBytes} = Spec,
    Offsets0
) when TotalSize0 > MaxBytes andalso Rest /= <<>> ->
    ?assertEqual(Kind, ?MANIFEST_KIND_FRAGMENT),
    Manifest = Manifest0#manifest{
        total_size = TotalSize0 - Size,
        entries = Rest
    },
    evaluate_retention1(Manifest, Now, Spec, [Offset | Offsets0]);
evaluate_retention1(
    #manifest{
        total_size = TotalSize0,
        entries = ?ENTRY(Offset, Ts, Kind, Size, _SeqNo, Rest)
    } = Manifest0,
    Now,
    #{max_age := MaxAge} = Spec,
    Offsets0
) when Now - Ts > MaxAge andalso Rest /= <<>> ->
    ?assertEqual(Kind, ?MANIFEST_KIND_FRAGMENT),
    Manifest = Manifest0#manifest{
        total_size = TotalSize0 - Size,
        entries = Rest
    },
    evaluate_retention1(Manifest, Now, Spec, [Offset | Offsets0]);
evaluate_retention1(#manifest{entries = Entries} = Manifest0, _Now, _Spec, Offsets) ->
    ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, _Size, _SeqNo, _Rest) = Entries,
    Manifest = Manifest0#manifest{
        first_offset = Offset,
        first_timestamp = Ts
    },
    %% No real point to this lists:reverse/1. It just makes it appear nicer.
    {Manifest, lists:reverse(Offsets)}.

evaluate_rebalance(_StreamId, Writer0, Effects0) ->
    %% TODO
    {Writer0, Effects0}.

evaluate_upload(
    Cfg,
    #{time := Ts},
    StreamId,
    #{
        kind := writer,
        manifest := #manifest{} = Manifest,
        modifications := Mods,
        last_uploaded := LastUploadTs,
        pending_change := none
    } = Writer0,
    Effects0
) ->
    ExceedsDebounce =
        case Cfg of
            #{debounce_modifications := M} when Mods >= M ->
                true;
            #{debounce_milliseconds := Millis} when Millis > Ts - LastUploadTs andalso Mods > 0 ->
                true;
            _ ->
                false
        end,
    case ExceedsDebounce of
        true ->
            UploadManifest = #upload_manifest{stream = StreamId, manifest = Manifest},
            Writer = Writer0#{pending_change := upload},
            {Writer, [UploadManifest | Effects0]};
        false ->
            {Writer0, Effects0}
    end;
evaluate_upload(_Cfg, _Meta, _StreamId, Writer, Effects) ->
    {Writer, Effects}.

-spec maybe_find_fragments(stream_id(), writer(), [effect()]) -> [effect()].
maybe_find_fragments(_StreamId, #{available_fragments := []}, Effects) ->
    Effects;
maybe_find_fragments(
    StreamId,
    #{
        available_fragments := Available,
        dir := Dir,
        manifest := #manifest{next_offset = NextOffset}
    },
    Effects0
) ->
    %% `available_fragments` is sorted in descending order.
    #fragment{first_offset = FirstOffset} = lists:last(Available),
    case FirstOffset > NextOffset of
        true ->
            FindFragments = #find_fragments{
                stream = StreamId,
                dir = Dir,
                from = NextOffset,
                to = FirstOffset
            },
            [FindFragments | Effects0];
        false ->
            Effects0
    end.

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

upload_available_fragments_test() ->
    StreamId = erlang:make_ref(),
    Dir = <<"">>,
    Fragments0 = [
        #fragment{
            first_offset = N * 2,
            last_offset = N * 2 + 1,
            next_offset = (N + 1) * 2
        }
     || N <- lists:seq(0, 5)
    ],
    %% `available_fragments` are stored in descending order, see
    %% `add_available_fragment/2` and the test above.
    Fragments = lists:reverse(Fragments0),
    %% Emit upload effects for everything below the commit offset.
    ?assertMatch(
        {
            #{available_fragments := []},
            [
                #upload_fragment{fragment = #fragment{first_offset = 0}},
                #upload_fragment{fragment = #fragment{first_offset = 2}},
                #upload_fragment{},
                #upload_fragment{},
                #upload_fragment{},
                #upload_fragment{fragment = #fragment{first_offset = 10, next_offset = 12}}
            ]
        },
        upload_available_fragments(
            StreamId,
            #{dir => Dir, commit_offset => 12, available_fragments => Fragments},
            []
        )
    ),
    ?assertMatch(
        {
            #{
                available_fragments := [
                    #fragment{first_offset = 10},
                    #fragment{},
                    #fragment{first_offset = 6}
                ]
            },
            [
                #upload_fragment{fragment = #fragment{first_offset = 0}},
                #upload_fragment{fragment = #fragment{first_offset = 2}},
                #upload_fragment{fragment = #fragment{first_offset = 4}}
            ]
        },
        upload_available_fragments(
            StreamId,
            #{dir => Dir, commit_offset => 6, available_fragments => Fragments},
            []
        )
    ),
    ok.

apply_infos_test() ->
    Ts = erlang:system_time(millisecond),
    [I1, I2, I3] =
        Infos = [
            #fragment_info{
                offset = N * 20,
                timestamp = Ts + N,
                next_offset = N * 20 + 20,
                seq_no = N,
                size = 200
            }
         || N <- lists:seq(0, 2)
        ],
    ?assertMatch(
        {ok, #manifest{first_timestamp = Ts, next_offset = 60}},
        apply_infos(Infos, #manifest{})
    ),
    ?assertEqual(
        {error, I2},
        apply_infos([I2, I3], #manifest{})
    ),
    ?assertEqual(
        {error, I3},
        apply_infos([I1, I3], #manifest{})
    ),
    ok.

evaluate_retention1_test() ->
    Ts = erlang:system_time(millisecond),
    Entries = <<
        ?ENTRY((N * 20), (Ts - 100 + N * 20), ?MANIFEST_KIND_FRAGMENT, 200, N, <<>>)
     || N <- lists:seq(0, 4)
    >>,
    Manifest = #manifest{
        first_offset = 0,
        first_timestamp = Ts - 100,
        next_offset = 6 * 20,
        total_size = 1000,
        entries = Entries
    },
    %% No retention spec, nothing to do.
    ?assertEqual({Manifest, []}, evaluate_retention1(Manifest, Ts, #{})),

    %% == MAX BYTES ==
    ?assertEqual({Manifest, []}, evaluate_retention1(Manifest, Ts, #{max_bytes => 1000})),
    ?assertMatch(
        {#manifest{first_offset = 20, total_size = 800}, [0]},
        evaluate_retention1(Manifest, Ts, #{max_bytes => 900})
    ),
    ?assertMatch(
        {#manifest{first_offset = 60, total_size = 400}, [0, 20, 40]},
        evaluate_retention1(Manifest, Ts, #{max_bytes => 500})
    ),
    %% Make sure we keep at least one entry.
    ?assertMatch(
        {#manifest{first_offset = 80, total_size = 200}, [0, 20, 40, 60]},
        evaluate_retention1(Manifest, Ts, #{max_bytes => 100})
    ),

    %% == MAX AGE ==
    ?assertEqual({Manifest, []}, evaluate_retention1(Manifest, Ts, #{max_age => 100_000})),
    ?assertMatch(
        {#manifest{first_offset = 20, total_size = 800}, [0]},
        evaluate_retention1(Manifest, Ts, #{max_age => 99})
    ),
    ?assertMatch(
        {#manifest{first_offset = 60, total_size = 400}, [0, 20, 40]},
        evaluate_retention1(Manifest, Ts, #{max_age => 59})
    ),
    ?assertMatch(
        {#manifest{first_offset = 80, total_size = 200}, [0, 20, 40, 60]},
        evaluate_retention1(Manifest, Ts, #{max_age => 1})
    ),

    ok.

-endif.
