%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_machine).
-moduledoc """
The "functional core" of the log manifest.

This module contains purely functional logic which handles events like
a new fragment becoming available, or the commit offset moving forward. These
events are applied to the state record and `apply/3` returns a list of effects
for the log manifest server to execute.
""".

-compile({no_auto_import, [apply/2, apply/3]}).

%% OTP 27 dialyzer is incorrect about these warnings. OTP 28 dialyzer correctly
%% passes without these exceptions. Once OTP 28 is required, remove these
%% exceptions:
-dialyzer({no_return, format_timestamp/1}).
-dialyzer(
    {no_unused, [
        format_size/1,
        format_size/2,
        format_entries/1,
        format_retention/1,
        format_duration_ms/1,
        format_duration_seconds/1,
        format_duration_minutes/1,
        format_duration_hours/1,
        format_duration_days/1,
        format_duration_weeks/1,
        format_duration_years/1,
        count_kinds/2
    ]}
).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-define(SERVER, rabbitmq_stream_s3_server).
-define(FORMAT_DURATION(D), (float_to_binary(D, [{decimals, 2}, compact])) / binary).

-type cfg() :: #{
    %% The number of entries of a given `rabbitmq_stream_s3:kind()` which may
    rebalance_factor => non_neg_integer(),
    %% A count of modifications that must be met or exceeded to trigger an
    %% upload of the manifest to the remote tier.
    debounce_modifications => non_neg_integer(),
    %% A number of milliseconds where modifications to a manifest may be
    %% buffered locally before the manifest is uploaded to the remote tier.
    debounce_milliseconds => non_neg_integer()
}.

-type manifest() :: #manifest{} | {pending, [gen_server:from() | pid()]}.

-type writer() :: #{
    kind := writer,
    %% PID of the `osiris_writer` process. Used to attach offset listeners.
    pid := pid(),
    replica_nodes := [node()],
    retention := rabbitmq_stream_s3:retention_spec(),
    %% Local log's directory.
    dir := directory(),
    epoch := osiris:epoch(),
    reference := stream_reference(),
    shared := atomics:atomics_ref(),
    counter := counters:counters_ref(),
    manifest := manifest(),
    seq := non_neg_integer(),
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

-type replica() :: #{
    kind := replica,
    manifest := manifest(),
    seq := non_neg_integer(),
    dir => directory(),
    counter => counters:counters_ref(),
    shared => atomics:atomics_ref()
}.

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

-export([new/0, new/1, get_manifest/2, apply/3, format/1]).

-export([execute_retention/4]).

%% Used by tests:
-export([apply_infos/2, new_edit/1]).

-spec writer(#writer_spawned{}) -> writer().
writer(#writer_spawned{
    pid = Pid,
    config = #{
        dir := Dir,
        epoch := Epoch,
        reference := Reference,
        shared := Shared,
        counter := Counter,
        replica_nodes := ReplicaNodes,
        retention := Retention0
    },
    available_fragments = Available
}) ->
    Retention = #{K => V || {K, V} <- Retention0, K =:= max_age orelse K =:= max_bytes},
    ?assertEqual(node(Pid), node()),
    #{
        kind => writer,
        pid => Pid,
        replica_nodes => ReplicaNodes,
        retention => Retention,
        dir => Dir,
        epoch => Epoch,
        reference => Reference,
        shared => Shared,
        counter => Counter,
        manifest => {pending, []},
        seq => 0,
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
        manifest => {pending, []},
        seq => 0
    }.
-spec replica(osiris_log:config()) -> replica().
replica(#{dir := Dir, shared := Shared, counter := Counter}) ->
    (replica())#{
        dir => Dir,
        shared => Shared,
        counter => Counter
    }.

-doc """
Create a default, empty machine state.
""".
-spec new() -> state().
new() ->
    new(#{
        rebalance_factor => application:get_env(
            rabbitmq_stream_s3, manifest_rebalance_factor, 1024
        ),
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
            case Finished of
                [] ->
                    Writer = Writer0#{uploaded_fragments := Uploaded1},
                    State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
                    {State, []};
                [_ | _] ->
                    {ok, Edit} = apply_infos(Finished, Manifest0),
                    Manifest = apply_edit(Edit, Manifest0),
                    %% assertion: the finished fragments were applied up to the
                    %% next-tiered-offset we expected from split_uploaded_infos/2.
                    #manifest{next_offset = NextOffset} = Manifest,
                    Writer1 = Writer0#{
                        manifest := Manifest,
                        modifications := Modifications0 + length(Finished),
                        uploaded_fragments := Pending
                    },
                    TriggerRetention = lists:any(
                        fun(#fragment_info{roll_reason = Reason}) -> Reason =:= segment_roll end,
                        Finished
                    ),
                    {Writer2, Edits, Effects0} = evaluate_writer(
                        Cfg,
                        Meta,
                        StreamId,
                        Writer1,
                        [Edit],
                        []
                    ),
                    {Writer, Effects} = notify_edits(
                        Edits,
                        TriggerRetention,
                        StreamId,
                        Writer2,
                        Effects0
                    ),
                    State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
                    {State, Effects}
            end;
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #manifest_edited{
        stream = StreamId,
        edits = Edits,
        seq = EditSeq,
        trigger_retention = ShouldTriggerRetention
    },
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{
            StreamId := #{
                kind := replica,
                seq := Seq,
                dir := Dir,
                counter := Counter,
                shared := Shared,
                manifest := #manifest{} = Manifest0
            } = Replica0
        } ->
            case Seq + 1 =:= EditSeq of
                true ->
                    Effects0 =
                        case ShouldTriggerRetention of
                            true ->
                                TriggerRetention = #trigger_retention{
                                    stream = StreamId,
                                    dir = Dir,
                                    shared = Shared,
                                    counter = Counter
                                },
                                [TriggerRetention];
                            false ->
                                []
                        end,
                    Manifest = apply_edits(Edits, Manifest0),
                    Replica = Replica0#{
                        manifest := Manifest,
                        seq := EditSeq
                    },
                    SetRange = #set_range{
                        stream = StreamId,
                        counter = Counter,
                        first_offset = Manifest#manifest.first_offset,
                        first_timestamp = Manifest#manifest.first_timestamp,
                        next_offset = Manifest#manifest.next_offset
                    },
                    Effects = [SetRange | Effects0],
                    {State0#?MODULE{streams = Streams0#{StreamId := Replica}}, Effects};
                false ->
                    ?LOG_DEBUG(
                        "Replica received an out-of-sequence edit. Refreshing manifest from writer... (expected ~b, actual ~b)",
                        [Seq + 1, EditSeq]
                    ),
                    Effect = #manifest_requested{stream = StreamId, requester = self()},
                    Replica = Replica0#{manifest := {pending, []}},
                    {Replica, [Effect]}
            end;
        _ ->
            {State0, []}
    end;
apply(
    #{time := Ts} = Meta,
    #manifest_uploaded{stream = StreamId, entry = Entry},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{
            StreamId := #{
                kind := writer,
                pending_change := Pending,
                manifest := #manifest{revision = ExpectedRevision}
            } = Writer0
        } when
            Pending /= none
        ->
            case Entry of
                #{revision := ExpectedRevision} ->
                    Writer = Writer0#{
                        modifications := 0,
                        last_uploaded := Ts,
                        pending_change := none
                    },
                    State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
                    {State, []};
                #{revision := ActualRevision} ->
                    ?LOG_INFO(
                        "received #manifest_uploaded{} for unexpected revision (expected ~b, actual ~b)",
                        [ExpectedRevision, ActualRevision]
                    ),
                    Event = #manifest_upload_rejected{
                        stream = StreamId,
                        conflict = Entry
                    },
                    apply(Meta, Event, State0)
            end;
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #manifest_upload_rejected{stream = StreamId, conflict = #{epoch := NewEpoch}},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #{kind := writer, epoch := Epoch} = Writer0} ->
            case Epoch >= NewEpoch of
                true ->
                    %% If this is the latest-elected writer then there is a
                    %% deposed writer making changes. Re-resolve the manifest
                    %% and continue working as a writer.
                    Writer = Writer0#{
                        pending_change := none,
                        manifest := {pending, []}
                    },
                    State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
                    {State, [#resolve_manifest{stream = StreamId}]};
                false ->
                    %% This writer has been deposed. Stand down gracefully.
                    State = State0#?MODULE{streams = maps:remove(StreamId, Streams0)},
                    {State, []}
            end;
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #group_uploaded{
        stream = StreamId,
        entry = Entry,
        pos = Pos,
        len = Len
    },
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #{kind := writer, manifest := #manifest{} = Manifest0} = Writer0} ->
            %% NOTE: the entries array may have been modified in the meantime,
            %% but only from new fragments being uploaded and applied.
            %% (Retention and rebalancing are done exclusively of each other.)
            %% Because uploads only append to the tail of the entries array,
            %% `Pos` and `Len` point to the same section of entries regardless
            %% of changes to the manifest since the group upload started.
            Edit = (new_edit(Manifest0))#edit{entries = Entry, pos = Pos, len = Len},
            Manifest = apply_edit(Edit, Manifest0),
            Writer1 = Writer0#{manifest := Manifest},
            {Writer2, Effects0} = upload(StreamId, Writer1, []),
            {Writer, Effects} = notify_edits([Edit], false, StreamId, Writer2, Effects0),
            State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #writer_spawned{stream = StreamId, pid = Pid} = Event,
    #?MODULE{streams = Streams0} = State0
) ->
    Writer0 = writer(Event),
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
apply(
    _Meta,
    #acceptor_spawned{stream = StreamId, config = Config},
    #?MODULE{streams = Streams0} = State0
) ->
    Stream0 = replica(Config),
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
        #{StreamId := #{manifest := #manifest{} = Manifest} = Stream} ->
            Reply =
                case Requester of
                    {_, _} ->
                        #reply{to = Requester, response = Manifest};
                    _ when is_pid(Requester) ->
                        Message = #manifest_resolved{
                            stream = StreamId,
                            manifest = Manifest,
                            seq = maps:get(seq, Stream, undefined)
                        },
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
                first_timestamp = FirstTs,
                next_offset = NextOffset
            } = Manifest,
        seq = Seq0
    } = Event0,
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #{manifest := {pending, Requesters}} = Stream0} ->
            Stream1 = Stream0#{manifest := Manifest},
            Effects0 =
                case Stream0 of
                    #{counter := Counter} ->
                        SetRange = #set_range{
                            stream = StreamId,
                            counter = Counter,
                            first_offset = FirstOffset,
                            first_timestamp = FirstTs,
                            next_offset = NextOffset
                        },
                        [SetRange];
                    _ ->
                        []
                end,
            Seq =
                case Stream1 of
                    #{kind := writer, seq := WriterSeq0} ->
                        ?assertEqual(undefined, Seq0),
                        WriterSeq0 + 1;
                    _ ->
                        ?assertNotEqual(undefined, Seq0),
                        Seq0
                end,
            Event = Event0#manifest_resolved{seq = Seq},
            Stream2 = Stream1#{seq := Seq},
            %% NOTE: `Requesters` is in reverse order.
            Effects1 = lists:foldl(
                fun
                    ({_, _} = R, Acc) ->
                        [#reply{to = R, response = Manifest} | Acc];
                    (Pid, Acc) when is_pid(Pid) ->
                        [#send{to = Pid, message = Event} | Acc]
                end,
                Effects0,
                Requesters
            ),
            case Stream2 of
                #{kind := writer, available_fragments := Available0} ->
                    %% If there is a hole between the last fragments in the
                    %% manifest and the first available fragment, backfill
                    %% fragments from local stream data.
                    Effects2 = maybe_find_fragments(StreamId, Stream1, Effects1),
                    %% Drop all available fragments which are older than what
                    %% has already been uploaded to the remote tier.
                    Available = lists:filter(
                        fun(#fragment{first_offset = Offset}) ->
                            Offset >= NextOffset
                        end,
                        Available0
                    ),
                    Stream3 = Stream2#{
                        last_uploaded := Ts,
                        available_fragments := Available
                    },
                    {Stream, Effects} = upload_available_fragments(StreamId, Stream3, Effects2),
                    State = State0#?MODULE{streams = Streams0#{StreamId := Stream}},
                    {State, Effects};
                _ ->
                    State = State0#?MODULE{streams = Streams0#{StreamId := Stream2}},
                    {State, Effects1}
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
                    #{
                        kind := writer,
                        manifest := #manifest{},
                        pending_change := none
                    } = Writer0,
                    {Streams1, Effs0}
                ) ->
                    {Writer1, Edits, Effs1} = evaluate_retention(
                        Meta,
                        StreamId,
                        Writer0,
                        [],
                        Effs0
                    ),
                    {Writer2, Effs2} = evaluate_upload(Cfg, Meta, StreamId, Writer1, Effs1),
                    {Writer, Effs} = notify_edits(Edits, false, StreamId, Writer2, Effs2),
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
        #{StreamId := #{kind := writer, manifest := Manifest} = Writer0} ->
            Writer1 = Writer0#{retention := Retention},
            case Manifest of
                #manifest{} ->
                    {Writer2, Edits, Effects0} = evaluate_retention(
                        Meta,
                        StreamId,
                        Writer1,
                        [],
                        []
                    ),
                    {Writer, Effects} = notify_edits(Edits, false, StreamId, Writer2, Effects0),
                    State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
                    {State, Effects};
                _ ->
                    %% Manifest is pending, can't evaluate retention yet.
                    State = State0#?MODULE{streams = Streams0#{StreamId := Writer1}},
                    {State, []}
            end;
        _ ->
            {State0, []}
    end;
apply(
    _Meta,
    #retention_executed{stream = StreamId, edit = Edit},
    #?MODULE{streams = Streams0} = State0
) ->
    case Streams0 of
        #{StreamId := #{kind := writer, manifest := #manifest{} = Manifest0} = Writer0} ->
            Manifest = apply_edit(Edit, Manifest0),
            Writer1 = Writer0#{manifest := Manifest},
            {Writer2, Effects0} = upload(StreamId, Writer1, []),
            {Writer, Effects} = notify_edits([Edit], false, StreamId, Writer2, Effects0),
            State = State0#?MODULE{streams = Streams0#{StreamId := Writer}},
            {State, Effects};
        _ ->
            {State0, []}
    end;
apply(_Meta, #stream_deleted{stream = StreamId}, #?MODULE{streams = Streams0} = State0) ->
    State = State0#?MODULE{streams = maps:remove(StreamId, Streams0)},
    {State, []};
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
    #fragment_info{first_offset = InfoOffset} = Info,
    [#fragment_info{first_offset = HeadOffset} = Head | Rest],
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
    [#fragment_info{first_offset = FirstOffset, next_offset = NextOffset} = Info | Rest],
    Acc
) when NextTieredOffset =:= FirstOffset ->
    split_uploaded_infos(NextOffset, Rest, [Info | Acc]);
split_uploaded_infos(NextTieredOffset, PendingUploaded, Acc) ->
    {NextTieredOffset, PendingUploaded, lists:reverse(Acc)}.

-spec new_edit(#manifest{}) -> #edit{}.
new_edit(#manifest{
    first_offset = FirstOffset,
    first_timestamp = FirstTs,
    first_last_timestamp = FirstLastTs,
    next_offset = NextOffset
}) ->
    #edit{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = NextOffset
    }.

-doc """
Create an edit that adds successfully uploaded fragments to their manifest
entries array.

`Infos` is expected to be sorted by offset ascending.
""".
-spec apply_infos([#fragment_info{}], #manifest{}) ->
    {ok, #edit{} | undefined} | {error, #fragment_info{}}.
apply_infos(Infos, #manifest{entries = Entries} = Manifest) ->
    Edit0 = (new_edit(Manifest))#edit{pos = byte_size(Entries)},
    apply_infos0(Infos, Edit0).

apply_infos0([], Edit) ->
    {ok, Edit};
apply_infos0(
    [
        #fragment_info{
            first_offset = Offset,
            next_offset = NextOffset,
            first_timestamp = FirstTs,
            last_timestamp = LastTs,
            seq_no = SeqNo,
            size = Size
        }
        | Rest
    ],
    #edit{
        next_offset = Offset,
        size = Size0,
        entries = Entries0
    } = Edit0
) ->
    Edit1 =
        case Offset of
            0 ->
                %% For the very first fragment, also set the offset and timestamps.
                Edit0#edit{
                    first_offset = Offset,
                    first_timestamp = FirstTs,
                    first_last_timestamp = LastTs
                };
            _ ->
                Edit0
        end,
    IsSeqZero =
        case SeqNo of
            0 -> 1;
            _ -> 0
        end,
    Edit = Edit1#edit{
        next_offset = NextOffset,
        size = Size0 + Size,
        entries = <<Entries0/binary, ?FRAGMENT(Offset, FirstTs, LastTs, IsSeqZero, Size)/binary>>
    },
    apply_infos0(Rest, Edit);
apply_infos0([Info | _], #edit{}) ->
    {error, Info}.

-spec apply_edits([#edit{}], #manifest{}) -> #manifest{}.
apply_edits(Edits, Manifest) ->
    lists:foldl(fun apply_edit/2, Manifest, Edits).

-spec apply_edit(#edit{}, #manifest{}) -> #manifest{}.
apply_edit(
    #edit{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = EditNextOffset,
        size = Size,
        entries = EditEntries,
        pos = Pos,
        len = Len
    },
    #manifest{
        next_offset = ManifestNextOffset,
        total_size = TotalSize0,
        entries = Entries0
    } = Manifest0
) ->
    Entries =
        if
            %% Pure insertion (append)
            Pos =:= byte_size(Entries0) andalso Len =:= 0 ->
                <<Entries0/binary, EditEntries/binary>>;
            %% No-op (empty)
            Len =:= 0 andalso EditEntries =:= <<>> ->
                Entries0;
            %% Pure deletion (truncate)
            EditEntries =:= <<>> ->
                %% We only truncate from the beginning. No hole punching.
                ?assertEqual(0, Pos),
                binary:part(Entries0, Len, byte_size(Entries0) - Len);
            %% Replacement (for rebalancing)
            true ->
                <<
                    (binary:part(Entries0, 0, Pos))/binary,
                    EditEntries/binary,
                    (binary:part(Entries0, Pos + Len, byte_size(Entries0) - Pos - Len))/binary
                >>
        end,
    NextOffset =
        case EditNextOffset of
            undefined ->
                ManifestNextOffset;
            _ when is_integer(EditNextOffset) ->
                EditNextOffset
        end,
    Manifest0#manifest{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        next_offset = NextOffset,
        total_size = TotalSize0 + Size,
        entries = Entries
    }.

-spec notify_edits([#edit{}], SuggestRetention :: boolean(), stream_id(), writer(), [effect()]) ->
    {writer(), [effect()]}.
notify_edits([], _SuggestRetention, _StreamId, Writer, Effects) ->
    {Writer, Effects};
notify_edits(
    Edits0,
    SuggestRetention,
    StreamId,
    #{
        dir := Dir,
        shared := Shared,
        counter := Counter,
        replica_nodes := ReplicaNodes,
        seq := Seq0,
        manifest := #manifest{
            first_offset = FirstOffset,
            first_timestamp = FirstTs,
            next_offset = NextOffset
        }
    } = Writer0,
    Effects0
) ->
    %% The list is prepended, so the latest edits are at the front. Reverse for
    %% a more natural order.
    Edits = lists:reverse(Edits0),
    Seq = Seq0 + 1,
    Effects1 =
        lists:foldl(
            fun(ReplicaNode, Acc) ->
                Event = #manifest_edited{
                    stream = StreamId,
                    edits = Edits,
                    seq = Seq,
                    trigger_retention = SuggestRetention
                },
                Effect = #send{
                    to = {?SERVER, ReplicaNode},
                    message = Event,
                    options = [noconnect]
                },
                [Effect | Acc]
            end,
            Effects0,
            ReplicaNodes
        ),
    Effects2 =
        case SuggestRetention of
            true ->
                TriggerRetention = #trigger_retention{
                    stream = StreamId,
                    dir = Dir,
                    shared = Shared,
                    counter = Counter
                },
                [TriggerRetention | Effects1];
            false ->
                Effects1
        end,
    SetRange = #set_range{
        stream = StreamId,
        counter = Counter,
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        next_offset = NextOffset
    },
    {Writer0#{seq := Seq}, [SetRange | Effects2]}.

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

-spec evaluate_writer(cfg(), metadata(), stream_id(), writer(), [#edit{}], [effect()]) ->
    {writer(), [#edit{}], [effect()]}.
evaluate_writer(Cfg, Meta, StreamId, Writer0, Edits0, Effects0) ->
    {Writer1, Edits, Effects1} = evaluate_retention(Meta, StreamId, Writer0, Edits0, Effects0),
    {Writer2, Effects2} = evaluate_rebalance(Cfg, StreamId, Writer1, Effects1),
    {Writer, Effects} = evaluate_upload(Cfg, Meta, StreamId, Writer2, Effects2),
    {Writer, Edits, Effects}.

-doc """
Determine whether a stream's retention rules should delete fragments from the
head of the stream. If so, update the manifest and create effects to perform
the necessary deletion(s) so that the retention rules are satisfied.
""".
-spec evaluate_retention(metadata(), stream_id(), writer(), [#edit{}], [effect()]) ->
    {writer(), [#edit{}], [effect()]}.
evaluate_retention(
    #{time := Now},
    StreamId,
    #{
        pending_change := none,
        retention := RetentionSpec,
        manifest := #manifest{
            total_size = TotalSize0,
            first_last_timestamp = FirstLastTs,
            entries = Entries0
        } = Manifest0
    } = Writer0,
    Edits0,
    Effects0
) ->
    ExceedsRetention =
        case RetentionSpec of
            _ when byte_size(Entries0) =< ?ENTRY_B ->
                %% Nothing to reclaim!
                false;
            #{max_bytes := MaxBytes} when TotalSize0 > MaxBytes ->
                true;
            #{max_age := MaxAge} when Now - FirstLastTs > MaxAge ->
                true;
            _ ->
                false
        end,
    case ExceedsRetention of
        true ->
            case Entries0 of
                ?FRAGMENT(_O, _FTs, _LTs, _Sq, _Sz, _) ->
                    %% Groups are created at the array's beginning. If the
                    %% first entry is a fragment then this entries array
                    %% contains no groups.
                    GetGroupFun = fun unreachable/1,
                    case execute_retention(Manifest0, Now, RetentionSpec, GetGroupFun) of
                        {_NoEdit, []} ->
                            %% This shouldn't happen. If retention rules are
                            %% out-of-order then we should be able to reclaim
                            %% at least one fragment.
                            ?LOG_WARNING(
                                "Retention did not reclaim any fragments even though retention is required. Stream '~ts'",
                                [StreamId]
                            ),
                            {Writer0, Edits0, Effects0};
                        {Edit, Offsets} ->
                            Manifest = apply_edit(Edit, Manifest0),
                            Writer1 = Writer0#{manifest := Manifest},
                            {Writer, Effects1} = upload(StreamId, Writer1, Effects0),
                            DeleteFragments = #delete_objects{stream = StreamId, objects = Offsets},
                            {Writer, [Edit | Edits0], [DeleteFragments | Effects1]}
                    end;
                _ ->
                    EvaluateRetention = #evaluate_retention{
                        stream = StreamId,
                        manifest = Manifest0,
                        retention_spec = RetentionSpec,
                        now = Now
                    },
                    Writer = Writer0#{pending_change := retention},
                    Effects = [EvaluateRetention | Effects0],
                    {Writer, Edits0, Effects}
            end;
        false ->
            {Writer0, Edits0, Effects0}
    end;
evaluate_retention(_Meta, _StreamId, Writer, Edits, Effects) ->
    {Writer, Edits, Effects}.

-spec unreachable(any()) -> no_return().
unreachable(_) -> erlang:error(unreachable).

-spec execute_retention(
    #manifest{},
    osiris:timestamp(),
    rabbitmq_stream_s3:retention_spec(),
    fun((#group_ref{}) -> rabbitmq_stream_s3:entries())
) -> {#edit{}, [osiris:offset() | #group_ref{}]}.
execute_retention(
    #manifest{entries = Entries, total_size = TotalSize0} = Manifest,
    Now,
    RetentionSpec,
    GetGroupFun
) ->
    Edit0 = new_edit(Manifest),
    %% Retention does not affect the tail of the entries array. Clear
    %% the next_offset to avoid clobbering edits made to the manifest
    %% tail during asynchronous retention evaluation.
    Edit1 = Edit0#edit{next_offset = undefined},
    {false, Edit, _TotalSize, Deletions} = execute_retention(
        Entries,
        Edit1,
        TotalSize0,
        Now,
        RetentionSpec,
        GetGroupFun,
        true,
        []
    ),
    {Edit, lists:reverse(Deletions)}.

%% NOTE: keep at least one entry so that we can set the `first_offset` and
%% `first_timestamp` (`Rest /= <<>>`).
execute_retention(
    ?ENTRY(_, _, _, _, Rest) = Entries,
    #edit{first_offset = FirstOffset} = Edit,
    TotalSize,
    Now,
    Spec,
    GetGroupFun,
    IsRoot,
    Deletions
) ->
    case Rest of
        ?ENTRY(RightOffset, _, _, _, _) when RightOffset < FirstOffset ->
            execute_retention(Rest, Edit, TotalSize, Now, Spec, GetGroupFun, IsRoot, Deletions);
        _ ->
            execute_retention1(Entries, Edit, TotalSize, Now, Spec, GetGroupFun, IsRoot, Deletions)
    end;
execute_retention(Entries, Edit, TotalSize, Now, Spec, GetGroupFun, IsRoot, Deletions) ->
    execute_retention1(Entries, Edit, TotalSize, Now, Spec, GetGroupFun, IsRoot, Deletions).

execute_retention1(
    ?FRAGMENT(Offset, _FTs, _LTs, _Sq, _Sz, Rest),
    #edit{first_offset = FirstOffset} = Edit,
    TotalSize,
    Now,
    Spec,
    GetGroupFun,
    IsRoot,
    Deletions
) when Offset < FirstOffset ->
    execute_retention1(Rest, Edit, TotalSize, Now, Spec, GetGroupFun, IsRoot, Deletions);
execute_retention1(
    ?FRAGMENT(Offset, _FTs, _LTs, _Sq, Size, Rest),
    #edit{size = Size0, len = Len0} = Edit0,
    TotalSize0,
    Now,
    #{max_bytes := MaxBytes} = Spec,
    GetGroupFun,
    IsRoot,
    Deletions
) when TotalSize0 > MaxBytes andalso (Rest /= <<>> orelse not IsRoot) ->
    TotalSize = TotalSize0 - Size,
    Edit1 = Edit0#edit{size = Size0 - Size},
    Edit =
        case IsRoot of
            true ->
                Edit1#edit{len = Len0 + ?ENTRY_B};
            false ->
                Edit1
        end,
    execute_retention1(Rest, Edit, TotalSize, Now, Spec, GetGroupFun, IsRoot, [Offset | Deletions]);
execute_retention1(
    ?FRAGMENT(Offset, _FTs, LastTs, _Sq, Size, Rest),
    #edit{size = Size0, len = Len0} = Edit0,
    TotalSize0,
    Now,
    #{max_age := MaxAge} = Spec,
    GetGroupFun,
    IsRoot,
    Deletions
) when Now - LastTs > MaxAge andalso (Rest /= <<>> orelse not IsRoot) ->
    TotalSize = TotalSize0 - Size,
    Edit1 = Edit0#edit{size = Size0 - Size},
    Edit =
        case IsRoot of
            true ->
                Edit1#edit{len = Len0 + ?ENTRY_B};
            false ->
                Edit1
        end,
    execute_retention1(Rest, Edit, TotalSize, Now, Spec, GetGroupFun, IsRoot, [Offset | Deletions]);
execute_retention1(
    ?FRAGMENT(Offset, FTs, LTs, _Sq, _Sz, _Rest),
    Edit0,
    TotalSize,
    _Now,
    _Spec,
    _GetGroupFun,
    _IsRoot,
    Deletions
) ->
    Edit = Edit0#edit{
        first_offset = Offset,
        first_timestamp = FTs,
        first_last_timestamp = LTs
    },
    {false, Edit, TotalSize, Deletions};
execute_retention1(
    ?GROUP(Offset, _FTs, _LTs, Kind, Uid, Rest),
    #edit{len = Len0} = Edit0,
    TotalSize0,
    Now,
    Spec,
    GetGroupFun,
    IsRoot,
    Deletions0
) ->
    %% Rebalancing always keeps one fragment at the end of the root.
    ?assert(Rest =/= <<>> orelse not IsRoot),
    GroupRef = #group_ref{uid = Uid, kind = Kind, offset = Offset},
    case GetGroupFun(GroupRef) of
        {ok, ChildEntries} ->
            {Continue, Edit1, TotalSize, Deletions1} = execute_retention(
                ChildEntries,
                Edit0,
                TotalSize0,
                Now,
                Spec,
                GetGroupFun,
                false,
                Deletions0
            ),
            case Continue of
                true ->
                    Edit =
                        case IsRoot of
                            true ->
                                Edit1#edit{len = Len0 + ?ENTRY_B};
                            false ->
                                Edit1
                        end,
                    Deletions = [GroupRef | Deletions1],
                    execute_retention1(
                        Rest, Edit, TotalSize, Now, Spec, GetGroupFun, IsRoot, Deletions
                    );
                false ->
                    {false, Edit1, TotalSize, Deletions1}
            end;
        {error, not_found} ->
            execute_retention1(Rest, Edit0, TotalSize0, Now, Spec, GetGroupFun, IsRoot, Deletions0)
    end;
execute_retention1(<<>>, Edit, TotalSize, _Now, _Spec, _GetGroupFun, IsRoot, Deletions) ->
    ?assertNot(IsRoot),
    {true, Edit, TotalSize, Deletions}.

-doc """
Evaluate whether the array `#manifest.entries` is too large and a group should
be introduced to reduce memory costs.

See the "rebalancing" section of the `overview.md` doc.
""".
-spec evaluate_rebalance(cfg(), stream_id(), writer(), [effect()]) -> {writer(), [effect()]}.
evaluate_rebalance(
    #{rebalance_factor := RebalanceFactor},
    StreamId,
    #{pending_change := none, manifest := #manifest{entries = Entries}} = Writer0,
    Effects0
) when byte_size(Entries) >= RebalanceFactor * ?ENTRY_B ->
    case rebalance(RebalanceFactor, Entries) of
        {Kind, GroupEntries, Pos, Len} ->
            Writer = Writer0#{pending_change := rebalance},
            UploadGroup = #upload_group{
                stream = StreamId,
                kind = Kind,
                entries = GroupEntries,
                pos = Pos,
                len = Len
            },
            {Writer, [UploadGroup | Effects0]};
        undefined ->
            {Writer0, Effects0}
    end;
evaluate_rebalance(_Cfg, _StreamId, Writer, Effects) ->
    {Writer, Effects}.

-spec rebalance(Factor :: non_neg_integer(), rabbitmq_stream_s3:entries()) ->
    {
        rabbitmq_stream_s3:kind(),
        rabbitmq_stream_s3:entries(),
        Pos :: non_neg_integer(),
        Len :: pos_integer()
    }
    | undefined.
rebalance(Factor, Entries) ->
    rebalance(Factor, Entries, 0).

rebalance(Factor, Entries, Idx) when
    byte_size(Entries) - (Idx * ?ENTRY_B) >= (Factor * ?ENTRY_B)
->
    ?ENTRY(_, _, _, Kind, _) = rabbitmq_stream_s3_array:at(
        Idx,
        ?ENTRY_B,
        Entries
    ),
    %% Scan ahead to the entry which would satisfy the branching factor.
    %% If this entry has the same kind then it is the last entry in the new
    %% group.
    GroupEndIdx =
        case Kind of
            ?MANIFEST_KIND_FRAGMENT ->
                %% Effectively increase `Factor` by one when considering
                %% rebalancing fragments. This ensures that we always keep
                %% at least one fragment at the tail.
                Idx + Factor;
            _ ->
                Idx + Factor - 1
        end,
    case rabbitmq_stream_s3_array:try_at(GroupEndIdx, ?ENTRY_B, Entries) of
        ?ENTRY(_, _, _, Kind, _) ->
            %% If the kind is the same, this chunk of entries can be extracted
            %% as a group.
            GroupKind = rabbitmq_stream_s3:next_group(Kind),
            Pos = Idx * ?ENTRY_B,
            Len = Factor * ?ENTRY_B,
            GroupEntries = binary:part(Entries, Pos, Len),
            {GroupKind, GroupEntries, Pos, Len};
        _ ->
            NextSmallestIdx = rabbitmq_stream_s3_array:partition_point(
                fun(?ENTRY(_O, _FTs, _LTs, K, _)) -> K >= Kind end,
                ?ENTRY_B,
                Entries
            ),
            case NextSmallestIdx of
                Idx ->
                    undefined;
                NextIdx ->
                    %% Otherwise continue scanning to find the next kind.
                    rebalance(Factor, Entries, NextIdx)
            end
    end;
rebalance(_Factor, _Entries, _Pos) ->
    undefined.

-spec evaluate_upload(cfg(), metadata(), stream_id(), writer(), [effect()]) ->
    {writer(), [effect()]}.
evaluate_upload(
    Cfg,
    #{time := Ts},
    StreamId,
    #{
        kind := writer,
        modifications := Mods,
        last_uploaded := LastUploadTs,
        pending_change := none
    } = Writer,
    Effects
) ->
    ExceedsDebounce =
        case Cfg of
            #{debounce_modifications := M} when Mods >= M ->
                true;
            #{debounce_milliseconds := Millis} when Ts - LastUploadTs > Millis andalso Mods > 0 ->
                true;
            _ ->
                false
        end,
    case ExceedsDebounce of
        true ->
            upload(StreamId, Writer, Effects);
        false ->
            {Writer, Effects}
    end;
evaluate_upload(_Cfg, _Meta, _StreamId, Writer, Effects) ->
    {Writer, Effects}.

-spec upload(stream_id(), writer(), [effect()]) -> {writer(), [effect()]}.
upload(
    StreamId,
    #{
        kind := writer,
        manifest := #manifest{revision = Revision0} = Manifest0,
        epoch := Epoch,
        reference := Reference
    } = Writer0,
    Effects0
) ->
    UploadManifest = #upload_manifest{
        stream = StreamId,
        epoch = Epoch,
        reference = Reference,
        manifest = Manifest0
    },
    Writer = Writer0#{
        pending_change := upload,
        manifest := Manifest0#manifest{revision = Revision0 + 1}
    },
    {Writer, [UploadManifest | Effects0]}.

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

-spec format(state()) -> map().
format(#?MODULE{cfg = Cfg, streams = Streams}) ->
    #{
        cfg => Cfg,
        streams => #{StreamId => format_stream(Stream) || StreamId := Stream <- Streams}
    }.

-spec format_stream(stream()) -> map().
format_stream(#{kind := replica, manifest := Manifest} = Replica0) ->
    Replica0#{manifest := format_manifest(Manifest)};
format_stream(
    #{
        kind := writer,
        manifest := Manifest,
        last_uploaded := LastUploaded,
        available_fragments := Available,
        uploaded_fragments := Uploaded,
        retention := Retention
    } = Writer0
) ->
    %% No point in printing these:
    Writer1 = maps:without([shared, counter], Writer0),
    %% Format reference as "queue 'queue name' in vhost 'vhost name'"?
    %% Depending on changes to rabbitmq_stream_s3_db we may not need to carry
    %% the reference in state, so maybe this is not worthwhile.
    Writer1#{
        manifest := format_manifest(Manifest),
        last_uploaded := format_timestamp(LastUploaded),
        available_fragments := [
            {O, N}
         || #fragment{first_offset = O, next_offset = N} <- Available
        ],
        uploaded_fragments := [
            {O, N}
         || #fragment_info{first_offset = O, next_offset = N} <- Uploaded
        ],
        retention := format_retention(Retention)
    }.

format_manifest({pending, _Requesters}) ->
    pending;
format_manifest(#manifest{
    first_offset = FirstOffset,
    first_timestamp = FirstTs,
    first_last_timestamp = FirstLastTs,
    next_offset = NextOffset,
    total_size = TotalSize,
    revision = Revision,
    entries = Entries
}) ->
    Format0 =
        case rabbitmq_stream_s3_array:last(?ENTRY_B, Entries) of
            ?ENTRY(LastOffset, _FTs, LastTs, _K, _) ->
                #{
                    last_offset => LastOffset,
                    last_timestamp => format_timestamp(LastTs)
                };
            undefined ->
                #{}
        end,
    Format0#{
        first_offset => FirstOffset,
        first_timestamp => format_timestamp(FirstTs),
        first_last_timestamp => format_timestamp(FirstLastTs),
        next_offset => NextOffset,
        total_size => format_size(TotalSize),
        revision => Revision,
        entries => format_entries(Entries)
    }.

-spec format_timestamp(osiris:timestamp()) -> binary() | string().
format_timestamp(Ts) when is_integer(Ts) ->
    %% OTP-28 adds {return,binary} to this function. OTP-27 and below discard
    %% it. On OTP-28 this function returns a binary but on OTP-27 and below
    %% this function returns a string.
    calendar:system_time_to_rfc3339(Ts, [{unit, millisecond}, {return, binary}, {offset, "Z"}]).

-spec format_size(Bytes :: non_neg_integer()) -> binary().
format_size(Size) when Size < 1024 ->
    <<(integer_to_binary(Size))/binary, " B">>;
format_size(Size) ->
    format_size(Size / 1024.0, [$k, $M, $G, $T, $P, $E, $Z]).

format_size(Size, [Metric | _]) when Size < 1024.0 ->
    <<(float_to_binary(Size, [{decimals, 3}, compact]))/binary, " ", Metric, "iB">>;
format_size(Size, [_ | Metrics]) ->
    format_size(Size / 1024.0, Metrics).

format_entries(Entries) ->
    EntriesB = byte_size(Entries),
    (count_kinds(Entries, #{}))#{
        size => format_size(EntriesB),
        len => EntriesB div ?ENTRY_B
    }.

count_kinds(<<>>, Counts) ->
    Counts;
count_kinds(?ENTRY(_O, _FTs, _LTs, Kind, Rest), Counts0) ->
    Key =
        case Kind of
            ?MANIFEST_KIND_FRAGMENT -> fragments;
            ?MANIFEST_KIND_GROUP -> groups;
            ?MANIFEST_KIND_KILO_GROUP -> kilo_groups;
            ?MANIFEST_KIND_MEGA_GROUP -> mega_groups
        end,
    Counts = maps:update_with(Key, fun(N) -> N + 1 end, 1, Counts0),
    count_kinds(Rest, Counts).

format_retention(Retention) when map_size(Retention) =:= 0 ->
    none;
format_retention(Retention) ->
    maps:map(
        fun
            (max_age, MaxAge) ->
                format_duration_ms(MaxAge);
            (max_bytes, MaxBytes) ->
                format_size(MaxBytes)
        end,
        Retention
    ).

format_duration_ms(Duration) when is_integer(Duration) andalso Duration =< 1000 ->
    <<(integer_to_binary(Duration))/binary, "msec">>;
format_duration_ms(Duration) ->
    format_duration_seconds(Duration / 1000).

format_duration_seconds(Duration) when is_float(Duration) andalso Duration =< 60.0 ->
    <<?FORMAT_DURATION(Duration), "sec">>;
format_duration_seconds(Duration) when is_float(Duration) ->
    format_duration_minutes(Duration / 60).

format_duration_minutes(Duration) when is_float(Duration) andalso Duration =< 60.0 ->
    <<?FORMAT_DURATION(Duration), "min">>;
format_duration_minutes(Duration) when is_float(Duration) ->
    format_duration_hours(Duration / 60).

format_duration_hours(Duration) when is_float(Duration) andalso Duration =< 24.0 ->
    <<?FORMAT_DURATION(Duration), " hours">>;
format_duration_hours(Duration) when is_float(Duration) ->
    format_duration_days(Duration / 24).

format_duration_days(Duration) when is_float(Duration) andalso Duration =< 7.0 ->
    <<?FORMAT_DURATION(Duration), " days">>;
format_duration_days(Duration) when is_float(Duration) ->
    format_duration_weeks(Duration / 7).

format_duration_weeks(Duration) when is_float(Duration) andalso Duration =< 52.0 ->
    <<?FORMAT_DURATION(Duration), " weeks">>;
format_duration_weeks(Duration) when is_float(Duration) ->
    format_duration_years(Duration / 52).

format_duration_years(Duration) when is_float(Duration) ->
    <<?FORMAT_DURATION(Duration), " years">>.

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
                first_offset = N * 20,
                first_timestamp = Ts + N,
                last_timestamp = Ts + N + 20,
                next_offset = N * 20 + 20,
                seq_no = N,
                size = 200
            }
         || N <- lists:seq(0, 2)
        ],
    ?assertMatch(
        {ok, #edit{first_timestamp = Ts, next_offset = 60}},
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

root_retention_test() ->
    %% When a manifest root only points to fragments, we can figure out which
    %% fragments to delete in-place without kicking off a task involving
    %% downloading group objects.
    Ts = erlang:system_time(millisecond),
    Entries = <<
        ?FRAGMENT((N * 20), (Ts - 100 + (N - 1) * 20), (Ts - 100 + N * 20), 0, 200)
     || N <- lists:seq(0, 4)
    >>,
    Manifest = #manifest{
        first_offset = 0,
        first_timestamp = Ts - 120,
        first_last_timestamp = Ts - 100,
        next_offset = 6 * 20,
        total_size = 1000,
        entries = Entries
    },
    ExecuteRetention = fun(Spec) ->
        execute_retention(Manifest, Ts, Spec, fun unreachable/1)
    end,
    %% No retention spec, nothing to do.
    ?assertMatch(
        {#edit{size = 0}, []},
        ExecuteRetention(#{})
    ),

    %% == MAX BYTES ==
    ?assertMatch(
        {#edit{first_offset = 0, size = 0}, []},
        ExecuteRetention(#{max_bytes => 1000})
    ),
    ?assertMatch(
        {#edit{first_offset = 20, size = -200}, [0]},
        ExecuteRetention(#{max_bytes => 900})
    ),
    ?assertMatch(
        {#edit{first_offset = 60, size = -600}, [0, 20, 40]},
        ExecuteRetention(#{max_bytes => 500})
    ),
    %% Make sure we keep at least one entry.
    ?assertMatch(
        {#edit{first_offset = 80, size = -800}, [0, 20, 40, 60]},
        ExecuteRetention(#{max_bytes => 100})
    ),

    %% == MAX AGE ==
    ?assertMatch(
        {#edit{first_offset = 0, size = 0}, []},
        ExecuteRetention(#{max_age => 100_000})
    ),
    ?assertMatch(
        {#edit{first_offset = 20, size = -200}, [0]},
        ExecuteRetention(#{max_age => 99})
    ),
    ?assertMatch(
        {#edit{first_offset = 60, size = -600}, [0, 20, 40]},
        ExecuteRetention(#{max_age => 59})
    ),
    ?assertMatch(
        {#edit{first_offset = 80, size = -800}, [0, 20, 40, 60]},
        ExecuteRetention(#{max_age => 1})
    ),

    ok.

recursive_retention_test() ->
    %% When a manifest contains groups we need to evaluate retention in a task,
    %% since we need to download groups. We might need to download groups
    %% recursively if the manifest has a kilo or mega group.
    %%
    %%     root
    %%     ├── KG0
    %%     │   ├── G0
    %%     │   │   ├── F0 (0)
    %%     │   │   └── F1 (20)
    %%     │   └── G1
    %%     │       ├── F2 (40)
    %%     │       └── F3 (60)
    %%     ├── G2
    %%     │   ├── F4 (80)
    %%     │   └── F5 (100)
    %%     └── F6 (120)
    Ts = erlang:system_time(millisecond),
    %% Rebalancing factor is 2 for simplicity.
    FragmentSize = 200,
    [F0, F1, F2, F3, F4, F5, F6] = [
        ?FRAGMENT(
            (N * 20),
            (Ts - 1000 + (N - 1) * 20),
            (Ts - 1000 + N * 20 - 1),
            0,
            FragmentSize
        )
     || N <- lists:seq(0, 6)
    ],
    %% Group 0 covers fragments 0 and 1, group 2 covers fragments 2 and 3, etc..
    [G0, G1, G2] = [
        ?GROUP(
            (N * 40),
            (Ts - 1000 + ((N * 2) - 1) * 20),
            (Ts - 1000 + (N * 2) * 20 - 1),
            ?MANIFEST_KIND_GROUP,
            (rabbitmq_stream_s3:uid())
        )
     || N <- lists:seq(0, 2)
    ],
    %% Kilo-group 0 covers groups 0 and 1 (i.e. fragments 1-3).
    KG0 = ?GROUP(
        0,
        (Ts - 1000 + -20),
        (Ts - 1000 + 39),
        ?MANIFEST_KIND_KILO_GROUP,
        (rabbitmq_stream_s3:uid())
    ),
    Entries = <<KG0/binary, G2/binary, F6/binary>>,
    Manifest = #manifest{
        first_offset = 0,
        first_timestamp = Ts - 1000 - 20,
        first_last_timestamp = Ts - 1000 - 1,
        next_offset = 7 * 20,
        total_size = 7 * 200,
        entries = Entries
    },
    GetGroupFun = fun
        (#group_ref{kind = ?MANIFEST_KIND_KILO_GROUP, offset = 0}) ->
            %% KG0
            {ok, <<G0/binary, G1/binary>>};
        (#group_ref{kind = ?MANIFEST_KIND_GROUP, offset = 0}) ->
            %% G0
            {ok, <<F0/binary, F1/binary>>};
        (#group_ref{kind = ?MANIFEST_KIND_GROUP, offset = 40}) ->
            %% G1
            {ok, <<F2/binary, F3/binary>>};
        (#group_ref{kind = ?MANIFEST_KIND_GROUP, offset = 80}) ->
            %% G2
            {ok, <<F4/binary, F5/binary>>}
    end,
    ExecuteRetention = fun(Spec) ->
        execute_retention(Manifest, Ts, Spec, GetGroupFun)
    end,

    %% No retention spec, no-op.
    ?assertMatch(
        {#edit{first_offset = 0, size = 0}, []},
        ExecuteRetention(#{})
    ),

    %% Recursively reclaim everything but the last fragment.
    ?assertMatch(
        {#edit{first_offset = 120, size = -1200}, [
            %% F0
            0,
            %% F1
            20,
            %% G0
            #group_ref{offset = 0, kind = ?MANIFEST_KIND_GROUP},
            %% F2
            40,
            %% F3
            60,
            %% G1
            #group_ref{offset = 40, kind = ?MANIFEST_KIND_GROUP},
            %% KG0
            #group_ref{offset = 0, kind = ?MANIFEST_KIND_KILO_GROUP},
            %% F4
            80,
            %% F5
            100,
            %% G2
            #group_ref{offset = 80, kind = ?MANIFEST_KIND_GROUP}
        ]},
        ExecuteRetention(#{max_bytes => FragmentSize})
    ),

    %% Reclaim just the kilo group.
    ?assertMatch(
        {#edit{first_offset = 80, len = ?ENTRY_B}, [
            %% F0
            0,
            %% F1
            20,
            %% G0
            #group_ref{offset = 0, kind = ?MANIFEST_KIND_GROUP},
            %% F2
            40,
            %% F3
            60,
            %% G1
            #group_ref{offset = 40, kind = ?MANIFEST_KIND_GROUP},
            %% KG0
            #group_ref{offset = 0, kind = ?MANIFEST_KIND_KILO_GROUP}
        ]},
        ExecuteRetention(#{max_bytes => 3 * FragmentSize})
    ),

    %% Reclaim G0. Then G1 (and KG0).
    {#edit{first_offset = 40, len = 0} = Edit1, [
        %% F0
        0,
        %% F1
        20,
        %% G0
        #group_ref{offset = 0, kind = ?MANIFEST_KIND_GROUP}
    ]} = ExecuteRetention(#{max_bytes => 5 * FragmentSize}),
    Manifest1 = apply_edit(Edit1, Manifest),
    ?assertEqual(5 * FragmentSize, Manifest1#manifest.total_size),
    {#edit{first_offset = 80, size = -400, len = ?ENTRY_B} = Edit3, [
        %% G0. Returned because looking it up was successful.
        #group_ref{offset = 0, kind = ?MANIFEST_KIND_GROUP},
        %% F2
        40,
        %% F3
        60,
        %% G1
        #group_ref{offset = 40, kind = ?MANIFEST_KIND_GROUP},
        %% KG0
        #group_ref{offset = 0, kind = ?MANIFEST_KIND_KILO_GROUP}
    ]} = execute_retention(Manifest1, Ts, #{max_bytes => 3 * FragmentSize}, GetGroupFun),
    Manifest2 = apply_edit(Edit3, Manifest1),
    ?assertEqual(3 * FragmentSize, Manifest2#manifest.total_size),

    ok.

format_size_test() ->
    ?assertEqual(<<"0 B">>, format_size(0)),
    ?assertEqual(<<"500 B">>, format_size(500)),
    ?assertEqual(<<"1.0 kiB">>, format_size(1024)),
    ?assertEqual(<<"1.205 kiB">>, format_size(1234)),
    ?assertEqual(<<"1.0 GiB">>, format_size(math:pow(1024, 3))),
    ?assertEqual(<<"1.5 GiB">>, format_size(math:pow(1024, 3) + math:pow(1024, 3) / 2)),
    ?assertEqual(<<"50.0 TiB">>, format_size(50 * math:pow(1024, 4))),
    ?assertEqual(<<"1.0 PiB">>, format_size(math:pow(1024, 5))),
    ok.

format_duration_test() ->
    ?assertEqual(<<"3.0 hours">>, format_duration_ms(3 * 60 * 60 * 1000)),
    ?assertEqual(<<"5.0 days">>, format_duration_ms(5 * 24 * 60 * 60 * 1000)),
    ?assertEqual(<<"7.0 weeks">>, format_duration_ms(7 * 7 * 24 * 60 * 60 * 1000)),
    ?assertEqual(<<"4.0 years">>, format_duration_ms(4 * 52 * 7 * 24 * 60 * 60 * 1000)),
    ok.

rebalance_group_test() ->
    Ts = erlang:system_time(millisecond),
    Entries = <<
        ?FRAGMENT((N * 20), (Ts - 100 + N * 20), (Ts - 100 + (N + 1) * 20), 0, 200)
     || N <- lists:seq(0, 4)
    >>,
    Factor = 3,
    Len = Factor * ?ENTRY_B,
    GroupEntries = binary:part(Entries, 0, Len),
    ?assertMatch(
        {?MANIFEST_KIND_GROUP, GroupEntries, 0, Len},
        rebalance(Factor, Entries)
    ),
    ok.

rebalance_kilo_group_test() ->
    Ts = erlang:system_time(millisecond),
    Groups = <<
        ?GROUP(
            (N * 20),
            (Ts - 100 + N * 20),
            (Ts - 100 + (N + 1) * 20),
            ?MANIFEST_KIND_GROUP,
            (rabbitmq_stream_s3:uid())
        )
     || N <- lists:seq(1, 5)
    >>,
    %% Existing kilo group is before the groups...
    Entries = ?GROUP(
        0,
        (Ts - 120),
        (Ts - 100),
        ?MANIFEST_KIND_KILO_GROUP,
        (rabbitmq_stream_s3:uid()),
        Groups
    ),
    Factor = 3,
    Len = Factor * ?ENTRY_B,
    GroupEntries = binary:part(Entries, 1 * ?ENTRY_B, Len),
    ?assertMatch(
        {?MANIFEST_KIND_KILO_GROUP, GroupEntries, 1 * ?ENTRY_B, Len},
        rebalance(Factor, Entries)
    ),
    ok.

-endif.
