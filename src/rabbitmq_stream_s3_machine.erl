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
-dialyzer({no_unused, [format_size/1, format_size/2, format_entries/1, count_kinds/2]}).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-define(SERVER, rabbitmq_stream_s3_server).

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
        #{
            StreamId := #{
                kind := writer,
                epoch := Epoch,
                reference := Reference,
                manifest := #manifest{revision = Revision0} = Manifest0
            } = Writer0
        } ->
            %% NOTE: the entries array may have been modified in the meantime,
            %% but only from new fragments being uploaded and applied.
            %% (Retention and rebalancing are done exclusively of each other.)
            %% Because uploads only append to the tail of the entries array,
            %% `Pos` and `Len` point to the same section of entries regardless
            %% of changes to the manifest since the group upload started.
            Edit = (new_edit(Manifest0))#edit{entries = Entry, pos = Pos, len = Len},
            Manifest1 = apply_edit(Edit, Manifest0),
            UploadManifest = #upload_manifest{
                stream = StreamId,
                epoch = Epoch,
                reference = Reference,
                manifest = Manifest1
            },
            Manifest = Manifest1#manifest{revision = Revision0 + 1},
            Writer1 = Writer0#{manifest := Manifest},
            Effects0 = [UploadManifest],
            {Writer, Effects} = notify_edits([Edit], false, StreamId, Writer1, Effects0),
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
        #{StreamId := #{kind := writer, manifest := Manifest0} = Writer0} ->
            Writer1 = Writer0#{retention := Retention},
            case Manifest0 of
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

-spec new_edit(#manifest{}) -> #edit{}.
new_edit(#manifest{
    first_offset = FirstOffset,
    first_timestamp = FirstTs,
    next_offset = NextOffset,
    total_size = TotalSize
}) ->
    #edit{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        next_offset = NextOffset,
        total_size = TotalSize
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
            offset = Offset,
            next_offset = NextOffset,
            timestamp = Ts,
            seq_no = SeqNo,
            size = Size
        }
        | Rest
    ],
    #edit{
        next_offset = Offset,
        total_size = TotalSize0,
        entries = Entries0
    } = Edit0
) ->
    Edit1 =
        case Offset of
            0 ->
                %% For the very first fragment, also set the offset and timestamp.
                Edit0#edit{first_offset = Offset, first_timestamp = Ts};
            _ ->
                Edit0
        end,
    Edit = Edit1#edit{
        next_offset = NextOffset,
        total_size = TotalSize0 + Size,
        entries =
            <<Entries0/binary,
                ?ENTRY(
                    Offset,
                    Ts,
                    ?MANIFEST_KIND_FRAGMENT,
                    Size,
                    SeqNo,
                    rabbitmq_stream_s3:null_uid(),
                    <<>>
                )/binary>>
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
        next_offset = NextOffset,
        total_size = TotalSize,
        entries = EditEntries,
        pos = Pos,
        len = Len
    },
    #manifest{entries = Entries0} = Manifest0
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
    Manifest0#manifest{
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        next_offset = NextOffset,
        total_size = TotalSize,
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
        seq := Seq0
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
    %% The latest edit has the most up-to-date information on it.
    [#edit{first_offset = FirstOffset, first_timestamp = FirstTs, next_offset = NextOffset} | _] =
        Edits0,
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
        epoch := Epoch,
        reference := Reference,
        retention := RetentionSpec,
        manifest := #manifest{
            total_size = TotalSize0,
            first_timestamp = FirstTs,
            revision = Revision0,
            entries = Entries0
        } = Manifest0
    } = Writer0,
    Edits0,
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
                ?ENTRY(_Offset, _Timestamp, ?MANIFEST_KIND_FRAGMENT, _Size, _SeqNo, _Uid, _Rest) ->
                    %% In the common case a stream will not be so long that it
                    %% needs groups. Luckily, this means we can determine
                    %% which fragments can be deleted very efficiently by
                    %% looking just at manifest's entries array.
                    Edit0 = new_edit(Manifest0),
                    {Edit, Offsets} = evaluate_retention1(
                        Entries0,
                        Edit0,
                        Now,
                        RetentionSpec
                    ),
                    Manifest1 = apply_edit(Edit, Manifest0),
                    UploadManifest = #upload_manifest{
                        stream = StreamId,
                        epoch = Epoch,
                        reference = Reference,
                        manifest = Manifest1
                    },
                    Manifest = Manifest1#manifest{revision = Revision0 + 1},
                    Writer = Writer0#{manifest := Manifest, pending_change := retention},
                    DeleteFragments = #delete_fragments{stream = StreamId, offsets = Offsets},
                    Edits = [Edit | Edits0],
                    Effects = [UploadManifest, DeleteFragments | Effects0],
                    {Writer, Edits, Effects};
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

evaluate_retention1(Entries, Edit, Now, RetentionSpec) ->
    evaluate_retention1(Entries, Edit, Now, RetentionSpec, []).

%% NOTE: keep at least one entry so that we can set the `first_offset` and
%% `first_timestamp`.
evaluate_retention1(
    ?ENTRY(Offset, _Ts, Kind, Size, _SeqNo, _Uid, Rest),
    #edit{total_size = TotalSize0, len = Len0} = Edit0,
    Now,
    #{max_bytes := MaxBytes} = Spec,
    Offsets0
) when TotalSize0 > MaxBytes andalso Rest /= <<>> ->
    ?assertEqual(Kind, ?MANIFEST_KIND_FRAGMENT),
    Edit = Edit0#edit{
        total_size = TotalSize0 - Size,
        len = Len0 + ?ENTRY_B
    },
    evaluate_retention1(Rest, Edit, Now, Spec, [Offset | Offsets0]);
evaluate_retention1(
    ?ENTRY(Offset, Ts, Kind, Size, _SeqNo, _Uid, Rest),
    #edit{total_size = TotalSize0, len = Len0} = Edit0,
    Now,
    #{max_age := MaxAge} = Spec,
    Offsets0
) when Now - Ts > MaxAge andalso Rest /= <<>> ->
    ?assertEqual(Kind, ?MANIFEST_KIND_FRAGMENT),
    Edit = Edit0#edit{
        total_size = TotalSize0 - Size,
        len = Len0 + ?ENTRY_B
    },
    evaluate_retention1(Rest, Edit, Now, Spec, [Offset | Offsets0]);
evaluate_retention1(
    ?ENTRY(Offset, Ts, ?MANIFEST_KIND_FRAGMENT, _Size, _SeqNo, _Uid, _Rest),
    Edit0,
    _Now,
    _Spec,
    Offsets
) ->
    Edit = Edit0#edit{
        first_offset = Offset,
        first_timestamp = Ts
    },
    %% No real point to this lists:reverse/1. It just makes it appear nicer.
    {Edit, lists:reverse(Offsets)}.

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
    ?ENTRY(_, _, Kind, _, _, _, _) = rabbitmq_stream_s3_array:at(
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
        ?ENTRY(_, _, Kind, _, _, _, _) ->
            %% If the kind is the same, this chunk of entries can be extracted
            %% as a group.
            GroupKind = rabbitmq_stream_s3:next_group(Kind),
            Pos = Idx * ?ENTRY_B,
            Len = Factor * ?ENTRY_B,
            GroupEntries = binary:part(Entries, Pos, Len),
            {GroupKind, GroupEntries, Pos, Len};
        _ ->
            NextSmallestIdx = rabbitmq_stream_s3_array:partition_point(
                fun(?ENTRY(_O, _T, K, _Sz, _Sq, _U, _)) -> K >= Kind end,
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
        manifest := #manifest{revision = Revision0} = Manifest0,
        epoch := Epoch,
        reference := Reference,
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
            #{debounce_milliseconds := Millis} when Ts - LastUploadTs > Millis andalso Mods > 0 ->
                true;
            _ ->
                false
        end,
    case ExceedsDebounce of
        true ->
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
        uploaded_fragments := Uploaded
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
        uploaded_fragments := [{O, N} || #fragment_info{offset = O, next_offset = N} <- Uploaded]
    }.

format_manifest({pending, _Requesters}) ->
    pending;
format_manifest(#manifest{
    first_offset = FirstOffset,
    first_timestamp = FirstTs,
    next_offset = NextOffset,
    total_size = TotalSize,
    revision = Revision,
    entries = Entries
}) ->
    Format0 =
        case rabbitmq_stream_s3_array:last(?ENTRY_B, Entries) of
            ?ENTRY(LastOffset, LastTs, _K, _S, _Seq, _Uid, _) ->
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
        next_offset => NextOffset,
        total_size => format_size(TotalSize),
        revision => Revision,
        entries => format_entries(Entries)
    }.

-spec format_timestamp(osiris:timestamp()) -> binary().
format_timestamp(Ts) when is_integer(Ts) ->
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
count_kinds(?ENTRY(_O, _T, Kind, _Sz, _Sq, _U, Rest), Counts0) ->
    Key =
        case Kind of
            ?MANIFEST_KIND_FRAGMENT -> fragments;
            ?MANIFEST_KIND_GROUP -> groups;
            ?MANIFEST_KIND_KILO_GROUP -> kilo_groups;
            ?MANIFEST_KIND_MEGA_GROUP -> mega_groups
        end,
    Counts = maps:update_with(Key, fun(N) -> N + 1 end, 1, Counts0),
    count_kinds(Rest, Counts).

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

evaluate_retention1_test() ->
    Ts = erlang:system_time(millisecond),
    Entries = <<
        ?ENTRY(
            (N * 20),
            (Ts - 100 + N * 20),
            ?MANIFEST_KIND_FRAGMENT,
            200,
            N,
            rabbitmq_stream_s3:null_uid(),
            <<>>
        )
     || N <- lists:seq(0, 4)
    >>,
    Manifest = #manifest{
        first_offset = 0,
        first_timestamp = Ts - 100,
        next_offset = 6 * 20,
        total_size = 1000,
        entries = Entries
    },
    Edit = new_edit(Manifest),
    %% No retention spec, nothing to do.
    ?assertMatch(
        {Edit, []},
        evaluate_retention1(Entries, Edit, Ts, #{})
    ),

    %% == MAX BYTES ==
    ?assertMatch(
        {Edit, []},
        evaluate_retention1(Entries, Edit, Ts, #{max_bytes => 1000})
    ),
    ?assertMatch(
        {#edit{first_offset = 20, total_size = 800}, [0]},
        evaluate_retention1(Entries, Edit, Ts, #{max_bytes => 900})
    ),
    ?assertMatch(
        {#edit{first_offset = 60, total_size = 400}, [0, 20, 40]},
        evaluate_retention1(Entries, Edit, Ts, #{max_bytes => 500})
    ),
    %% Make sure we keep at least one entry.
    ?assertMatch(
        {#edit{first_offset = 80, total_size = 200}, [0, 20, 40, 60]},
        evaluate_retention1(Entries, Edit, Ts, #{max_bytes => 100})
    ),

    %% == MAX AGE ==
    ?assertMatch(
        {Edit, []},
        evaluate_retention1(Entries, Edit, Ts, #{max_age => 100_000})
    ),
    ?assertMatch(
        {#edit{first_offset = 20, total_size = 800}, [0]},
        evaluate_retention1(Entries, Edit, Ts, #{max_age => 99})
    ),
    ?assertMatch(
        {#edit{first_offset = 60, total_size = 400}, [0, 20, 40]},
        evaluate_retention1(Entries, Edit, Ts, #{max_age => 59})
    ),
    ?assertMatch(
        {#edit{first_offset = 80, total_size = 200}, [0, 20, 40, 60]},
        evaluate_retention1(Entries, Edit, Ts, #{max_age => 1})
    ),

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

rebalance_group_test() ->
    Ts = erlang:system_time(millisecond),
    Entries = <<
        ?ENTRY(
            (N * 20),
            (Ts - 100 + N * 20),
            ?MANIFEST_KIND_FRAGMENT,
            200,
            N,
            rabbitmq_stream_s3:null_uid(),
            <<>>
        )
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
        ?ENTRY(
            (N * 20),
            (Ts - 100 + N * 20),
            ?MANIFEST_KIND_GROUP,
            2000,
            N,
            rabbitmq_stream_s3:uid(),
            <<>>
        )
     || N <- lists:seq(1, 5)
    >>,
    %% Existing kilo group is before the groups...
    Entries = ?ENTRY(
        0,
        (Ts - 120),
        ?MANIFEST_KIND_KILO_GROUP,
        20_000,
        0,
        rabbitmq_stream_s3:uid(),
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
