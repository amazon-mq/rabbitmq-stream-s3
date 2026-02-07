%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_server).

-include_lib("kernel/include/logger.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-define(SERVER, ?MODULE).
-define(RANGE_TABLE, rabbitmq_stream_s3_log_manifest_range).

-behaviour(gen_server).

-rabbit_boot_step(
    {rabbitmq_stream_s3_log_manifest, [
        {description, "tiered storage S3 coordinator"},
        {mfa, {?MODULE, start, []}},
        {requires, pre_boot},
        {enables, core_initialized}
    ]}
).

%% records for the gen_server to handle:
-record(init_acceptor, {
    pid :: pid(),
    stream :: stream_id(),
    config :: osiris_log:config()
}).
-record(get_manifest, {stream :: stream_id()}).
-record(task_completed, {task_pid :: pid(), event :: event() | ok}).

-record(?MODULE, {
    machine = rabbitmq_stream_s3_machine:new() :: rabbitmq_stream_s3_machine:state(),
    tasks = #{} :: #{pid() => effect()},
    %% A lookup from stream reference to ID. This is used to determine the
    %% stream ID associated with an offset notification `{osiris_offset,
    %% Reference, osiris:offset()}` sent by the writer (because we used
    %% `osiris:register_offset_listener/3`).
    references = #{} :: #{stream_reference() => stream_id()}
}).

%% API
-export([
    get_manifest/1,
    init_acceptor/2,
    init_writer/3,
    fragment_available/2,
    retention_updated/2,
    delete_stream/1
]).

%% Useful for other modules
-export([
    get_fragment_trailer/1,
    get_fragment_trailer/2
]).

%% gen_server
-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    format_status/1,
    terminate/2,
    code_change/3
]).

%% For `sys:get_state/1` debugging.
-export([format_state/1]).

%% Need to be exported for `erlang:apply/3`.
-export([start/0, format_osiris_event/1, execute_task/2]).

%%----------------------------------------------------------------------------

%% This server needs to be started by a boot step so that it is online before
%% the stream coordinator. Otherwise the stream coordinator will attempt to
%% recover replicas before this server is started and writer_manifest/1 will
%% fail a few times and look messy in the logs.
start() ->
    ok = rabbitmq_stream_s3_api:init(),

    ok = rabbitmq_stream_s3_counters:init(),
    ok = rabbitmq_stream_s3_db:setup(),
    rabbit_sup:start_child(?MODULE).

-spec get_manifest(stream_id()) -> #manifest{} | undefined.
get_manifest(StreamId) ->
    gen_server:call(?SERVER, #get_manifest{stream = StreamId}, infinity).

-spec init_writer(stream_id(), osiris_log:config(), [#fragment{}]) -> ok.
init_writer(StreamId, Config, AvailableFragments) ->
    gen_server:cast(?SERVER, #writer_spawned{
        stream = StreamId,
        pid = self(),
        config = Config,
        available_fragments = AvailableFragments
    }).

-spec init_acceptor(stream_id(), osiris_log:config()) -> ok.
init_acceptor(StreamId, Config) ->
    gen_server:cast(?SERVER, #init_acceptor{
        stream = StreamId,
        config = Config,
        pid = self()
    }).

-spec retention_updated(stream_id(), [osiris:retention_spec()]) -> ok.
retention_updated(StreamId, Retention) ->
    gen_server:cast(?SERVER, #retention_updated{stream = StreamId, retention = Retention}).

-spec fragment_available(stream_id(), #fragment{}) -> ok.
fragment_available(StreamId, Fragment) ->
    gen_server:cast(?SERVER, #fragment_available{stream = StreamId, fragment = Fragment}).

-spec delete_stream(stream_id()) -> ok.
delete_stream(StreamId) ->
    gen_server:cast(?SERVER, #delete_stream{stream = StreamId}).

%%----------------------------------------------------------------------------

%%---------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    %% Table keyed by stream ID. This is used for local retention: segments
    %% with all offsets less than or equal to the last tiered offset can be
    %% deleted by retention since they are stored in the remote tier.
    _ = ets:new(?RANGE_TABLE, [named_table]),
    set_tick_timer(),
    {ok, #?MODULE{}}.

handle_call(#get_manifest{stream = StreamId}, From, State) ->
    %% TODO: this is too simplistic. Reading from the root manifest should be
    %% done within the server. And then the server should give a spec to
    %% readers to find anything within branches.
    Event = #manifest_requested{stream = StreamId, requester = From},
    {noreply, evolve_event(Event, State)};
handle_call(Request, From, State) ->
    ?LOG_INFO(?MODULE_STRING " received unexpected call from ~p: ~W", [From, Request, 10]),
    {noreply, State}.

handle_cast(
    #writer_spawned{stream = StreamId, config = #{reference := Reference}} = Event,
    #?MODULE{references = References0} = State0
) ->
    State1 = State0#?MODULE{references = References0#{Reference => StreamId}},
    {noreply, evolve_event(Event, State1)};
handle_cast(
    #init_acceptor{
        stream = StreamId,
        config =
            #{
                leader_pid := LeaderPid,
                reference := Reference
            } = Config
    },
    #?MODULE{references = References0} = State0
) ->
    ok = gen_server:cast({?SERVER, node(LeaderPid)}, #manifest_requested{
        stream = StreamId,
        requester = self()
    }),
    State1 = State0#?MODULE{references = References0#{Reference => StreamId}},
    Event = #acceptor_spawned{stream = StreamId, config = Config},
    {noreply, evolve_event(Event, State1)};
handle_cast(#manifest_requested{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_cast(
    #task_completed{task_pid = TaskPid, event = Event},
    #?MODULE{tasks = Tasks0} = State0
) ->
    %% assertion
    #{TaskPid := _Effect} = Tasks0,
    Tasks = maps:remove(TaskPid, Tasks0),
    State1 = State0#?MODULE{tasks = Tasks},
    State =
        case Event of
            ok ->
                State1;
            _ ->
                evolve_event(Event, State1)
        end,
    {noreply, State};
handle_cast(#fragment_available{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_cast(#retention_updated{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_cast(#delete_stream{} = Effect, State) ->
    {noreply, apply_effect(Effect, State)};
handle_cast(Message, State) ->
    ?LOG_DEBUG(?MODULE_STRING " received unexpected cast: ~W", [Message, 10]),
    {noreply, State}.

handle_info({osiris_offset, Reference, Offset}, #?MODULE{references = References} = State) ->
    case References of
        #{Reference := StreamId} ->
            Event = #commit_offset_increased{stream = StreamId, offset = Offset},
            {noreply, evolve_event(Event, State)};
        _ ->
            {noreply, State}
    end;
handle_info(#set_range{} = Effect, State) ->
    {noreply, apply_effect(Effect, State)};
handle_info(#manifest_resolved{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_info(#fragments_applied{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_info(tick_timeout, State0) ->
    State = evolve_event(#tick{}, State0),
    ok = set_tick_timer(),
    {noreply, State};
handle_info({'DOWN', _MRef, process, Pid, Reason}, #?MODULE{tasks = Tasks0} = State0) ->
    case maps:take(Pid, Tasks0) of
        {Effect, Tasks} ->
            ?LOG_INFO("Task ~0p (~p) down with reason ~0p. Tasks: ~0P", [
                Effect, Pid, Reason, Tasks0, 10
            ]),
            %% TODO: retry. We have the effect and can attempt it again.
            {noreply, State0#?MODULE{tasks = Tasks}};
        error ->
            {noreply, State0}
    end;
handle_info(Message, State) ->
    ?LOG_DEBUG(
        ?MODULE_STRING " received unexpected message: ~W",
        [Message, 10]
    ),
    {noreply, State}.

format_status(#{state := #?MODULE{} = State0} = Status0) ->
    Status0#{state := format_state(State0)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------------------------------------------------------

format_osiris_event(Event) ->
    Event.

format_state(#?MODULE{machine = Machine, tasks = Tasks}) ->
    #{
        machine => rabbitmq_stream_s3_machine:format(Machine),
        tasks => map_size(Tasks)
    }.

-spec evolve_event(event(), #?MODULE{}) -> #?MODULE{}.
evolve_event(Event, #?MODULE{machine = MacState0} = State0) ->
    {MacState, Effects} = rabbitmq_stream_s3_machine:apply(metadata(), Event, MacState0),
    lists:foldl(fun apply_effect/2, State0#?MODULE{machine = MacState}, Effects).

-spec apply_effect(effect(), #?MODULE{}) -> #?MODULE{}.
apply_effect(#reply{to = To, response = Response}, State) ->
    ok = gen_server:reply(To, Response),
    State;
apply_effect(#send{to = Pid, message = Message, options = Options}, State) ->
    _ = erlang:send(Pid, Message, Options),
    State;
apply_effect(#register_offset_listener{writer_pid = Pid, offset = Offset}, State) ->
    ok = osiris:register_offset_listener(Pid, Offset, {?MODULE, format_osiris_event, []}),
    State;
apply_effect(
    #set_range{
        stream = StreamId,
        counter = Cnt,
        first_offset = FirstOffset,
        first_timestamp = FirstTs,
        next_offset = NextOffset
    },
    State
) ->
    ok = counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET, FirstOffset),
    ok = counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_TIMESTAMP, FirstTs),
    _ = ets:update_element(
        ?RANGE_TABLE,
        StreamId,
        [{2, FirstOffset}, {3, NextOffset}],
        {StreamId, FirstOffset, NextOffset}
    ),
    State;
apply_effect(
    #trigger_retention{
        stream = StreamId,
        dir = Dir,
        shared = Shared,
        counter = Cnt
    },
    State
) ->
    %% This is an abbreviated version of the `EvalFun` used by `osiris_log`.
    %% It also moves the first offset and timestamp forward. We don't want to
    %% do that though since the data can exist in the remote tier.
    EvalFun = fun
        ({{FstOff, _}, FstTs, NumSegLeft}) when
            is_integer(FstOff),
            is_integer(FstTs)
        ->
            osiris_log_shared:set_first_chunk_id(Shared, FstOff),
            counters:put(Cnt, ?C_OSIRIS_LOG_SEGMENTS, NumSegLeft);
        (_) ->
            ok
    end,
    Spec = [{'fun', local_retention_fun(StreamId)}],
    ok = osiris_retention:eval(StreamId, Dir, Spec, EvalFun),
    State;
apply_effect(#upload_fragment{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#upload_manifest{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#rebalance_manifest{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#resolve_manifest{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#find_fragments{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#delete_fragments{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#delete_stream{} = Effect, State) ->
    spawn_task(Effect, State).

spawn_task(Effect, #?MODULE{tasks = Tasks0} = State0) ->
    %% NOTE: use of `erlang:self/0` is intentional here. If we casted to the
    %% server's registered name instead, a restarted manifest server after a
    %% crash could get nonsensical task events from old incarnations.
    {TaskPid, _MRef} = spawn_monitor(?MODULE, execute_task, [Effect, self()]),
    State0#?MODULE{tasks = Tasks0#{TaskPid => Effect}}.

execute_task(Effect, ManifestServer) ->
    Event = execute_task(Effect),
    gen_server:cast(ManifestServer, #task_completed{task_pid = self(), event = Event}).

execute_task(#upload_fragment{
    stream = StreamId,
    dir = Dir,
    fragment =
        #fragment{
            segment_offset = SegmentOffset,
            segment_pos = SegmentPos,
            first_offset = FragmentOffset,
            first_timestamp = Ts,
            next_offset = NextOffset,
            checksum = Checksum0,
            num_chunks = {IdxStart, IdxLen},
            seq_no = SeqNo,
            size = Size,
            roll_reason = RollReason
        } = Fragment
}) ->
    Timeout = application:get_env(rabbitmq_stream_s3, segment_upload_timeout, 45_000),
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    FragmentFilename = rabbitmq_stream_s3:offset_filename(FragmentOffset, <<"fragment">>),
    SegmentFilename = rabbitmq_stream_s3:offset_filename(SegmentOffset, <<"segment">>),
    IndexFilename = rabbitmq_stream_s3:offset_filename(SegmentOffset, <<"index">>),
    Key = rabbitmq_stream_s3:fragment_key(StreamId, FragmentOffset),
    ?LOG_DEBUG(
        "Starting upload of ~ts (~b of ~ts, next offset ~b of ~ts): ~w", [
            FragmentFilename, SeqNo, SegmentFilename, NextOffset, StreamId, Fragment
        ]
    ),

    try
        {UploadMSec, {UploadSize, FragmentInfo0}} = timer:tc(
            fun() ->
                {ok, SegFd} = file:open(filename:join(Dir, SegmentFilename), [read, raw, binary]),
                {ok, IdxFd} = file:open(filename:join(Dir, IndexFilename), [read, raw, binary]),

                {ok, SegData} = file:pread(SegFd, SegmentPos, Size),
                {ok, IdxData0} = file:pread(
                    IdxFd,
                    ?IDX_HEADER_B + (IdxStart * ?INDEX_RECORD_SIZE_B),
                    IdxLen * ?INDEX_RECORD_SIZE_B
                ),
                %% Convert from osiris index style to fragment index. We can
                %% drop epoch since it's not necessary after commit. TODO: right?
                %% TODO: this is pretty messy. Use the INDEX_RECORD macro.
                IdxData = <<
                    <<
                        IdxChId:64/unsigned,
                        IdxTs:64/signed,
                        (SegmentFilePos - SegmentPos + ?SEGMENT_HEADER_B):32/unsigned
                    >>
                 || <<
                        IdxChId:64/unsigned,
                        IdxTs:64/signed,
                        _Epoch:64/unsigned,
                        SegmentFilePos:32/unsigned,
                        _ChType:8/unsigned
                    >> <= IdxData0
                >>,
                Trailer = ?FRAGMENT_TRAILER(
                    FragmentOffset,
                    Ts,
                    NextOffset,
                    SeqNo,
                    Size,
                    IdxStart,
                    SegmentPos,
                    (?SEGMENT_HEADER_B + Size + ?IDX_HEADER_B),
                    (byte_size(IdxData))
                ),
                Data = [?SEGMENT_HEADER, SegData, ?IDX_HEADER, IdxData, Trailer],
                Checksum =
                    case Checksum0 of
                        undefined ->
                            erlang:crc32(Data);
                        _ ->
                            erlang:crc32(Checksum0, [?IDX_HEADER, IdxData, Trailer])
                    end,
                %% TODO: should be able to upload this in chunks. Gun should
                %% support that.
                ok = rabbitmq_stream_s3_api:put(
                    Conn,
                    Key,
                    Data,
                    #{
                        unsigned_payload => true,
                        crc32 => Checksum,
                        timeout => Timeout
                    }
                ),
                {iolist_size(Data), fragment_trailer_to_info(Trailer)}
            end,
            millisecond
        ),
        ?LOG_DEBUG("Uploaded ~ts of ~ts in ~b msec (~b bytes)", [
            FragmentFilename, SegmentFilename, UploadMSec, UploadSize
        ]),
        %% TODO: update counters for fragments.
        rabbitmq_stream_s3_counters:segment_uploaded(UploadSize),
        FragmentInfo = FragmentInfo0#fragment_info{roll_reason = RollReason},
        #fragment_uploaded{stream = StreamId, info = FragmentInfo}
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end;
execute_task(#rebalance_manifest{
    stream = StreamId,
    kind = GroupKind,
    size = GroupSize,
    new_group = GroupEntries,
    rebalanced = RebalancedEntries,
    manifest = Manifest0
}) ->
    Ext = rabbitmq_stream_s3:group_extension(GroupKind),
    ?ENTRY(GroupOffset, Ts, _, _, _, Uid, _) = GroupEntries,
    Key = rabbitmq_stream_s3:group_key(StreamId, Uid, GroupKind, GroupOffset),
    Data = [
        group_header(GroupKind),
        <<GroupOffset:64/unsigned, Ts:64/signed, 0:2/signed, GroupSize:70/unsigned>>,
        GroupEntries
    ],

    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    try
        ?LOG_DEBUG("rebalancing: adding a ~ts to the manifest for '~tp'", [Ext, StreamId]),
        {UploadMsec, ok} = timer:tc(
            fun() ->
                ok = rabbitmq_stream_s3_api:put(Conn, Key, Data)
            end,
            millisecond
        ),
        DataSize = iolist_size(Data),
        ?LOG_DEBUG("Uploaded ~ts for '~tp' in ~b msec (~b bytes)", [
            Ext, StreamId, UploadMsec, DataSize
        ]),
        %% TODO: counters per group kind.
        %% rabbitmq_stream_s3_counters:manifest_written(Size),
        ok
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end,
    Manifest = Manifest0#manifest{entries = RebalancedEntries},
    %% TODO: update `rabbitmq_stream_s3_db`.
    #manifest_rebalanced{stream = StreamId, manifest = Manifest};
execute_task(#upload_manifest{
    stream = StreamId,
    epoch = Epoch,
    reference = Reference,
    manifest = #manifest{
        first_offset = Offset,
        first_timestamp = Ts,
        next_offset = NextOffset,
        total_size = Size,
        revision = ExpectedRevision,
        entries = Entries
    }
}) ->
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    Uid = rabbitmq_stream_s3:uid(),
    Key = rabbitmq_stream_s3:manifest_key(StreamId, Uid),
    Data = ?MANIFEST(Offset, Ts, NextOffset, Size, Entries),
    try
        ?LOG_DEBUG("Uploading manifest for stream '~ts'", [StreamId]),
        {UploadMsec, ok} = timer:tc(
            fun() ->
                ok = rabbitmq_stream_s3_api:put(Conn, Key, Data)
            end,
            millisecond
        ),
        ManifestSize = iolist_size(Data),
        ?LOG_DEBUG("Uploaded manifest for stream '~ts' in ~b msec (~b bytes)", [
            StreamId, UploadMsec, ManifestSize
        ]),
        rabbitmq_stream_s3_counters:manifest_written(ManifestSize),
        case rabbitmq_stream_s3_db:put(StreamId, Reference, Epoch, ExpectedRevision, Uid) of
            {ok, OldInfo, NewRevision} ->
                case OldInfo of
                    undefined ->
                        ?LOG_DEBUG("Initial manifest created for stream '~ts' at epoch ~b", [
                            StreamId, Epoch
                        ]),
                        ok;
                    {OldUid, OldEpoch} ->
                        ?LOG_DEBUG(
                            "Manifest updated for stream '~ts', epoch ~b->~b, uid '~ts'->'~ts'", [
                                StreamId,
                                OldEpoch,
                                Epoch,
                                rabbitmq_stream_s3:format_uid(OldUid),
                                rabbitmq_stream_s3:format_uid(Uid)
                            ]
                        ),
                        OldKey = rabbitmq_stream_s3:manifest_key(StreamId, OldUid),
                        case rabbitmq_stream_s3_api:delete(Conn, OldKey) of
                            ok ->
                                ?LOG_DEBUG("Cleaned up old manifest with uid '~ts'", [
                                    rabbitmq_stream_s3:format_uid(OldUid)
                                ]),
                                ok;
                            {error, _} = Err ->
                                ?LOG_DEBUG("Failed to clean up old manifest with uid '~ts': ~0p", [
                                    rabbitmq_stream_s3:format_uid(OldUid), Err
                                ]),
                                ok
                        end
                end,
                Entry = #{uid => Uid, epoch => Epoch, revision => NewRevision},
                #manifest_uploaded{stream = StreamId, entry = Entry};
            {error,
                {conflict,
                    #{uid := ActualUid, epoch := ActualEpoch, revision := ActualRevision} =
                        Entry}} ->
                ?LOG_INFO(
                    "An uploaded manifest was rejected by the metadata store's optimistic lock. Expected revision ~b actual ~b, uid '~ts' actual '~ts', epoch ~b actual ~b)",
                    [
                        ExpectedRevision,
                        ActualRevision,
                        rabbitmq_stream_s3:format_uid(Uid),
                        rabbitmq_stream_s3:format_uid(ActualUid),
                        Epoch,
                        ActualEpoch
                    ]
                ),
                #manifest_upload_rejected{
                    stream = StreamId,
                    conflict = Entry
                };
            {error, not_found} ->
                ?LOG_INFO(
                    "An uploaded manifest was rejected by the metadata store's optimistic lock. Expected revision ~b, uid '~ts', epoch ~b but found no entry",
                    [
                        ExpectedRevision,
                        rabbitmq_stream_s3:format_uid(Uid),
                        Epoch
                    ]
                ),
                #stream_deleted{stream = StreamId};
            {error, _} = Err ->
                exit(Err)
        end
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end;
execute_task(#resolve_manifest{stream = StreamId}) ->
    Manifest0 =
        case rabbitmq_stream_s3_db:get(StreamId) of
            {ok, #{uid := Uid, revision := Revision}} ->
                Key = rabbitmq_stream_s3:manifest_key(StreamId, Uid),
                {ok, Conn} = rabbitmq_stream_s3_api:open(),
                try
                    case rabbitmq_stream_s3_api:get(Conn, Key) of
                        {ok,
                            ?MANIFEST(FirstOffset, FirstTimestamp, NextOffset, TotalSize, Entries) =
                                Data} ->
                            rabbitmq_stream_s3_counters:manifest_read(byte_size(Data)),
                            #manifest{
                                first_offset = FirstOffset,
                                first_timestamp = FirstTimestamp,
                                next_offset = NextOffset,
                                total_size = TotalSize,
                                revision = Revision,
                                entries = Entries
                            };
                        {error, not_found} ->
                            #manifest{};
                        {error, _} = Err ->
                            exit(Err)
                    end
                after
                    ok = rabbitmq_stream_s3_api:close(Conn)
                end;
            {error, not_found} ->
                #manifest{};
            {error, _} = Err ->
                exit(Err)
        end,
    #manifest_resolved{
        stream = StreamId,
        manifest = resolve_manifest_tail(StreamId, Manifest0)
    };
execute_task(#delete_fragments{stream = StreamId, offsets = Offsets}) ->
    NumFragments = length(Offsets),
    ?LOG_DEBUG("Deleting ~b fragments for stream '~ts' with offsets ~0p", [
        NumFragments, StreamId, Offsets
    ]),
    Keys = [rabbitmq_stream_s3:fragment_key(StreamId, Offset) || Offset <- Offsets],
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    try
        {DeleteMsec, ok} = timer:tc(
            fun() ->
                ok = rabbitmq_stream_s3_api:delete(Conn, Keys)
            end,
            millisecond
        ),
        ?LOG_DEBUG("Deleted ~b fragments from stream '~ts' in ~b msec", [
            NumFragments, StreamId, DeleteMsec
        ]),
        ok
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end,
    ok;
execute_task(#find_fragments{stream = StreamId, dir = Dir, from = FromOffset, to = ToOffset}) ->
    _ = [
        gen_server:cast(?SERVER, #fragment_available{stream = StreamId, fragment = F})
     || #fragment{
            first_offset = FirstOffset,
            next_offset = NextOffset
        } = F <- rabbitmq_stream_s3_log_manifest:find_fragments_in_range(
            Dir,
            FromOffset,
            ToOffset
        ),
        FirstOffset >= FromOffset,
        NextOffset =< ToOffset
    ],
    ok;
execute_task(#delete_stream{stream = StreamId}) ->
    ?LOG_DEBUG("Deleting stream '~ts'", [StreamId]),
    %% NOTE: See `rabbitmq_stream_s3_db:handle_queue_deletion/1`. The node
    %% where this task runs might not be a member of this stream. So we can't
    %% rely on information in the manifest to perform the deletion.
    %%
    %% LIST all keys with the prefix of this stream ID and delete 1000 objects
    %% at a time.
    ok.

resolve_manifest_tail(
    StreamId,
    #manifest{
        next_offset = NextOffset0,
        entries = Entries0,
        total_size = TotalSize0
    } = Manifest0
) ->
    case get_fragment_trailer(StreamId, NextOffset0) of
        {ok, #fragment_info{
            offset = NextOffset0,
            next_offset = NextOffset,
            timestamp = Ts,
            size = Size,
            seq_no = SeqNo
        }} ->
            Entries =
                <<Entries0/binary,
                    (?ENTRY(
                        NextOffset0,
                        Ts,
                        ?MANIFEST_KIND_FRAGMENT,
                        Size,
                        SeqNo,
                        rabbitmq_stream_s3:uid(),
                        <<>>
                    ))/binary>>,
            Manifest = Manifest0#manifest{
                next_offset = NextOffset,
                total_size = TotalSize0 + Size,
                entries = Entries
            },
            resolve_manifest_tail(StreamId, Manifest);
        {error, not_found} ->
            Manifest0
    end.

group_header(?MANIFEST_KIND_GROUP) ->
    <<?MANIFEST_GROUP_MAGIC, ?MANIFEST_GROUP_VERSION:32/unsigned>>;
group_header(?MANIFEST_KIND_KILO_GROUP) ->
    <<?MANIFEST_KILO_GROUP_MAGIC, ?MANIFEST_KILO_GROUP_VERSION:32/unsigned>>;
group_header(?MANIFEST_KIND_MEGA_GROUP) ->
    <<?MANIFEST_MEGA_GROUP_MAGIC, ?MANIFEST_MEGA_GROUP_VERSION:32/unsigned>>.

get_fragment_trailer(StreamId, FragmentOffset) ->
    get_fragment_trailer(rabbitmq_stream_s3:fragment_key(StreamId, FragmentOffset)).

get_fragment_trailer(Key) ->
    {ok, Conn} = rabbitmq_stream_s3_api:open(),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME]),
    try rabbitmq_stream_s3_api:get_range(Conn, Key, -?FRAGMENT_TRAILER_B) of
        {ok, Data} ->
            {ok, fragment_trailer_to_info(Data)};
        {error, _} = Err ->
            Err
    after
        ok = rabbitmq_stream_s3_api:close(Conn)
    end.

-spec fragment_trailer_to_info(binary()) -> #fragment_info{}.
fragment_trailer_to_info(
    ?FRAGMENT_TRAILER(
        Offset,
        Ts,
        NextOffset,
        SeqNo,
        Size,
        NumChunksInSegment,
        SegmentStartPos,
        IdxStartPos,
        IdxSize
    )
) ->
    #fragment_info{
        offset = Offset,
        timestamp = Ts,
        next_offset = NextOffset,
        seq_no = SeqNo,
        num_chunks_in_segment = NumChunksInSegment,
        segment_start_pos = SegmentStartPos,
        size = Size,
        index_start_pos = IdxStartPos,
        index_size = IdxSize
    }.

set_tick_timer() ->
    Timeout = application:get_env(rabbitmq_stream_s3, tick_timeout_milliseconds, 5000),
    _ = erlang:send_after(Timeout, self(), tick_timeout),
    ok.

-spec metadata() -> rabbitmq_stream_s3_machine:metadata().
metadata() ->
    #{time => erlang:system_time(millisecond)}.

-spec local_retention_fun(stream_id()) -> osiris:retention_fun().
local_retention_fun(StreamId) ->
    fun(IdxFiles) ->
        try ets:lookup_element(?RANGE_TABLE, StreamId, 3) of
            NextTieredOffset ->
                eval_local_retention(IdxFiles, NextTieredOffset)
        catch
            error:badarg ->
                {[], IdxFiles}
        end
    end.

-spec eval_local_retention(IdxFiles :: [filename()], osiris:offset()) ->
    {ToDelete :: [filename()], ToKeep :: [filename(), ...]}.
eval_local_retention(IdxFiles, NextTieredOffset) ->
    %% Always keep the current active segment no matter what the last tiered
    %% offset is.
    eval_local_retention(lists:reverse(IdxFiles), NextTieredOffset, [], []).

eval_local_retention([], _NextTieredOffset, ToDelete, ToKeep) ->
    %% Always keep the current active segment no matter what the last tiered
    %% offset is.
    {lists:reverse(ToDelete), ToKeep};
eval_local_retention([IdxFile | Rest], NextTieredOffset, ToDelete, ToKeep) ->
    Offset = rabbitmq_stream_s3:index_file_offset(IdxFile),
    case Offset >= NextTieredOffset of
        true ->
            eval_local_retention(Rest, NextTieredOffset, ToDelete, [IdxFile | ToKeep]);
        false ->
            {lists:reverse(Rest), [IdxFile | ToKeep]}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

eval_local_retention_test() ->
    IdxFiles = [
        <<"/data/00000000000000000000.index">>,
        <<"/data/00000000000000000100.index">>,
        <<"/data/00000000000000000200.index">>,
        <<"/data/00000000000000000300.index">>,
        <<"/data/00000000000000000400.index">>
    ],
    ?assertEqual(
        {
            [
                <<"/data/00000000000000000000.index">>,
                <<"/data/00000000000000000100.index">>
            ],
            [
                <<"/data/00000000000000000200.index">>,
                <<"/data/00000000000000000300.index">>,
                <<"/data/00000000000000000400.index">>
            ]
        },
        eval_local_retention(IdxFiles, 251)
    ),
    %% Always keep the current segment:
    ?assertEqual(
        {
            [
                <<"/data/00000000000000000000.index">>,
                <<"/data/00000000000000000100.index">>,
                <<"/data/00000000000000000200.index">>,
                <<"/data/00000000000000000300.index">>
            ],
            [
                <<"/data/00000000000000000400.index">>
            ]
        },
        eval_local_retention(IdxFiles, 451)
    ),
    ?assertEqual(
        {
            [
                <<"/data/00000000000000000000.index">>,
                <<"/data/00000000000000000100.index">>,
                <<"/data/00000000000000000200.index">>
            ],
            [
                <<"/data/00000000000000000300.index">>,
                <<"/data/00000000000000000400.index">>
            ]
        },
        eval_local_retention(IdxFiles, 301)
    ),
    ?assertEqual(
        {
            [],
            [
                <<"/data/00000000000000000000.index">>,
                <<"/data/00000000000000000100.index">>,
                <<"/data/00000000000000000200.index">>,
                <<"/data/00000000000000000300.index">>,
                <<"/data/00000000000000000400.index">>
            ]
        },
        eval_local_retention(IdxFiles, 0)
    ),
    ok.

-endif.
