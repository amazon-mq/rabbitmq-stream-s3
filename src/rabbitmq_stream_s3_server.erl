%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("include/rabbitmq_stream_s3.hrl").

-define(SERVER, ?MODULE).
%% Protected set of {stream_id(), FirstOffset, NextOffset} used to quickly
%% look up remote tier ranges for the sake of local retention.
-define(RANGE_TABLE, rabbitmq_stream_s3_server_range).
%% Public set of {stream_id(), {#group_ref{}, rabbitmq_stream_s3:entries()}}
%% used to avoid unnecessarily re-downloading the first group in a stream.
-define(FIRST_GROUPS, rabbitmq_stream_s3_server_first_groups).
-define(MiB, 1048576).

-behaviour(gen_server).

-rabbit_boot_step(
    {rabbitmq_stream_s3_server, [
        {description, "tiered storage S3 coordinator"},
        {mfa, {?MODULE, start, []}},
        {requires, rabbitmq_stream_s3_http_pools},
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
-record(retry_task, {task :: reference()}).

-record(task, {
    effect :: effect(),
    failures = 0 :: non_neg_integer()
}).

-record(?MODULE, {
    machine = rabbitmq_stream_s3_machine:new() :: rabbitmq_stream_s3_machine:state(),
    tasks = #{} :: #{pid() => #task{}},
    delayed_tasks = #{} :: #{reference() => #task{}},
    %% A lookup from stream reference to ID. This is used to determine the
    %% stream ID associated with an offset notification `{osiris_offset,
    %% Reference, osiris:offset()}` sent by the writer (because we used
    %% `osiris:register_offset_listener/3`).
    references = #{} :: #{stream_reference() => stream_id()},
    members = #{} :: #{reference() => {stream_id(), stream_reference()}}
}).

%% API
-export([
    get_manifest/1,
    get_range/1,
    init_acceptor/2,
    init_writer/3,
    fragment_available/2,
    retention_updated/2,
    delete_stream/1
]).

%% Useful for other modules
-export([
    get_fragment_info/1,
    get_fragment_info/2,
    split_fragment_info/1,
    get_group_fun/2,
    get_group/3
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

-define(C_ACTIVE_TASKS, 1).
-define(C_TOTAL_TASKS, 2).
-define(C_TASK_FAILURES, 3).
-define(C_FRAGMENTS_CREATED, 4).
-define(C_GROUPS_CREATED, 5).
-define(C_KILO_GROUPS_CREATED, 6).
-define(C_MEGA_GROUPS_CREATED, 7).
-define(C_ROOTS_CREATED, 8).
-define(C_MANIFESTS_RESOLVED, 9).
-define(C_MANIFESTS_RESOLVED_EMPTY, 10).
-define(C_FRAGMENTS_DELETED, 11).
-define(C_GROUPS_DELETED, 12).
-define(C_KILO_GROUPS_DELETED, 13).
-define(C_MEGA_GROUPS_DELETED, 14).
-define(C_STREAMS_DELETED, 15).
-define(C_LOCAL_TIER_RETENTION_EVALUATIONS, 16).
-define(C_REMOTE_TIER_RETENTION_EVALUATIONS, 17).
-define(C_REMOTE_BYTES, 18).
-define(C_REMOTE_MESSAGES, 19).
-define(C_REMOTE_OLDEST_TIMESTAMP, 20).
-define(COUNTERS, [
    {active_tasks, ?C_ACTIVE_TASKS, gauge, "Current number of tasks"},
    {total_tasks, ?C_TOTAL_TASKS, counter, "Total number of tasks spawned"},
    {task_failures, ?C_TASK_FAILURES, counter, "Number of times a task has crashed"},
    {fragments_created, ?C_FRAGMENTS_CREATED, counter,
        "Number of fragments objects created in the remote tier"},
    {groups_created, ?C_GROUPS_CREATED, counter, "Number of group manifest objects created"},
    {kilo_groups_created, ?C_KILO_GROUPS_CREATED, counter,
        "Number of kilo group manifest objects created"},
    {mega_groups_created, ?C_MEGA_GROUPS_CREATED, counter,
        "Number of mega group manifest objects created"},
    {roots_created, ?C_ROOTS_CREATED, counter, "Number of root manifest objects created"},
    {manifests_resolved, ?C_MANIFESTS_RESOLVED, counter,
        "Number of times a non-empty manifest has been resolved"},
    {manifests_resolved_empty, ?C_MANIFESTS_RESOLVED_EMPTY, counter,
        "Number of times a manifest has been resolved as empty/non-existent"},
    %% NOTE: objects deleted when a stream is deleted do not count towards these
    %% counters. Those objects are not effectively tracked in counters - the best
    %% proxy is in `rabbitmq_stream_s3_api_aws` in LIST requests.
    {fragments_deleted, ?C_FRAGMENTS_DELETED, counter,
        "Number of fragment objects deleted by retention"},
    {groups_deleted, ?C_GROUPS_DELETED, counter, "Number of group objects deleted by retention"},
    {kilo_groups_deleted, ?C_KILO_GROUPS_DELETED, counter,
        "Number of kilo group objects deleted by retention"},
    {mega_groups_deleted, ?C_MEGA_GROUPS_DELETED, counter,
        "Number of mega group objects deleted by retention"},
    {streams_deleted, ?C_STREAMS_DELETED, counter,
        "Number of times a stream has been successfully deleted from the remote tier"},
    {local_tier_retention_evaluations, ?C_LOCAL_TIER_RETENTION_EVALUATIONS, counter,
        "Number of times retention has been evaluated against the local tier"},
    {remote_tier_retention_evaluations, ?C_REMOTE_TIER_RETENTION_EVALUATIONS, counter,
        "Number of times retention has been evaluated against the remote tier"},
    {remote_bytes, ?C_REMOTE_BYTES, gauge, "Total bytes stored in the remote tier"},
    {remote_messages, ?C_REMOTE_MESSAGES, gauge, "Total messages stored in the remote tier"},
    {remote_oldest_timestamp, ?C_REMOTE_OLDEST_TIMESTAMP, gauge,
        "Timestamp (ms) of the oldest message stored in the remote tier, or 0 if empty"}
]).
-define(COUNTER_KEY, {?MODULE, counters}).

%% For `sys:get_state/1` debugging.
-export([format_state/0, format_state/1]).

%% Need to be exported for `erlang:apply/3`.
-export([start/0, format_osiris_event/1, execute_task/1, execute_task/2]).

%%----------------------------------------------------------------------------

%% This server needs to be started by a boot step so that it is online before
%% the stream coordinator. Otherwise the stream coordinator will attempt to
%% recover replicas before this server is started and writer_manifest/1 will
%% fail a few times and look messy in the logs.
start() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),

    ok = rabbitmq_stream_s3_api:init(),
    ok = rabbitmq_stream_s3_db:setup(),
    _ = ets:new(?FIRST_GROUPS, [named_table, public]),
    rabbit_sup:start_child(?MODULE).

-spec get_manifest(stream_id()) -> #manifest{} | undefined.
get_manifest(StreamId) ->
    gen_server:call(?SERVER, #get_manifest{stream = StreamId}, ?GEN_SERVER_CALL_TIMEOUT).

-doc "Gets the range of offsets in the remote tier".
-spec get_range(stream_id()) -> rabbitmq_stream_s3:range().
get_range(StreamId) ->
    try ets:lookup(?RANGE_TABLE, StreamId) of
        [{StreamId, FirstOffset, NextOffset}] ->
            {FirstOffset, NextOffset - 1};
        [] ->
            empty
    catch
        error:badarg ->
            empty
    end.

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
    #writer_spawned{
        stream = StreamId,
        pid = Pid,
        config = #{reference := Reference}
    } = Event,
    #?MODULE{members = Members0, references = References0} = State0
) ->
    MRef = erlang:monitor(process, Pid),
    State1 = State0#?MODULE{
        members = Members0#{MRef => {StreamId, Reference}},
        references = References0#{Reference => StreamId}
    },
    {noreply, evolve_event(Event, State1)};
handle_cast(
    #init_acceptor{
        stream = StreamId,
        pid = Pid,
        config =
            #{
                leader_pid := LeaderPid,
                reference := Reference
            } = Config
    },
    #?MODULE{members = Members0, references = References0} = State0
) ->
    MRef = erlang:monitor(process, Pid),
    ok = gen_server:cast({?SERVER, node(LeaderPid)}, #manifest_requested{
        stream = StreamId,
        requester = self()
    }),
    State1 = State0#?MODULE{
        members = Members0#{MRef => {StreamId, Reference}},
        references = References0#{Reference => StreamId}
    },
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
handle_info(#manifest_edited{} = Event, State) ->
    {noreply, evolve_event(Event, State)};
handle_info(tick_timeout, State0) ->
    State = evolve_event(#tick{}, State0),
    ok = set_tick_timer(),
    {noreply, State};
handle_info(
    {'DOWN', MRef, process, Pid, Reason},
    #?MODULE{
        tasks = Tasks0,
        delayed_tasks = DelayedTasks0,
        references = References0,
        members = Members0
    } = State0
) ->
    case Tasks0 of
        #{Pid := #task{failures = Failures0} = Task0} ->
            counters:add(counter(), ?C_TASK_FAILURES, 1),
            Tasks = maps:remove(Pid, Tasks0),
            Failures = Failures0 + 1,
            Task = Task0#task{failures = Failures},
            %% With default settings, retry after 100, 400, 900, 1600, 2500, 3600,
            %% 4900, 5000, 5000, 5000, ... ms
            Max = application:get_env(rabbitmq_stream_s3, task_retry_delay_max_ms, 5_000),
            Constant = application:get_env(rabbitmq_stream_s3, task_retry_delay_constant, 10),
            Exponent = application:get_env(rabbitmq_stream_s3, task_retry_delay_exponent, 2),
            DelayMs = min(trunc(math:pow(Constant * Failures, Exponent)), Max),
            TaskRef = erlang:make_ref(),
            erlang:send_after(DelayMs, self(), #retry_task{task = TaskRef}),
            DelayedTasks = DelayedTasks0#{TaskRef => Task},
            %% TODO: move all application:get_env calls to rabbitmq_stream_s3_config.
            case application:get_env(rabbitmq_stream_s3, verbose_logging, false) of
                true ->
                    ?LOG_INFO(
                        "Task ~p (~p) down (~b other outstanding), retrying in ~bms. Reason: ~p", [
                            Task0,
                            Pid,
                            map_size(Tasks),
                            DelayMs,
                            Reason
                        ]
                    );
                false ->
                    ?LOG_INFO(
                        "Task ~P (~p) down (~b other outstanding), retrying in ~bms. Reason: ~P", [
                            Task0,
                            5,
                            Pid,
                            map_size(Tasks),
                            DelayMs,
                            Reason,
                            5
                        ]
                    )
            end,
            State = State0#?MODULE{
                tasks = Tasks,
                delayed_tasks = DelayedTasks
            },
            {noreply, State};
        _ ->
            case Members0 of
                #{MRef := {StreamId, Reference}} ->
                    State1 = State0#?MODULE{
                        references = maps:remove(Reference, References0),
                        members = maps:remove(MRef, Members0)
                    },
                    State = evolve_event(#member_stopped{stream = StreamId}, State1),
                    {noreply, State};
                _ ->
                    {noreply, State0}
            end
    end;
handle_info(
    #retry_task{task = TaskRef},
    #?MODULE{delayed_tasks = DelayedTasks0} = State0
) ->
    #{TaskRef := Task} = DelayedTasks0,
    State1 = State0#?MODULE{delayed_tasks = maps:remove(TaskRef, DelayedTasks0)},
    {noreply, spawn_task(Task, State1)};
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

format_state() ->
    format_state(sys:get_state(?MODULE)).

format_state(#?MODULE{machine = Machine, tasks = Tasks, delayed_tasks = DelayedTasks}) ->
    #{
        machine => rabbitmq_stream_s3_machine:format(Machine),
        tasks => map_size(Tasks),
        delayed_tasks => map_size(DelayedTasks)
    }.

-spec evolve_event(event(), #?MODULE{}) -> #?MODULE{}.
evolve_event(Event, #?MODULE{machine = MacState0} = State0) ->
    {MacState, Effects} = rabbitmq_stream_s3_machine:apply(metadata(), Event, MacState0),
    State = lists:foldl(fun apply_effect/2, State0#?MODULE{machine = MacState}, Effects),
    #{
        bytes := Bytes,
        messages := Messages,
        oldest_timestamp := OldestTs
    } = rabbitmq_stream_s3_machine:metrics(MacState),
    Cnt = counter(),
    counters:put(Cnt, ?C_REMOTE_BYTES, Bytes),
    counters:put(Cnt, ?C_REMOTE_MESSAGES, Messages),
    counters:put(Cnt, ?C_REMOTE_OLDEST_TIMESTAMP, OldestTs),
    State.

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
            counters:add(counter(), ?C_LOCAL_TIER_RETENTION_EVALUATIONS, 1),
            %% Update the shared atomic so that the log reader routes offsets
            %% below FstOff to the remote tier. Do NOT update
            %% ?C_OSIRIS_LOG_FIRST_OFFSET here: that counter reflects the
            %% first offset of the entire stream (local + remote) and is set
            %% by #set_range{} to the remote tier's first offset. Overwriting
            %% it with the local tier's first offset would cause stream metrics
            %% to hide the existence of data in the remote tier.
            osiris_log_shared:set_first_chunk_id(Shared, FstOff),
            counters:put(Cnt, ?C_OSIRIS_LOG_SEGMENTS, NumSegLeft);
        (Result) ->
            ?LOG_DEBUG(
                "trigger_retention EvalFun: unexpected result ~p for stream '~ts', skipping set_first_chunk_id",
                [Result, StreamId]
            ),
            ok
    end,
    Spec = [{'fun', local_retention_fun(StreamId)}],
    ok = osiris_retention:eval(StreamId, Dir, Spec, EvalFun),
    State;
apply_effect(#upload_fragment{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#upload_manifest{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#upload_group{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#resolve_manifest{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#find_fragments{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#evaluate_retention{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#delete_objects{} = Effect, State) ->
    spawn_task(Effect, State);
apply_effect(#delete_stream{} = Effect, State) ->
    spawn_task(Effect, State).

-spec spawn_task(#task{} | effect(), #?MODULE{}) -> #?MODULE{}.
spawn_task(#task{effect = Effect} = Task, #?MODULE{tasks = Tasks0} = State0) ->
    %% NOTE: use of `erlang:self/0` is intentional here. If we casted to the
    %% server's registered name instead, a restarted manifest server after a
    %% crash could get nonsensical task events from old incarnations.
    {TaskPid, _MRef} = spawn_monitor(?MODULE, execute_task, [Effect, self()]),
    State0#?MODULE{tasks = Tasks0#{TaskPid => Task}};
spawn_task(Effect, State) ->
    spawn_task(#task{effect = Effect}, State).

execute_task(Effect, ManifestServer) ->
    Cnt = counter(),
    counters:add(Cnt, ?C_ACTIVE_TASKS, 1),
    counters:add(Cnt, ?C_TOTAL_TASKS, 1),
    try
        Event = execute_task(Effect),
        gen_server:cast(ManifestServer, #task_completed{task_pid = self(), event = Event})
    after
        counters:sub(Cnt, ?C_ACTIVE_TASKS, 1)
    end.

execute_task(#upload_fragment{stream = StreamId, dir = Dir, fragment = Fragment}) ->
    FragmentInfo = upload_fragment(StreamId, Dir, Fragment),
    #fragment_uploaded{stream = StreamId, info = FragmentInfo};
execute_task(#upload_group{
    stream = StreamId,
    kind = GroupKind,
    entries = ?ENTRY(GroupOffset, GroupFTs, _, _, _) = GroupEntries,
    pos = Pos,
    len = Len
}) ->
    ?ENTRY(_, _, GroupLTs, _, _) = rabbitmq_stream_s3_array:last(?ENTRY_B, GroupEntries),
    Uid = rabbitmq_stream_s3:uid(),
    Ext = rabbitmq_stream_s3:group_name(GroupKind),
    GroupRef = #group_ref{uid = Uid, kind = GroupKind, offset = GroupOffset},
    Key = rabbitmq_stream_s3:group_key(StreamId, GroupRef),
    Data = <<
        (group_header(GroupKind))/binary,
        GroupOffset:64/unsigned,
        GroupFTs:64/signed,
        %% TODO: remove these bytes? They are unused.
        0:2/signed,
        0:70/unsigned,
        GroupEntries/binary
    >>,

    ?LOG_DEBUG("rebalancing: adding a ~ts to the manifest for stream '~ts'", [Ext, StreamId]),
    {UploadMsec, ok} = timer:tc(
        fun() ->
            case rabbitmq_stream_s3_api:put(Key, Data) of
                ok -> ok;
                {error, _} = Err -> exit(Err)
            end
        end,
        millisecond
    ),
    DataSize = iolist_size(Data),
    ?LOG_DEBUG("Uploaded ~ts for stream '~ts' in ~b msec (~b bytes)", [
        Ext, StreamId, UploadMsec, DataSize
    ]),
    Counter =
        case GroupKind of
            ?MANIFEST_KIND_GROUP -> ?C_GROUPS_CREATED;
            ?MANIFEST_KIND_KILO_GROUP -> ?C_KILO_GROUPS_CREATED;
            ?MANIFEST_KIND_MEGA_GROUP -> ?C_MEGA_GROUPS_CREATED
        end,
    counters:add(counter(), Counter, 1),
    #group_uploaded{
        stream = StreamId,
        entry = ?GROUP(GroupOffset, GroupFTs, GroupLTs, GroupKind, Uid),
        pos = Pos,
        len = Len
    };
execute_task(#upload_manifest{
    stream = StreamId,
    epoch = Epoch,
    reference = Reference,
    manifest = #manifest{
        first_offset = Offset,
        next_offset = NextOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        total_size = Size,
        revision = ExpectedRevision,
        entries = Entries
    }
}) ->
    Uid = rabbitmq_stream_s3:uid(),
    Key = rabbitmq_stream_s3:manifest_key(StreamId, Uid),
    Data = ?MANIFEST(Offset, NextOffset, FirstTs, FirstLastTs, Size, Entries),
    ?LOG_DEBUG("Uploading manifest for stream '~ts'", [StreamId]),
    {UploadMsec, ok} = timer:tc(
        fun() ->
            case rabbitmq_stream_s3_api:put(Key, Data) of
                ok -> ok;
                {error, _} = Err -> exit(Err)
            end
        end,
        millisecond
    ),
    ManifestSize = iolist_size(Data),
    ?LOG_DEBUG("Uploaded manifest for stream '~ts' in ~b msec (~b bytes)", [
        StreamId, UploadMsec, ManifestSize
    ]),
    counters:add(counter(), ?C_ROOTS_CREATED, 1),
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
                    case rabbitmq_stream_s3_api:delete(OldKey) of
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
    end;
execute_task(#resolve_manifest{stream = StreamId}) ->
    Manifest0 =
        case rabbitmq_stream_s3_db:get(StreamId) of
            {ok, #{uid := Uid, revision := Revision}} ->
                Key = rabbitmq_stream_s3:manifest_key(StreamId, Uid),
                case rabbitmq_stream_s3_api:get(Key) of
                    {ok,
                        ?MANIFEST(
                            FirstOffset,
                            NextOffset,
                            FirstTs,
                            FirstLastTs,
                            TotalSize,
                            Entries
                        )} ->
                        counters:add(counter(), ?C_MANIFESTS_RESOLVED, 1),
                        #manifest{
                            first_offset = FirstOffset,
                            next_offset = NextOffset,
                            first_timestamp = FirstTs,
                            first_last_timestamp = FirstLastTs,
                            total_size = TotalSize,
                            revision = Revision,
                            entries = Entries
                        };
                    {error, not_found} ->
                        counters:add(counter(), ?C_MANIFESTS_RESOLVED_EMPTY, 1),
                        #manifest{};
                    {error, _} = Err ->
                        exit(Err)
                end;
            {error, not_found} ->
                counters:add(counter(), ?C_MANIFESTS_RESOLVED_EMPTY, 1),
                #manifest{};
            {error, _} = Err ->
                exit(Err)
        end,
    #manifest_resolved{
        stream = StreamId,
        manifest = resolve_manifest_tail(StreamId, Manifest0)
    };
execute_task(#delete_objects{stream = StreamId, objects = Objects}) ->
    NumObjects = length(Objects),
    ?LOG_DEBUG("Deleting ~b objects for stream '~ts' ~0p", [
        NumObjects, StreamId, Objects
    ]),
    %% If one of the objects being deleted is a group, attempt to clear it from
    %% the FIRST_GROUPS ETS cache. This ensures that a stream which only has
    %% one group does not keep the group object cached indefinitely after it
    %% is deleted by retention.
    FirstGroup = lists:search(
        fun
            (#group_ref{kind = ?MANIFEST_KIND_GROUP}) -> true;
            (_) -> false
        end,
        Objects
    ),
    case FirstGroup of
        {value, GroupRef} ->
            _ = catch ets:match_delete(?FIRST_GROUPS, {StreamId, {GroupRef, '_'}}),
            ok;
        false ->
            ok
    end,
    %% Then delete all keys.
    Keys = lists:map(
        fun
            (#group_ref{} = Ref) -> rabbitmq_stream_s3:group_key(StreamId, Ref);
            (Offset) -> rabbitmq_stream_s3:fragment_key(StreamId, Offset)
        end,
        Objects
    ),
    {DeleteMsec, ok} = timer:tc(
        fun() ->
            case rabbitmq_stream_s3_api:delete(Keys) of
                ok -> ok;
                {error, _} = Err -> exit(Err)
            end
        end,
        millisecond
    ),
    ?LOG_DEBUG("Deleted ~b objects from stream '~ts' in ~b msec", [
        NumObjects, StreamId, DeleteMsec
    ]),
    Counters = lists:foldl(
        fun(Object, Acc) ->
            Key =
                case Object of
                    #group_ref{kind = ?MANIFEST_KIND_GROUP} -> ?C_GROUPS_DELETED;
                    #group_ref{kind = ?MANIFEST_KIND_KILO_GROUP} -> ?C_KILO_GROUPS_DELETED;
                    #group_ref{kind = ?MANIFEST_KIND_MEGA_GROUP} -> ?C_MEGA_GROUPS_DELETED;
                    _ when is_integer(Object) -> ?C_FRAGMENTS_DELETED
                end,
            maps:update_with(Key, fun(V) -> V + 1 end, 1, Acc)
        end,
        #{},
        Objects
    ),
    Cnt = counter(),
    maps:foreach(fun(Counter, Incr) -> counters:add(Cnt, Counter, Incr) end, Counters),
    ok;
execute_task(#find_fragments{stream = StreamId, dir = Dir, from = FromOffset, to = ToOffset}) ->
    %% TODO: counters around this would be nice?
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
    ?LOG_INFO("Deleting remote tier data for deleted stream '~ts'", [StreamId]),
    %% NOTE: See `rabbitmq_stream_s3_db:handle_queue_deletion/1`. The node
    %% where this task runs might not be a member of this stream. So we can't
    %% rely on information in the manifest to perform the deletion.
    Prefix = rabbitmq_stream_s3:stream_prefix(StreamId),
    {DeleteMsec, Result} = timer:tc(
        rabbitmq_stream_s3_api,
        delete_prefix,
        [Prefix],
        millisecond
    ),
    case Result of
        {ok, Details} ->
            ?LOG_INFO("Deleted remote tier data for deleted stream '~ts' in ~b msec ~0p", [
                StreamId, DeleteMsec, Details
            ]),
            counters:add(counter(), ?C_STREAMS_DELETED, 1),
            ok;
        {error, _} = Err ->
            exit(Err)
    end;
execute_task(#evaluate_retention{
    stream = StreamId,
    manifest = #manifest{} = Manifest,
    retention_spec = RetentionSpec,
    now = Now
}) ->
    ?LOG_DEBUG("Evaluating retention for stream '~ts'", [StreamId]),
    {Edit, Deletions} = rabbitmq_stream_s3_machine:execute_retention(
        Manifest,
        Now,
        RetentionSpec,
        get_group_fun(StreamId, retention)
    ),
    counters:add(counter(), ?C_REMOTE_TIER_RETENTION_EVALUATIONS, 1),
    case Deletions of
        [] ->
            ok;
        _ ->
            %% Don't await deletion in this task. Object deletion can be done
            %% in the background.
            ExecuteDelete = #delete_objects{stream = StreamId, objects = Deletions},
            _ = spawn(?MODULE, execute_task, [ExecuteDelete]),
            ok
    end,
    #retention_executed{stream = StreamId, edit = Edit}.

upload_fragment(
    StreamId,
    Dir,
    #fragment{
        segment_offset = SegmentOffset,
        first_offset = FragmentOffset,
        next_offset = NextOffset,
        seq_no = SeqNo,
        size = Size,
        roll_reason = RollReason
    } = Fragment
) ->
    ?LOG_DEBUG(
        "Starting upload of ~20..0B.fragment (~b of ~20..0B.segment, next offset ~b of ~ts, size ~b)",
        [
            FragmentOffset, SeqNo, SegmentOffset, NextOffset, StreamId, Size
        ]
    ),
    {UploadMSec, {UploadSize, FragmentInfo0}} = timer:tc(
        fun() -> upload_fragment0(StreamId, Dir, Fragment) end,
        millisecond
    ),
    ?LOG_DEBUG("Uploaded ~20..0B (seg ~20..0B) in ~b msec (~b bytes)", [
        FragmentOffset, SegmentOffset, UploadMSec, UploadSize
    ]),
    counters:add(counter(), ?C_FRAGMENTS_CREATED, 1),
    FragmentInfo0#fragment_info{roll_reason = RollReason}.

upload_fragment0(
    StreamId,
    Dir,
    #fragment{
        segment_offset = SegmentOffset,
        segment_pos = SegmentPos,
        first_offset = FragmentOffset,
        first_timestamp = FirstTs,
        last_timestamp = LastTs,
        next_offset = NextOffset,
        checksum = SegmentDataCrc,
        num_chunks = {IdxStart, IdxLen},
        seq_no = SeqNo,
        size = Size
    }
) ->
    Timeout = application:get_env(rabbitmq_stream_s3, segment_upload_timeout, 45_000),
    SegmentFilename = rabbitmq_stream_s3:offset_filename(SegmentOffset, <<"segment">>),
    IndexFilename = rabbitmq_stream_s3:offset_filename(SegmentOffset, <<"index">>),

    Key = rabbitmq_stream_s3:fragment_key(StreamId, FragmentOffset),

    ContentLength = ?FRAGMENT_HEADER_B + Size + ?IDX_HEADER_B + (IdxLen * ?INDEX_RECORD_B),
    {ok, Stream0} = rabbitmq_stream_s3_api:stream_put(Key, ContentLength, #{timeout => Timeout}),
    Header = ?FRAGMENT_HEADER(
        FragmentOffset,
        NextOffset,
        FirstTs,
        LastTs,
        SeqNo,
        SegmentOffset,
        SegmentPos,
        (?FRAGMENT_HEADER_B + Size),
        <<>>
    ),
    Stream1 = rabbitmq_stream_s3_api:stream_data(Stream0, Header),
    Crc0 = erlang:crc32(Header),

    {ok, SegFd} = file:open(filename:join(Dir, SegmentFilename), [read, raw, binary]),
    {Stream2, Crc1} =
        try
            stream_segment_data(Stream1, SegFd, Crc0, SegmentPos, Size)
        after
            file:close(SegFd)
        end,
    case SegmentDataCrc of
        undefined ->
            ok;
        _ ->
            ?assertEqual(Crc1, erlang:crc32_combine(Crc0, SegmentDataCrc, Size))
    end,

    %% TODO: do we even need the index header anymore? It doesn't have
    %% structural significance now that we know the index start in the remote
    %% reader.
    Stream3 = rabbitmq_stream_s3_api:stream_data(Stream2, ?IDX_HEADER),
    Crc2 = erlang:crc32(Crc1, ?IDX_HEADER),
    {ok, IdxFd} = file:open(filename:join(Dir, IndexFilename), [read, raw, binary]),
    {Stream4, Crc} =
        try
            stream_index_data(
                Stream3,
                IdxFd,
                Crc2,
                ?IDX_HEADER_B + (IdxStart * ?INDEX_RECORD_SIZE_B),
                IdxLen,
                SegmentPos
            )
        after
            file:close(IdxFd)
        end,

    case rabbitmq_stream_s3_api:stream_finish(Stream4, Crc) of
        ok ->
            ok;
        {error, _} = Err ->
            exit(Err)
    end,
    {ContentLength, fragment_header_to_info(Header)}.

stream_segment_data(Stream, _Fd, Crc, _Pos, 0) ->
    {Stream, Crc};
stream_segment_data(Stream0, Fd, Crc0, Pos, Size) ->
    Bytes = min(Size, ?MiB),
    {ok, Data} = file:pread(Fd, Pos, Bytes),
    Crc = erlang:crc32(Crc0, Data),
    Stream = rabbitmq_stream_s3_api:stream_data(Stream0, Data),
    stream_segment_data(Stream, Fd, Crc, Pos + Bytes, Size - Bytes).

stream_index_data(Stream, Fd, Crc0, StartPos, IdxLen, SegmentPos) ->
    stream_index_data(Stream, Fd, Crc0, StartPos, IdxLen, SegmentPos, 0).

stream_index_data(Stream, _Fd, Crc, _Pos, Total, _SegmentPos, Total) ->
    {Stream, Crc};
stream_index_data(Stream0, Fd, Crc0, Pos, Total, SegmentPos, Done) ->
    ChunkRecords = min(Total - Done, ?MiB div ?INDEX_RECORD_SIZE_B),
    {ok, RawData} = file:pread(Fd, Pos, ChunkRecords * ?INDEX_RECORD_SIZE_B),
    Transformed = <<
        <<
            IdxChId:64/unsigned,
            IdxTs:64/signed,
            (SegFilePos - SegmentPos + ?FRAGMENT_HEADER_B):32/unsigned
        >>
     || <<
            IdxChId:64/unsigned,
            IdxTs:64/signed,
            _Epoch:64/unsigned,
            SegFilePos:32/unsigned,
            _ChType:8/unsigned
        >> <= RawData
    >>,
    Crc = erlang:crc32(Crc0, Transformed),
    Stream = rabbitmq_stream_s3_api:stream_data(Stream0, Transformed),
    stream_index_data(
        Stream,
        Fd,
        Crc,
        Pos + ChunkRecords * ?INDEX_RECORD_SIZE_B,
        Total,
        SegmentPos,
        Done + ChunkRecords
    ).

resolve_manifest_tail(
    StreamId,
    #manifest{
        next_offset = NextOffset0,
        entries = Entries0,
        total_size = TotalSize0
    } = Manifest0
) ->
    case get_fragment_info(StreamId, NextOffset0) of
        {ok, #fragment_info{
            first_offset = NextOffset0,
            next_offset = NextOffset,
            first_timestamp = FirstTs,
            last_timestamp = LastTs,
            size = Size,
            seq_no = SeqNo
        }} ->
            IsSeqZero =
                case SeqNo of
                    0 -> 1;
                    _ -> 0
                end,
            Entries =
                <<Entries0/binary,
                    ?FRAGMENT(NextOffset0, FirstTs, LastTs, IsSeqZero, Size)/binary>>,
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

get_fragment_info(StreamId, FragmentOffset) ->
    Key = rabbitmq_stream_s3:fragment_key(StreamId, FragmentOffset),
    get_fragment_info(Key).

get_fragment_info(Key) ->
    case rabbitmq_stream_s3_api:get_range(Key, {0, ?FRAGMENT_HEADER_B}) of
        {ok, Data} ->
            {ok, fragment_header_to_info(Data)};
        {error, _} = Err ->
            Err
    end.

-spec split_fragment_info(binary()) -> {#fragment_info{}, binary()}.
split_fragment_info(
    ?FRAGMENT_HEADER(
        Offset,
        NextOffset,
        FirstTs,
        LastTs,
        SeqNo,
        SegmentOffset,
        SegmentStartPos,
        IdxStartPos,
        Rem
    )
) ->
    Info = #fragment_info{
        first_offset = Offset,
        next_offset = NextOffset,
        first_timestamp = FirstTs,
        last_timestamp = LastTs,
        seq_no = SeqNo,
        segment_offset = SegmentOffset,
        segment_start_pos = SegmentStartPos,
        size = IdxStartPos - ?FRAGMENT_HEADER_B,
        index_start_pos = IdxStartPos
    },
    {Info, Rem}.

-spec fragment_header_to_info(binary()) -> #fragment_info{}.
fragment_header_to_info(Data) ->
    {Info, _} = split_fragment_info(Data),
    Info.

set_tick_timer() ->
    Timeout = application:get_env(rabbitmq_stream_s3, tick_timeout_milliseconds, 5000),
    _ = erlang:send_after(Timeout, self(), tick_timeout),
    ok.

get_group_fun(StreamId, Reason) ->
    fun(#group_ref{} = Group) ->
        get_group(StreamId, Group, Reason)
    end.

-spec get_group(stream_id(), #group_ref{}, retention | resolve_offset_spec) ->
    {ok, rabbitmq_stream_s3:entries()} | {error, any()}.
get_group(StreamId, #group_ref{kind = ?MANIFEST_KIND_GROUP} = GroupRef, retention) ->
    case get_group_cached(StreamId, GroupRef) of
        undefined ->
            case get_group0(StreamId, GroupRef) of
                {ok, Entries} = Ok ->
                    _ = catch ets:insert(?FIRST_GROUPS, {StreamId, {GroupRef, Entries}}),
                    Ok;
                {error, _} = Err ->
                    Err
            end;
        Entries ->
            {ok, Entries}
    end;
get_group(StreamId, #group_ref{} = GroupRef, _Reason) ->
    get_group0(StreamId, GroupRef).

get_group_cached(StreamId, #group_ref{kind = ?MANIFEST_KIND_GROUP} = GroupRef) ->
    try ets:lookup(?FIRST_GROUPS, StreamId) of
        [{StreamId, {GroupRef, Entries}}] ->
            Entries;
        _ ->
            undefined
    catch
        error:badarg ->
            undefined
    end.

get_group0(StreamId, #group_ref{offset = Offset} = GroupRef) ->
    Key = rabbitmq_stream_s3:group_key(StreamId, GroupRef),
    ?LOG_DEBUG("Looking up key ~ts (~ts)", [Key, ?FUNCTION_NAME]),
    case rabbitmq_stream_s3_api:get(Key) of
        {ok, Data} ->
            <<
                _Magic:4/binary,
                _Vsn:32/unsigned,
                Offset:64/unsigned,
                _FirstTimestamp:64/unsigned,
                0:2/unsigned,
                _TotalSize:70/unsigned,
                GroupEntries/binary
            >> = Data,
            {ok, GroupEntries};
        {error, _} = Err ->
            Err
    end.

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
    %% NOTE: if `Offset =:= NextTieredOffset`, then the segment file before
    %% this was fully uploaded since `NextTieredOffset` is the last offset to
    %% be successfully uploaded, plus one.
    case Offset > NextTieredOffset of
        true ->
            eval_local_retention(Rest, NextTieredOffset, ToDelete, [IdxFile | ToKeep]);
        false ->
            {lists:reverse(Rest), [IdxFile | ToKeep]}
    end.

counter() ->
    persistent_term:get(?COUNTER_KEY).

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
