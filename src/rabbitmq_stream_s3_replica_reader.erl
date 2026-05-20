%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_replica_reader).
-moduledoc """
Per-stream gen_server owning the upload lifecycle.

Reads committed chunks from the local log, assembles fragments,
submits them to the governor for transfer, and executes effects
returned by the functional core module.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/resource.hrl").
-include("include/logging.hrl").
-include("include/rabbitmq_stream_s3.hrl").

-export([start_link/1, format_state/1]).
-export([identity_formatter/1]).
-export([counter_fields/0, init_counters/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2,
    format_status/1
]).

-export_type([config/0]).

-type config() :: #{
    stream := stream_id(),
    writer_pid := pid(),
    dir := directory(),
    counter := counters:counters_ref(),
    reference := term(),
    epoch := non_neg_integer(),
    shared => atomics:atomics_ref(),
    fragment_target_size => non_neg_integer(),
    persist_threshold => non_neg_integer(),
    retention => [osiris:retention_spec()]
}.

-record(cfg, {
    stream :: stream_id(),
    dir :: directory(),
    writer_pid :: pid(),
    shared :: atomics:atomics_ref(),
    counter :: counters:counters_ref(),
    fragment_target_size :: non_neg_integer(),
    reference :: term(),
    epoch :: non_neg_integer()
}).

-define(OFFSET_FORMATTER, {?MODULE, identity_formatter, []}).
%% Once Osiris supports `identity` as an atom argument to
%% `register_offset_listener/3` (i.e. a `wrap_osiris_event(identity, Evt) ->
%% Evt;` clause in osiris_writer), we can replace ?OFFSET_FORMATTER with the
%% atom `identity` and drop the identity_formatter export. Change is on the
%% Osiris-upstream wishlist.

%% Per-stream counters. Created in init/1 with stream-identifying labels and
%% deleted in terminate/2. The Prometheus collector folds these per-stream
%% counter sets into a single per-node aggregate for the default endpoint
%% and emits them with full labels for the per-object endpoint.
%%
%% Index numbering:
%%   1-N: counters / gauges that the collector emits.
-define(C_TRANSFERS_COMPLETED, 1).
-define(C_TRANSFERS_FAILED, 2).
-define(C_BYTES_TRANSFERRED, 3).
-define(C_GROUPS_CREATED, 4).
-define(C_KILO_GROUPS_CREATED, 5).
-define(C_MEGA_GROUPS_CREATED, 6).
-define(C_ROOTS_CREATED, 7).
-define(C_PERSISTS_COMPLETED, 8).
-define(C_PERSISTS_FAILED, 9).
-define(C_PERSIST_CONFLICTS, 10).
-define(C_MANIFESTS_RESOLVED, 11).
-define(C_MANIFESTS_RESOLVED_EMPTY, 12).
-define(C_FRAGMENTS_DELETED, 13).
-define(C_GROUPS_DELETED, 14).
-define(C_KILO_GROUPS_DELETED, 15).
-define(C_MEGA_GROUPS_DELETED, 16).
-define(C_LOCAL_TIER_RETENTION_EVALUATIONS, 17).
-define(C_REMOTE_TIER_RETENTION_EVALUATIONS, 18).
-define(C_LOCAL_LOG_AHEAD_RECOVERIES, 19).
-define(C_BYTES_DRAINED, 20).
-define(C_BYTES_PERSISTED, 21).
%% Counters above; gauges below. The macro ?NODE_COUNTERS filters by
%% type, and seshat requires counter indexes to be 1..N sequential. Add
%% new counters before this divider and renumber the gauges that follow.
-define(C_TRANSFERS_IN_FLIGHT, 22).
-define(C_LAST_PERSIST_TIMESTAMP_MS, 23).
-define(C_MANIFEST_FIRST_OFFSET, 24).
-define(C_MANIFEST_NEXT_OFFSET, 25).
-define(C_MANIFEST_FIRST_TIMESTAMP_MS, 26).
-define(C_REMOTE_BYTES, 27).
-define(C_REMOTE_MESSAGES, 28).
-define(C_BYTES_IN_ASSEMBLY, 29).
-define(C_BYTES_IN_TRANSFER, 30).
-define(C_BYTES_IN_PERSIST, 31).

-define(STREAM_COUNTERS, [
    {transfers_completed, ?C_TRANSFERS_COMPLETED, counter,
        "Fragment uploads to the remote tier that succeeded"},
    {transfers_failed, ?C_TRANSFERS_FAILED, counter,
        "Fragment uploads to the remote tier that failed"},
    {bytes_transferred, ?C_BYTES_TRANSFERRED, counter,
        "Total payload bytes uploaded to the remote tier"},
    {groups_created, ?C_GROUPS_CREATED, counter, "Number of group manifest objects created"},
    {kilo_groups_created, ?C_KILO_GROUPS_CREATED, counter,
        "Number of kilo-group manifest objects created"},
    {mega_groups_created, ?C_MEGA_GROUPS_CREATED, counter,
        "Number of mega-group manifest objects created"},
    {roots_created, ?C_ROOTS_CREATED, counter, "Number of root manifest objects uploaded"},
    {persists_completed, ?C_PERSISTS_COMPLETED, counter,
        "Manifest persists (S3 + Khepri) that succeeded"},
    {persists_failed, ?C_PERSISTS_FAILED, counter, "Manifest persists that failed for any reason"},
    {persist_conflicts, ?C_PERSIST_CONFLICTS, counter,
        "Manifest persists that failed due to a Khepri conflict (competing writer)"},
    {manifests_resolved, ?C_MANIFESTS_RESOLVED, counter,
        "Number of times a non-empty manifest was resolved on startup"},
    {manifests_resolved_empty, ?C_MANIFESTS_RESOLVED_EMPTY, counter,
        "Number of times an empty manifest was resolved on startup"},
    {fragments_deleted, ?C_FRAGMENTS_DELETED, counter,
        "Number of fragment objects deleted by remote tier retention"},
    {groups_deleted, ?C_GROUPS_DELETED, counter,
        "Number of group objects deleted by remote tier retention"},
    {kilo_groups_deleted, ?C_KILO_GROUPS_DELETED, counter,
        "Number of kilo-group objects deleted by remote tier retention"},
    {mega_groups_deleted, ?C_MEGA_GROUPS_DELETED, counter,
        "Number of mega-group objects deleted by remote tier retention"},
    {local_tier_retention_evaluations, ?C_LOCAL_TIER_RETENTION_EVALUATIONS, counter,
        "Number of times retention has been evaluated against the local tier"},
    {remote_tier_retention_evaluations, ?C_REMOTE_TIER_RETENTION_EVALUATIONS, counter,
        "Number of times retention has been evaluated against the remote tier"},
    {local_log_ahead_recoveries, ?C_LOCAL_LOG_AHEAD_RECOVERIES, counter,
        "Times the replica reader discarded the remote manifest because the local log "
        "was ahead (e.g. local retention deleted un-uploaded data)"},
    {bytes_drained_total, ?C_BYTES_DRAINED, counter,
        "Cumulative bytes the replica reader has drained from osiris (sum of chunk "
        "byte sizes read via osiris_log:read_header)"},
    {bytes_persisted_total, ?C_BYTES_PERSISTED, counter,
        "Cumulative bytes whose fragment manifest update has succeeded (each persist "
        "covers one or more uploaded fragments)"},
    {transfers_in_flight, ?C_TRANSFERS_IN_FLIGHT, gauge,
        "Fragments cut and submitted to the governor that have not yet completed"},
    {last_persist_timestamp_ms, ?C_LAST_PERSIST_TIMESTAMP_MS, gauge,
        "Wall-clock time (ms since epoch) of the last successful manifest persist"},
    {manifest_first_offset, ?C_MANIFEST_FIRST_OFFSET, gauge,
        "Offset of the oldest record present in the remote tier"},
    {manifest_next_offset, ?C_MANIFEST_NEXT_OFFSET, gauge,
        "Next offset to upload to the remote tier"},
    {manifest_first_timestamp_ms, ?C_MANIFEST_FIRST_TIMESTAMP_MS, gauge,
        "Timestamp (ms since epoch) of the oldest record present in the remote tier, "
        "or 0 when empty"},
    {remote_bytes, ?C_REMOTE_BYTES, gauge,
        "Total bytes of segment data stored in the remote tier for this stream"},
    {remote_messages, ?C_REMOTE_MESSAGES, gauge,
        "Approximate number of records stored in the remote tier (next_offset - "
        "first_offset)"},
    {bytes_in_assembly, ?C_BYTES_IN_ASSEMBLY, gauge,
        "Bytes drained from osiris but not yet cut into a fragment. Pipeline stage 1."},
    {bytes_in_transfer, ?C_BYTES_IN_TRANSFER, gauge,
        "Bytes in fragments waiting at the governor or being uploaded to S3. Pipeline "
        "stage 2."},
    {bytes_in_persist, ?C_BYTES_IN_PERSIST, gauge,
        "Bytes in fragments uploaded to S3 whose manifest update has not yet "
        "succeeded. Pipeline stage 3."}
]).

%% Node-level shadow counter set. Per-stream counters are dropped from
%% the labelless `/metrics` aggregate by the prometheus collector to
%% preserve Prometheus monotonicity across stream deletion. The shadow
%% holds the cluster-cumulative value (one counter ref per node, no
%% per-stream label). Each `inc/3` call writes both refs in lockstep.
%%
%% The shadow only contains counter-typed fields; gauges aggregate
%% naturally (sum across streams) on the default endpoint and don't
%% need a shadow.
-define(NODE_COUNTERS, [F || {_, _, counter, _} = F <- ?STREAM_COUNTERS]).

-define(NODE_COUNTER_KEY, {?MODULE, node_counter}).

-record(state, {
    cfg :: #cfg{},
    %% The remote replica reader configuration map.
    config :: map(),
    %% Set after manifest resolve + data reader open.
    log :: osiris_log:state() | undefined,
    assembly :: rabbitmq_stream_s3_fragment_assembly:state() | undefined,
    %% Functional core state.
    core :: rabbitmq_stream_s3_replica_reader_core:state() | undefined,
    %% Nodes registered for manifest broadcast.
    replicas = #{} :: #{node() => reference()},
    %% Monotonic sequence number for broadcast edits. Incremented per edit
    %% batch sent. Replicas use this to detect gaps and request re-sync.
    broadcast_seq = 0 :: non_neg_integer(),
    %% User-configured retention specs for remote tier evaluation.
    retention = [] :: [osiris:retention_spec()],
    %% Commit timer reference.
    persist_timer :: reference() | undefined,
    %% Monitor ref and PID for the in-flight commit task.
    persist_mon :: reference() | undefined,
    persist_pid :: pid() | undefined,
    %% Per-stream seshat counter ref. The id used to register this counter
    %% in the rabbitmq_stream_s3 seshat group, used for cleanup on terminate.
    metrics :: counters:counters_ref() | undefined,
    metrics_id :: term() | undefined,
    %% Map from in-flight transfer ref to its byte size. Tracked in the
    %% shell so we can attribute bytes_transferred and decrement
    %% transfers_in_flight when the result message arrives.
    transfer_sizes = #{} :: #{reference() => non_neg_integer()},
    %% Bytes uploaded but not yet covered by a started persist. Moves to
    %% persisting_bytes when the next start_persist effect fires.
    persist_pending_bytes = 0 :: non_neg_integer(),
    %% Bytes covered by the currently-in-flight persist. Snapshotted from
    %% persist_pending_bytes at start_persist; cleared on persist_result.
    %% bytes_in_persist gauge = persist_pending_bytes + persisting_bytes.
    persisting_bytes = 0 :: non_neg_integer(),
    %% Monitor ref for the in-flight group upload task.
    group_mon :: reference() | undefined,
    %% Monitor ref for the in-flight retention evaluation task.
    retention_mon :: reference() | undefined,
    %% Kind of the group upload currently in flight. Cleared when the
    %% group_upload_result message arrives. Only one rebalance is in
    %% flight at a time so a single slot suffices.
    pending_group_kind :: rabbitmq_stream_s3:kind() | undefined
}).

-doc "Start a remote replica reader for the given stream.".
-spec start_link(config()) -> gen_server:start_ret().
start_link(#{stream := StreamId} = Args) ->
    gen_server:start_link(
        {via, rabbitmq_stream_s3_registry, {StreamId, node()}},
        ?MODULE,
        Args,
        []
    ).

init(
    #{
        stream := StreamId,
        writer_pid := WriterPid,
        dir := Dir,
        reference := Reference,
        epoch := Epoch
    } = Args
) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    monitor(process, WriterPid),
    TargetSize = maps:get(
        fragment_target_size,
        Args,
        application:get_env(rabbitmq_stream_s3, fragment_target_size, ?MAX_FRAGMENT_SIZE_B)
    ),
    Shared = maps:get(shared, Args, undefined),
    {MetricsId, Metrics} = init_metrics(StreamId, Reference),
    ?LOG_INFO("Remote replica reader starting for stream ~ts", [StreamId]),
    Cfg = #cfg{
        stream = StreamId,
        dir = Dir,
        writer_pid = WriterPid,
        shared = Shared,
        counter = maps:get(counter, Args),
        fragment_target_size = TargetSize,
        reference = Reference,
        epoch = Epoch
    },
    Retention = maps:get(retention, Args, []),
    {ok,
        #state{
            cfg = Cfg,
            config = Args,
            retention = Retention,
            core = undefined,
            metrics = Metrics,
            metrics_id = MetricsId
        },
        {continue, resolve_manifest}}.

handle_call({await_offset, Offset}, From, #state{core = Core0} = State) ->
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:await_offset(Offset, From, Core0),
    {noreply, execute_effects(Effects, State#state{core = Core})};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(
    {register_acceptor, Node},
    #state{
        replicas = Replicas,
        core = Core,
        broadcast_seq = Seq,
        cfg = #cfg{stream = StreamId, epoch = Epoch}
    } = State
) ->
    case maps:is_key(Node, Replicas) of
        true ->
            {noreply, State};
        false ->
            MonRef = monitor(process, {rabbitmq_stream_s3_manifest_replica, Node}),
            Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
            rabbitmq_stream_s3_manifest_replica:sync(StreamId, Seq, Epoch, Manifest, Node),
            {noreply, State#state{replicas = Replicas#{Node => MonRef}}}
    end;
handle_cast({retention_updated, Retention}, State) ->
    UserRetention = [S || S <- Retention, element(1, S) =/= 'fun'],
    {noreply, State#state{retention = UserRetention}};
handle_cast(
    {resync, Node},
    #state{
        core = Core,
        broadcast_seq = Seq,
        cfg = #cfg{stream = StreamId, epoch = Epoch}
    } = State
) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    rabbitmq_stream_s3_manifest_replica:sync(StreamId, Seq, Epoch, Manifest, Node),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_continue(resolve_manifest, #state{cfg = #cfg{stream = StreamId}} = State0) ->
    Manifest = resolve_manifest(StreamId),
    State1 = on_manifest_resolved(Manifest, State0),
    {Core, _Effects} = rabbitmq_stream_s3_replica_reader_core:init(Manifest, State1#state.config),
    State = start_reading(State1#state{core = Core}),
    {noreply, State}.

handle_info({osiris_offset, _Ref, _Offset}, State0) ->
    State = drain(State0),
    {noreply, State};
handle_info(retry_resolve, #state{cfg = #cfg{stream = StreamId}} = State0) ->
    Manifest = resolve_manifest(StreamId),
    State1 = on_manifest_resolved(Manifest, State0),
    {Core, _} = rabbitmq_stream_s3_replica_reader_core:init(Manifest, State1#state.config),
    State = start_reading(State1#state{core = Core}),
    {noreply, State};
handle_info({transfer_result, Ref, {ok, Uid}}, #state{core = Core0} = State0) ->
    State1 = on_transfer_result(Ref, ok, State0),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, Uid, Core0),
    State2 = State1#state{core = Core},
    State3 = execute_effects(Effects, State2),
    {noreply, State3};
handle_info({transfer_result, Ref, {error, Reason}}, #state{core = Core0} = State0) ->
    State1 = on_transfer_result(Ref, {error, Reason}, State0),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref, Reason, Core0),
    {noreply, execute_effects(Effects, State1#state{core = Core})};
handle_info({group_upload_result, {ok, Uid}}, #state{core = Core0, group_mon = Mon} = State0) ->
    demonitor(Mon, [flush]),
    State1 = on_group_upload_completed(State0),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(Uid, Core0),
    {noreply, execute_effects(Effects, State1#state{core = Core, group_mon = undefined})};
handle_info(
    {group_upload_result, {error, Reason}},
    #state{core = Core0, group_mon = Mon, cfg = #cfg{stream = StreamId}} = State0
) ->
    demonitor(Mon, [flush]),
    ?LOG_WARNING("~ts group upload failed: ~p", [StreamId, Reason]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:group_upload_failed(Reason, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core, group_mon = undefined, pending_group_kind = undefined
        })};
handle_info({retention_result, unchanged}, #state{core = Core0, retention_mon = Mon} = State0) ->
    demonitor(Mon, [flush]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(unchanged, Core0),
    {noreply, execute_effects(Effects, State0#state{core = Core, retention_mon = undefined})};
handle_info(
    {retention_result, {Edit, Refs}},
    #state{core = Core0, retention_mon = Mon, cfg = #cfg{stream = StreamId}} = State0
) ->
    demonitor(Mon, [flush]),
    State = on_remote_retention(Edit, Refs, StreamId, Core0, State0#state{retention_mon = undefined}),
    {noreply, State};
handle_info(persist_timer, #state{core = Core0} = State0) ->
    Now = erlang:system_time(millisecond),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, Core0),
    {noreply, execute_effects(Effects, State0#state{core = Core, persist_timer = undefined})};
handle_info(
    {'DOWN', _Mon, process, Pid, _Reason},
    #state{cfg = #cfg{writer_pid = Pid, stream = StreamId}} = State
) ->
    ?LOG_INFO("Writer down, stopping remote replica reader for stream ~ts", [StreamId]),
    {stop, normal, State};
handle_info(
    {'DOWN', MonRef, process, {rabbitmq_stream_s3_manifest_replica, Node}, _Reason},
    #state{replicas = Replicas} = State
) ->
    case maps:get(Node, Replicas, undefined) of
        MonRef -> {noreply, State#state{replicas = maps:remove(Node, Replicas)}};
        _ -> {noreply, State}
    end;
handle_info({persist_result, {ok, Revision}}, #state{core = Core0, persist_mon = Mon} = State0) ->
    demonitor(Mon, [flush]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(Revision, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core, persist_mon = undefined, persist_pid = undefined
        })};
handle_info(
    {persist_result, {error, {conflict, _Entry}}}, #state{core = Core0, persist_mon = Mon} = State0
) ->
    demonitor(Mon, [flush]),
    State1 = on_persist_failed(conflict, State0),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(conflict, Core0),
    {noreply,
        execute_effects(Effects, State1#state{
            core = Core, persist_mon = undefined, persist_pid = undefined
        })};
handle_info({persist_result, {error, Reason}}, #state{core = Core0, persist_mon = Mon} = State0) ->
    demonitor(Mon, [flush]),
    State1 = on_persist_failed(Reason, State0),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(Reason, Core0),
    {noreply,
        execute_effects(Effects, State1#state{
            core = Core, persist_mon = undefined, persist_pid = undefined
        })};
handle_info(
    {'DOWN', Mon, process, _, Reason},
    #state{persist_mon = Mon, core = Core0, cfg = #cfg{stream = StreamId}} = State0
) ->
    ?LOG_WARNING("~ts commit task crashed: ~p", [StreamId, Reason]),
    State1 = on_persist_failed(Reason, State0),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(Reason, Core0),
    {noreply,
        execute_effects(Effects, State1#state{
            core = Core, persist_mon = undefined, persist_pid = undefined
        })};
handle_info(
    {'DOWN', Mon, process, _, Reason},
    #state{group_mon = Mon, core = Core0, cfg = #cfg{stream = StreamId}} = State0
) ->
    ?LOG_WARNING("~ts group upload task crashed: ~p", [StreamId, Reason]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:group_upload_failed(Reason, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core, group_mon = undefined, pending_group_kind = undefined
        })};
handle_info(
    {'DOWN', Mon, process, _, Reason},
    #state{retention_mon = Mon, core = Core0, cfg = #cfg{stream = StreamId}} = State0
) ->
    ?LOG_WARNING("~ts retention evaluation task crashed: ~p", [StreamId, Reason]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(Reason, Core0),
    {noreply, execute_effects(Effects, State0#state{core = Core, retention_mon = undefined})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{
    cfg = #cfg{stream = StreamId},
    persist_mon = Mon,
    persist_pid = CommitPid,
    metrics_id = MetricsId
}) ->
    %% Kill any in-flight commit task to prevent orphaned Khepri writes.
    %% An orphaned write advances the revision, causing conflicts for the
    %% next incarnation of this replica reader.
    case CommitPid of
        undefined ->
            ok;
        _ ->
            demonitor(Mon, [flush]),
            exit(CommitPid, kill)
    end,
    ok = delete_metrics(MetricsId),
    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    ok.

format_status(#{state := State} = Status) ->
    Status#{state := format_state(State)}.

format_state(#state{
    cfg = #cfg{stream = StreamId, fragment_target_size = Target},
    log = Log,
    assembly = Assembly,
    core = Core
}) ->
    #{
        stream => StreamId,
        fragment_target_size => Target,
        core =>
            case Core of
                undefined -> undefined;
                _ -> rabbitmq_stream_s3_replica_reader_core:format_state(Core)
            end,
        log_next_offset =>
            case Log of
                undefined -> undefined;
                _ -> osiris_log:next_offset(Log)
            end,
        assembly =>
            case Assembly of
                undefined -> undefined;
                _ -> rabbitmq_stream_s3_fragment_assembly:info(Assembly)
            end
    }.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

identity_formatter(Evt) -> Evt.

%% Proactively register replica nodes for manifest broadcast.
%% Idempotent: skips nodes already in the replicas map.
register_replicas(Nodes, State) ->
    lists:foldl(fun register_replica/2, State, Nodes).

register_replica(
    Node,
    #state{
        replicas = Replicas,
        core = Core,
        broadcast_seq = Seq,
        cfg = #cfg{stream = StreamId, epoch = Epoch}
    } = State
) ->
    case maps:is_key(Node, Replicas) of
        true ->
            State;
        false ->
            MonRef = monitor(process, {rabbitmq_stream_s3_manifest_replica, Node}),
            Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
            rabbitmq_stream_s3_manifest_replica:sync(StreamId, Seq, Epoch, Manifest, Node),
            State#state{replicas = Replicas#{Node => MonRef}}
    end.

delete_manifest_objects(StreamId, Manifest) ->
    spawn(fun() ->
        logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
        GetGroupFun = fun(GroupRef) ->
            Key = rabbitmq_stream_s3:group_key(StreamId, GroupRef),
            case rabbitmq_stream_s3_api:get(Key) of
                {ok, Data} -> {ok, Data};
                {error, _} = Err -> Err
            end
        end,
        Refs = rabbitmq_stream_s3_fragment_iterator:all_refs(Manifest, GetGroupFun),
        Keys = lists:map(
            fun
                (#fragment_ref{} = FRef) ->
                    rabbitmq_stream_s3:fragment_key(StreamId, FRef);
                (#group_ref{} = GroupRef) ->
                    rabbitmq_stream_s3:group_key(StreamId, GroupRef)
            end,
            Refs
        ),
        rabbitmq_stream_s3_reaper:delete_objects(StreamId, Keys)
    end),
    ok.

%% ------------------------------------------------------------------
%% Persist (executed in spawned task)
%% ------------------------------------------------------------------

-spec do_upload_group(stream_id(), rabbitmq_stream_s3:kind(), binary()) ->
    {ok, rabbitmq_stream_s3:uid()} | {error, term()}.
do_upload_group(StreamId, Kind, Entries) ->
    Uid = rabbitmq_stream_s3:uid(),
    %% First offset from the first entry in the group.
    <<FirstOffset:64/unsigned, FirstTs:64/signed, _/binary>> = Entries,
    GroupRef = #group_ref{offset = FirstOffset, kind = Kind, uid = Uid},
    Key = rabbitmq_stream_s3:group_key(StreamId, GroupRef),
    Header = serialize_group_header(Kind, FirstOffset, FirstTs),
    Data = <<Header/binary, Entries/binary>>,
    case rabbitmq_stream_s3_api:put(Key, Data) of
        ok -> {ok, Uid};
        {error, _} = Err -> Err
    end.

serialize_group_header(Kind, FirstOffset, FirstTs) ->
    {Magic, Version} = group_magic(Kind),
    <<Magic/binary, Version:32/unsigned, FirstOffset:64/unsigned, 0:64/unsigned, FirstTs:64/signed,
        0:64/signed, 0:2/unsigned, 0:70/unsigned>>.

group_magic(?MANIFEST_KIND_GROUP) ->
    {<<?MANIFEST_GROUP_MAGIC>>, ?MANIFEST_GROUP_VERSION};
group_magic(?MANIFEST_KIND_KILO_GROUP) ->
    {<<?MANIFEST_KILO_GROUP_MAGIC>>, ?MANIFEST_KILO_GROUP_VERSION};
group_magic(?MANIFEST_KIND_MEGA_GROUP) ->
    {<<?MANIFEST_MEGA_GROUP_MAGIC>>, ?MANIFEST_MEGA_GROUP_VERSION}.

-spec do_commit(
    stream_id(), #manifest{}, non_neg_integer(), term(), rabbitmq_stream_s3_db:revision()
) ->
    {ok, rabbitmq_stream_s3_db:revision()} | {error, term()}.
do_commit(StreamId, Manifest, Epoch, Reference, ExpectedRevision) ->
    Uid = rabbitmq_stream_s3:uid(),
    Data = serialize_manifest(Manifest),
    Ref = #manifest_ref{epoch = Epoch, uid = Uid},
    Key = rabbitmq_stream_s3:manifest_key(StreamId, Ref),
    case rabbitmq_stream_s3_api:put(Key, Data) of
        ok ->
            case commit_khepri(StreamId, Epoch, Reference, ExpectedRevision, Uid) of
                {ok, OldRef, NewRevision} ->
                    delete_old_manifest(StreamId, OldRef),
                    {ok, NewRevision};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

-spec commit_khepri(
    stream_id(),
    non_neg_integer(),
    term(),
    rabbitmq_stream_s3_db:revision(),
    rabbitmq_stream_s3:uid()
) ->
    {ok, #manifest_ref{} | undefined, rabbitmq_stream_s3_db:revision()}
    | {error, term()}.
commit_khepri(_StreamId, _Epoch, undefined, ExpectedRevision, _Uid) ->
    %% No Khepri reference (test mode). Synthesize a revision.
    {ok, undefined, ExpectedRevision + 1};
commit_khepri(StreamId, Epoch, Reference, ExpectedRevision, Uid) ->
    case rabbitmq_stream_s3_db:put(StreamId, Reference, Epoch, ExpectedRevision, Uid) of
        {ok, {OldUid, OldEpoch}, NewRevision} ->
            {ok, #manifest_ref{epoch = OldEpoch, uid = OldUid}, NewRevision};
        {ok, undefined, NewRevision} ->
            {ok, undefined, NewRevision};
        {error, _} = Err ->
            Err
    end.

delete_old_manifest(_StreamId, undefined) ->
    ok;
delete_old_manifest(StreamId, #manifest_ref{} = Ref) ->
    Key = rabbitmq_stream_s3:manifest_key(StreamId, Ref),
    rabbitmq_stream_s3_reaper:delete_objects(StreamId, [Key]).

-spec serialize_manifest(#manifest{}) -> binary().
serialize_manifest(#manifest{
    first_offset = FirstOffset,
    next_offset = NextOffset,
    first_timestamp = FirstTs,
    first_last_timestamp = FirstLastTs,
    total_size = TotalSize,
    entries = Entries
}) ->
    <<?MANIFEST_ROOT_MAGIC, ?MANIFEST_ROOT_VERSION:32/unsigned, FirstOffset:64/unsigned,
        NextOffset:64/unsigned, FirstTs:64/signed, FirstLastTs:64/signed, 0:2/unsigned,
        TotalSize:70/unsigned, Entries/binary>>.

%% ------------------------------------------------------------------
%% Effect execution
%% ------------------------------------------------------------------

-spec execute_effects([rabbitmq_stream_s3_replica_reader_core:core_effect()], #state{}) -> #state{}.
execute_effects([], State) ->
    State;
execute_effects([Effect | Rest], State0) ->
    State = execute_effect(Effect, State0),
    execute_effects(Rest, State).

execute_effect({submit_transfer, Ref, _StreamId, Dir, Meta}, #state{cfg = Cfg} = State0) ->
    StreamId = Cfg#cfg.stream,
    Size = maps:get(size, Meta),
    Self = self(),
    Fun = fun() ->
        case upload_fragment(Dir, StreamId, Meta) of
            {ok, Uid} -> {ok, Uid};
            {error, _} = Err -> Err
        end
    end,
    rabbitmq_stream_s3_governor:submit(Fun, Size, Self, Ref),
    on_transfer_submitted(Ref, Size, State0);
execute_effect({resubmit_transfer, Ref, _StreamId, Dir, Meta}, #state{cfg = Cfg} = State0) ->
    StreamId = Cfg#cfg.stream,
    Size = maps:get(size, Meta),
    Self = self(),
    Fun = fun() ->
        case upload_fragment(Dir, StreamId, Meta) of
            {ok, Uid} -> {ok, Uid};
            {error, _} = Err -> Err
        end
    end,
    rabbitmq_stream_s3_governor:submit(Fun, Size, Self, Ref),
    %% on_transfer_result already decremented the gauges and removed Ref
    %% from transfer_sizes. Restore them so the eventual completion is
    %% accounted for correctly.
    on_transfer_submitted(Ref, Size, State0);
execute_effect(
    {upload_group, StreamId, Kind, Entries, _Pos, _Len},
    State0
) ->
    Self = self(),
    {_Pid, MonRef} = spawn_monitor(fun() ->
        logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
        Result =
            try
                do_upload_group(StreamId, Kind, Entries)
            catch
                Class:Reason:Stack ->
                    ?LOG_WARNING(
                        "Group upload crashed: ~p:~p~n~p", [Class, Reason, Stack]
                    ),
                    {error, {crashed, Reason}}
            end,
        Self ! {group_upload_result, Result}
    end),
    State0#state{group_mon = MonRef, pending_group_kind = Kind};
execute_effect(
    {start_persist, Manifest, Epoch, Reference, ExpectedRevision, _Edits},
    #state{cfg = #cfg{stream = StreamId}} = State
) ->
    Self = self(),
    {CommitPid, MonRef} = spawn_monitor(fun() ->
        logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
        Result = do_commit(StreamId, Manifest, Epoch, Reference, ExpectedRevision),
        Self ! {persist_result, Result}
    end),
    %% Snapshot the bytes that this persist will cover. New transfer
    %% completions during the persist accumulate in persist_pending_bytes
    %% and will be covered by the next persist. The bytes_in_persist
    %% gauge is unchanged here (its value equals
    %% persisting_bytes + persist_pending_bytes both before and after).
    Snapshot = State#state.persist_pending_bytes,
    State#state{
        persist_mon = MonRef,
        persist_pid = CommitPid,
        persisting_bytes = Snapshot,
        persist_pending_bytes = 0
    };
execute_effect({update_range, _FirstOffset, _NextOffset}, #state{cfg = Cfg, core = Core} = State) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(Cfg#cfg.stream, Manifest),
    on_persist_completed(Manifest, State);
execute_effect(
    {broadcast, StreamId, Edits},
    #state{replicas = Replicas, broadcast_seq = Seq0, cfg = #cfg{epoch = Epoch}} = State
) ->
    Seq = Seq0 + 1,
    maps:foreach(
        fun(Node, _MonRef) ->
            rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, Edits, Seq, Epoch, Node)
        end,
        Replicas
    ),
    State#state{broadcast_seq = Seq};
execute_effect(
    {evaluate_retention, _StreamId, _Dir},
    #state{core = Core, retention = Retention, cfg = Cfg} = State
) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    maybe_evaluate_retention(Manifest, State),
    maybe_evaluate_remote_retention(Manifest, Retention, Cfg#cfg.stream, State);
execute_effect({reply_waiters, Replies}, State) ->
    [gen_server:reply(From, Reply) || {From, Reply} <- Replies],
    State;
execute_effect({start_persist_timer, Ms}, #state{persist_timer = OldRef} = State) ->
    _ = cancel_timer(OldRef),
    Ref = erlang:send_after(Ms, self(), persist_timer),
    State#state{persist_timer = Ref};
execute_effect(cancel_persist_timer, #state{persist_timer = Ref} = State) ->
    _ = cancel_timer(Ref),
    State#state{persist_timer = undefined};
execute_effect(reinitialize, #state{cfg = #cfg{stream = StreamId}} = State0) ->
    ?LOG_INFO("~ts reinitializing after commit conflict", [StreamId]),
    Manifest = resolve_manifest(StreamId),
    State = on_manifest_resolved(Manifest, State0),
    {Core, _} = rabbitmq_stream_s3_replica_reader_core:init(Manifest, State#state.config),
    start_reading(State#state{
        core = Core,
        log = undefined,
        assembly = undefined,
        transfer_sizes = #{},
        persist_pending_bytes = 0,
        persisting_bytes = 0
    }).

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

%% ------------------------------------------------------------------
%% Retention
%% ------------------------------------------------------------------

-spec maybe_evaluate_retention(#manifest{}, #state{}) -> ok.
maybe_evaluate_retention(Manifest, #state{cfg = Cfg} = State) ->
    case Manifest#manifest.next_offset > 0 of
        true ->
            #cfg{stream = StreamId, dir = Dir, shared = Shared, counter = Cnt} = Cfg,
            Spec = [{'fun', rabbitmq_stream_s3_hooks:local_retention_fun(StreamId)}],
            inc(State, ?C_LOCAL_TIER_RETENTION_EVALUATIONS, 1),
            EvalFun = fun
                ({{FstOff, _}, _FstTs, NumSegLeft}) when is_integer(FstOff) ->
                    osiris_log_shared:set_first_chunk_id(Shared, FstOff),
                    update_counter(Cnt, FstOff, NumSegLeft);
                (_) ->
                    ok
            end,
            osiris_retention:eval(StreamId, Dir, Spec, EvalFun);
        false ->
            ok
    end.

update_counter(Cnt, FstOff, NumSegLeft) ->
    counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET, FstOff),
    counters:put(Cnt, ?C_OSIRIS_LOG_SEGMENTS, NumSegLeft).

-spec maybe_evaluate_remote_retention(
    #manifest{}, [osiris:retention_spec()], stream_id(), #state{}
) ->
    #state{}.
maybe_evaluate_remote_retention(_Manifest, [], _StreamId, State) ->
    State;
maybe_evaluate_remote_retention(Manifest, Retention, StreamId, #state{core = Core0} = State) ->
    inc(State, ?C_REMOTE_TIER_RETENTION_EVALUATIONS, 1),
    Now = erlang:system_time(millisecond),
    %% First try without group download (handles fragments-only case synchronously).
    case rabbitmq_stream_s3_manifest:evaluate_remote_retention(Manifest, Retention, Now) of
        unchanged ->
            %% No leading fragments to remove. If the first entry is a group,
            %% spawn an async task to download it and evaluate retention within.
            maybe_spawn_group_retention(Manifest, Retention, Now, StreamId, State);
        {Edit, Refs} ->
            on_remote_retention(Edit, Refs, StreamId, Core0, State)
    end.

%% Handle a retention result (synchronous fragments-only case or async completion).
-spec on_remote_retention(#edit{}, [#fragment_ref{} | #group_ref{}], stream_id(), term(), #state{}) ->
    #state{}.
on_remote_retention(Edit, Refs, StreamId, Core0, State) ->
    on_remote_retention_deleted(Refs, State),
    Keys = lists:map(
        fun
            (#fragment_ref{} = FRef) ->
                rabbitmq_stream_s3:fragment_key(StreamId, FRef);
            (#group_ref{} = GRef) ->
                rabbitmq_stream_s3:group_key(StreamId, GRef)
        end,
        Refs
    ),
    rabbitmq_stream_s3_reaper:delete_objects(StreamId, Keys),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_complete(Edit, Core0),
    execute_effects(Effects, State#state{core = Core}).

%% Spawn an async task to evaluate retention within a group object.
-spec maybe_spawn_group_retention(
    #manifest{}, [osiris:retention_spec()], integer(), stream_id(), #state{}
) -> #state{}.
maybe_spawn_group_retention(
    #manifest{entries = <<_:64, _:64/signed, _:64/signed, Kind:8, _:40, _:32, _/binary>>} =
        Manifest,
    Retention,
    Now,
    StreamId,
    State
) when Kind =/= ?MANIFEST_KIND_FRAGMENT ->
    Self = self(),
    GetGroupFun = rabbitmq_stream_s3_manifest:get_group_fun(StreamId),
    {_Pid, MonRef} = spawn_monitor(fun() ->
        logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
        Result =
            try
                rabbitmq_stream_s3_manifest:evaluate_remote_retention(
                    Manifest, Retention, Now, GetGroupFun
                )
            catch
                Class:Reason:Stack ->
                    ?LOG_WARNING(
                        "Retention evaluation crashed: ~p:~p~n~p", [Class, Reason, Stack]
                    ),
                    unchanged
            end,
        Self ! {retention_result, Result}
    end),
    Core = rabbitmq_stream_s3_replica_reader_core:retention_started(State#state.core),
    State#state{core = Core, retention_mon = MonRef};
maybe_spawn_group_retention(_Manifest, _Retention, _Now, _StreamId, State) ->
    State.

%% ------------------------------------------------------------------
%% Reading
%% ------------------------------------------------------------------

-spec resolve_manifest(stream_id()) -> #manifest{}.
resolve_manifest(StreamId) ->
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{} = M ->
            %% Cache hit. Ensure revision is current from Khepri.
            case catch rabbitmq_stream_s3_db:get(StreamId) of
                {ok, #{revision := Rev}} -> M#manifest{revision = Rev};
                _ -> M
            end;
        undefined ->
            case catch rabbitmq_stream_s3_db:get(StreamId) of
                {ok, #{uid := Uid, epoch := Epoch, revision := Rev}} ->
                    Ref = #manifest_ref{epoch = Epoch, uid = Uid},
                    Key = rabbitmq_stream_s3:manifest_key(StreamId, Ref),
                    case rabbitmq_stream_s3_api:get(Key, #{}) of
                        {ok, Data} ->
                            (parse_manifest_root(Data))#manifest{revision = Rev};
                        {error, _} ->
                            #manifest{}
                    end;
                _ ->
                    #manifest{}
            end
    end.

-spec parse_manifest_root(binary()) -> #manifest{}.
parse_manifest_root(?MANIFEST(FirstOffset, NextOffset, FirstTs, FirstLastTs, TotalSize, Entries)) ->
    #manifest{
        first_offset = FirstOffset,
        next_offset = NextOffset,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        total_size = TotalSize,
        entries = Entries
    }.

-spec start_reading(#state{}) -> #state{}.
start_reading(
    #state{
        cfg = #cfg{
            writer_pid = WriterPid
        }
    } = State
) ->
    case is_process_alive(WriterPid) of
        false ->
            State;
        true ->
            start_reading0(State)
    end.

start_reading0(
    #state{
        cfg = #cfg{
            writer_pid = WriterPid,
            fragment_target_size = TargetSize,
            stream = StreamId
        },
        core = Core
    } = State
) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    StartOffset = Manifest#manifest.next_offset,
    RepState = osiris_writer:query_replication_state(WriterPid),
    %% Proactively sync any replica nodes that are already running.
    %% If their manifest_replica isn't up yet, the cast is dropped and
    %% they will send {register_acceptor, ...} when their plugin starts.
    ReplicaNodes = maps:keys(RepState) -- [node()],
    State1 = register_replicas(ReplicaNodes, State),
    osiris:register_offset_listener(WriterPid, StartOffset, ?OFFSET_FORMATTER),
    ?LOG_DEBUG("~ts start_reading start_offset=~b", [StreamId, StartOffset]),
    try osiris_writer:init_data_reader(WriterPid, {StartOffset, empty}, #{}) of
        {ok, Log} ->
            Assembly = rabbitmq_stream_s3_fragment_assembly:new(TargetSize),
            drain(State1#state{log = Log, assembly = Assembly});
        {error, {offset_out_of_range, {LocalFirst, _}}} when LocalFirst > StartOffset ->
            %% Local retention deleted data the remote tier never received.
            %% Discard the remote manifest (the data would have been retained
            %% there too) and restart from the local log's first offset.
            ?LOG_INFO(
                "~ts local log ahead of manifest "
                "(local_first=~b, manifest_next=~b). "
                "Discarding remote manifest and restarting.",
                [StreamId, LocalFirst, StartOffset]
            ),
            inc(State1, ?C_LOCAL_LOG_AHEAD_RECOVERIES, 1),
            delete_manifest_objects(StreamId, Manifest),
            FreshManifest = #manifest{
                next_offset = LocalFirst, revision = Manifest#manifest.revision
            },
            {Core1, _} = rabbitmq_stream_s3_replica_reader_core:init(
                FreshManifest, State1#state.config
            ),
            ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, FreshManifest),
            start_reading(State1#state{core = Core1});
        {error, Reason} ->
            ?LOG_WARNING(
                "Failed to open data reader for stream ~ts: ~p",
                [StreamId, Reason]
            ),
            erlang:send_after(1000, self(), retry_resolve),
            State1
    catch
        missing_file ->
            ?LOG_WARNING(
                "Segment file missing for stream ~ts, retrying",
                [StreamId]
            ),
            %% Retention deleted the segment between listing and opening.
            %% Retry immediately (same pattern as osiris_log:init_offset_reader).
            start_reading(State1#state{core = Core, log = undefined, assembly = undefined})
    end.

-spec drain(#state{}) -> #state{}.
drain(#state{log = undefined} = State) ->
    State;
drain(
    #state{
        cfg = #cfg{
            stream = StreamId,
            writer_pid = WriterPid,
            fragment_target_size = TargetSize
        },
        log = Log0,
        assembly = Assembly0,
        core = Core0
    } = State
) ->
    case osiris_log:read_header(Log0) of
        {ok, Header, Log1} ->
            SegFile = osiris_log:get_current_file(Log1),
            SegOffset = rabbitmq_stream_s3:segment_file_offset(SegFile),
            Pos = maps:get(position, Header),
            NextPos = maps:get(next_position, Header),
            ChunkSize = NextPos - Pos,
            Chunk = #{
                chunk_id => maps:get(chunk_id, Header),
                timestamp => maps:get(timestamp, Header),
                num_records => maps:get(num_records, Header),
                data_size => maps:get(data_size, Header),
                position => Pos,
                next_position => NextPos,
                segment_offset => SegOffset,
                crc => maps:get(crc, Header)
            },
            %% Pipeline stage 1: bytes have been read from osiris and are
            %% about to be added to the assembly. Increment the cumulative
            %% drained counter and the in-assembly gauge. The gauge is
            %% decremented when the assembly is cut into a fragment.
            inc(State, ?C_BYTES_DRAINED, ChunkSize),
            inc_gauge(State, ?C_BYTES_IN_ASSEMBLY, ChunkSize),
            Assembly1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(Chunk, Assembly0),
            case rabbitmq_stream_s3_fragment_assembly:is_cut(Assembly1) of
                true ->
                    Meta = rabbitmq_stream_s3_fragment_assembly:metadata(Assembly1),
                    FragmentSize = maps:get(size, Meta),
                    %% Pipeline stage 1 -> 2: the fragment leaves the
                    %% assembly. Bytes are removed from the in-assembly
                    %% gauge; the in-transfer gauge is incremented in
                    %% on_transfer_submitted when the submit_transfer
                    %% effect fires.
                    dec(State, ?C_BYTES_IN_ASSEMBLY, FragmentSize),
                    IdxRecords = rabbitmq_stream_s3_fragment_assembly:index_records(Assembly1),
                    {Core, _Ref, Effects} =
                        rabbitmq_stream_s3_replica_reader_core:fragment_cut(
                            Meta#{index_records => IdxRecords}, Core0
                        ),
                    Assembly2 = rabbitmq_stream_s3_fragment_assembly:new(TargetSize),
                    State1 = execute_effects(
                        Effects, State#state{log = Log1, assembly = Assembly2, core = Core}
                    ),
                    drain(State1);
                false ->
                    drain(State#state{log = Log1, assembly = Assembly1})
            end;
        {end_of_stream, Log1} ->
            NextOffset = osiris_log:next_offset(Log1),
            osiris:register_offset_listener(WriterPid, NextOffset, ?OFFSET_FORMATTER),
            State#state{log = Log1, assembly = Assembly0};
        {error, Reason} ->
            ?LOG_ERROR("Read error for stream ~ts: ~p", [StreamId, Reason]),
            State
    end.

%% ------------------------------------------------------------------
%% Upload (executed inside governor task)
%% ------------------------------------------------------------------

%% 4 MiB
-define(READ_BUFFER_SIZE, 4_194_304).

-spec upload_fragment(
    directory(), stream_id(), rabbitmq_stream_s3_fragment_assembly:fragment_meta()
) ->
    {ok, rabbitmq_stream_s3:uid()} | {error, term()}.
upload_fragment(Dir, StreamId, Meta) ->
    Uid = rabbitmq_stream_s3:uid(),
    Spans = maps:get(spans, Meta),
    Size = maps:get(size, Meta),
    NumChunks = maps:get(num_chunks, Meta),
    FirstOffset = maps:get(first_offset, Meta),

    IndexSize = NumChunks * ?INDEX_RECORD_B,
    ContentLength = ?SEGMENT_HEADER_B + Size + IndexSize,
    Key = rabbitmq_stream_s3:fragment_key(StreamId, FirstOffset, Uid),

    maybe
        {ok, Stream0} ?= rabbitmq_stream_s3_api:stream_put(Key, ContentLength, #{}),
        Header = <<"OSIF", ?FRAGMENT_VERSION:32/unsigned>>,
        Stream1 = rabbitmq_stream_s3_api:stream_data(Stream0, Header),
        Crc0 = erlang:crc32(Header),
        {ok, Stream2, Crc1} ?= stream_spans(Stream1, Crc0, Dir, Spans),
        IdxRecords = maps:get(index_records, Meta),
        Stream3 = rabbitmq_stream_s3_api:stream_data(Stream2, IdxRecords),
        Crc = erlang:crc32(Crc1, IdxRecords),
        ok ?= rabbitmq_stream_s3_api:stream_finish(Stream3, Crc),
        {ok, Uid}
    end.

stream_spans(Stream, Crc, _Dir, []) ->
    {ok, Stream, Crc};
stream_spans(Stream0, Crc0, Dir, [{SegOffset, StartPos, EndPos} | Rest]) ->
    SegFile = filename:join(Dir, rabbitmq_stream_s3:offset_filename(SegOffset, <<"segment">>)),
    case file:open(SegFile, [read, raw, binary]) of
        {ok, Fd} ->
            Result =
                try
                    stream_span(Stream0, Crc0, Fd, StartPos, EndPos - StartPos)
                after
                    file:close(Fd)
                end,
            case Result of
                {ok, Stream1, Crc1} ->
                    stream_spans(Stream1, Crc1, Dir, Rest);
                {error, _} = Err ->
                    Err
            end;
        {error, Reason} ->
            {error, {open_failed, SegFile, Reason}}
    end.

stream_span(Stream, Crc, _Fd, _Pos, 0) ->
    {ok, Stream, Crc};
stream_span(Stream0, Crc0, Fd, Pos, Remaining) ->
    ReadSize = min(Remaining, ?READ_BUFFER_SIZE),
    case file:pread(Fd, Pos, ReadSize) of
        {ok, Data} ->
            Stream1 = rabbitmq_stream_s3_api:stream_data(Stream0, Data),
            Crc1 = erlang:crc32(Crc0, Data),
            stream_span(Stream1, Crc1, Fd, Pos + ReadSize, Remaining - ReadSize);
        eof ->
            {error, {unexpected_eof, Pos, ReadSize}};
        {error, _} = Err ->
            Err
    end.

%% ------------------------------------------------------------------
%% Metrics
%% ------------------------------------------------------------------

-doc "Field spec used by the prometheus collector to enumerate per-stream metrics.".
-spec counter_fields() -> [{atom(), pos_integer(), counter | gauge, string()}].
counter_fields() ->
    ?STREAM_COUNTERS.

-doc """
Initialise the node-level shadow counter set.

Called once at plugin start from `rabbitmq_stream_s3_api:init/0`. The
shadow counter holds the cluster-cumulative value of every per-stream
counter; the per-stream values are emitted on `/metrics/per-object`
while the labelless aggregate on `/metrics` is sourced from this
shadow. The shadow is independent of any stream's lifetime, which
preserves Prometheus monotonicity when streams are deleted.
""".
-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?NODE_COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?NODE_COUNTER_KEY, Cnt),
    ok.

%% Node-level counter ref accessor. Returns `undefined` when the plugin
%% has not initialised counters yet (e.g. some test setups). Callers
%% must handle that case.
node_counter() ->
    persistent_term:get(?NODE_COUNTER_KEY, undefined).

%% Build the seshat ID and labels and create the per-stream counter ref.
%% Falls back to a labelless registration when the reference is not a
%% queue resource (e.g. some test setups). seshat:format/2 with
%% labels => as_binary skips entries without labels, so unlabelled
%% counters never appear in the prometheus output.
init_metrics(StreamId, #resource{kind = queue, virtual_host = VHost, name = Name}) ->
    Id = {?MODULE, {VHost, Name}},
    Labels = #{
        module => ?MODULE,
        vhost => VHost,
        queue => Name,
        stream_id => StreamId
    },
    Cnt = seshat:new(rabbitmq_stream_s3, Id, ?STREAM_COUNTERS, Labels),
    {Id, Cnt};
init_metrics(StreamId, _Reference) ->
    %% Fallback: register without labels so the counter still works in tests
    %% that don't supply a queue resource. seshat:format/2 with
    %% labels => as_binary will skip these, keeping the prometheus output
    %% clean.
    Id = {?MODULE, StreamId},
    Cnt = seshat:new(rabbitmq_stream_s3, Id, ?STREAM_COUNTERS, #{}),
    {Id, Cnt}.

delete_metrics(undefined) ->
    ok;
delete_metrics(Id) ->
    seshat:delete(rabbitmq_stream_s3, Id).

%% Increment a per-stream counter by N. Writes both the per-stream
%% counter ref and the node-level shadow counter ref (if initialised).
%% No-op when per-stream metrics are not set up.
%%
%% This must only be called for fields whose type is `counter` in
%% ?STREAM_COUNTERS. The node-level shadow only contains counter
%% fields; calling this with a gauge index would write past the end of
%% the shadow array.
inc(#state{metrics = undefined}, _Idx, _N) ->
    ok;
inc(#state{metrics = Cnt}, Idx, N) when is_integer(N), N > 0 ->
    counters:add(Cnt, Idx, N),
    case node_counter() of
        undefined -> ok;
        NodeCnt -> counters:add(NodeCnt, Idx, N)
    end;
inc(_, _, _) ->
    ok.

%% Increment a per-stream gauge by N (for gauges used as in-flight
%% counters). Per-stream only. The node-level aggregate for gauges is
%% computed at scrape time by the prometheus collector folding
%% per-stream values.
inc_gauge(#state{metrics = undefined}, _Idx, _N) ->
    ok;
inc_gauge(#state{metrics = Cnt}, Idx, N) when is_integer(N), N > 0 ->
    counters:add(Cnt, Idx, N);
inc_gauge(_, _, _) ->
    ok.

%% Set a gauge.
set(#state{metrics = undefined}, _Idx, _V) ->
    ok;
set(#state{metrics = Cnt}, Idx, V) when is_integer(V) ->
    counters:put(Cnt, Idx, V).

%% Decrement a gauge by N.
dec(#state{metrics = undefined}, _Idx, _N) ->
    ok;
dec(#state{metrics = Cnt}, Idx, N) when is_integer(N), N > 0 ->
    counters:sub(Cnt, Idx, N);
dec(_, _, _) ->
    ok.

%% Called from execute_effect/2 for {submit_transfer, ...}.
on_transfer_submitted(Ref, Size, #state{transfer_sizes = Sizes} = State) ->
    inc_gauge(State, ?C_TRANSFERS_IN_FLIGHT, 1),
    %% Pipeline stage 2: bytes enter the transfer phase (governor queue
    %% or in-flight S3 PUT). Decremented in on_transfer_result.
    inc_gauge(State, ?C_BYTES_IN_TRANSFER, Size),
    State#state{transfer_sizes = Sizes#{Ref => Size}}.

%% Called from handle_info on transfer_result. Outcome is `ok' or
%% `{error, Reason}'.
on_transfer_result(Ref, Outcome, #state{transfer_sizes = Sizes} = State0) ->
    case maps:take(Ref, Sizes) of
        {Size, Sizes1} ->
            dec(State0, ?C_TRANSFERS_IN_FLIGHT, 1),
            dec(State0, ?C_BYTES_IN_TRANSFER, Size),
            case Outcome of
                ok ->
                    inc(State0, ?C_TRANSFERS_COMPLETED, 1),
                    inc(State0, ?C_BYTES_TRANSFERRED, Size),
                    %% Stage 3: bytes are now waiting for a manifest
                    %% persist to confirm them.
                    inc_gauge(State0, ?C_BYTES_IN_PERSIST, Size),
                    Pending = State0#state.persist_pending_bytes + Size,
                    State0#state{
                        transfer_sizes = Sizes1,
                        persist_pending_bytes = Pending
                    };
                {error, _} ->
                    inc(State0, ?C_TRANSFERS_FAILED, 1),
                    State0#state{transfer_sizes = Sizes1}
            end;
        error ->
            %% Should not happen but be defensive.
            State0
    end.

on_group_upload_completed(#state{pending_group_kind = Kind} = State) ->
    Idx =
        case Kind of
            ?MANIFEST_KIND_GROUP -> ?C_GROUPS_CREATED;
            ?MANIFEST_KIND_KILO_GROUP -> ?C_KILO_GROUPS_CREATED;
            ?MANIFEST_KIND_MEGA_GROUP -> ?C_MEGA_GROUPS_CREATED;
            undefined -> undefined
        end,
    case Idx of
        undefined -> ok;
        _ -> inc(State, Idx, 1)
    end,
    State#state{pending_group_kind = undefined}.

%% Called from execute_effect/2 for {update_range, ...}, which the core
%% only emits after a successful manifest persist.
on_persist_completed(#manifest{} = Manifest, State) ->
    inc(State, ?C_PERSISTS_COMPLETED, 1),
    inc(State, ?C_ROOTS_CREATED, 1),
    set(State, ?C_LAST_PERSIST_TIMESTAMP_MS, erlang:system_time(millisecond)),
    %% Pipeline stage 3: the bytes covered by this persist exit the
    %% pipeline. Decrement the in-persist gauge and bump the cumulative
    %% persisted-bytes counter. persisting_bytes was snapshotted from
    %% persist_pending_bytes at start_persist; reset it now that it has
    %% been credited.
    PersistedBytes = State#state.persisting_bytes,
    dec(State, ?C_BYTES_IN_PERSIST, PersistedBytes),
    inc(State, ?C_BYTES_PERSISTED, PersistedBytes),
    update_manifest_gauges(Manifest, State),
    State#state{persisting_bytes = 0}.

update_manifest_gauges(
    #manifest{
        first_offset = FirstOff,
        next_offset = NextOff,
        first_timestamp = FirstTs,
        total_size = TotalSize
    },
    State
) ->
    set(State, ?C_MANIFEST_FIRST_OFFSET, FirstOff),
    set(State, ?C_MANIFEST_NEXT_OFFSET, NextOff),
    %% first_timestamp may be -1 when manifest is empty: clamp to 0 for the
    %% gauge so dashboards can use `> 0` filters on the gauge.
    FirstTsMs =
        case FirstTs of
            T when T < 0 -> 0;
            T -> T
        end,
    set(State, ?C_MANIFEST_FIRST_TIMESTAMP_MS, FirstTsMs),
    set(State, ?C_REMOTE_BYTES, TotalSize),
    %% remote_messages is approximate: counts the offset range covered by
    %% the manifest. Non-negative because next >= first.
    set(State, ?C_REMOTE_MESSAGES, max(0, NextOff - FirstOff)),
    ok.

on_persist_failed(conflict, State0) ->
    inc(State0, ?C_PERSISTS_FAILED, 1),
    inc(State0, ?C_PERSIST_CONFLICTS, 1),
    return_persisting_bytes(State0);
on_persist_failed(_Reason, State0) ->
    inc(State0, ?C_PERSISTS_FAILED, 1),
    return_persisting_bytes(State0).

%% A failed persist returns its snapshotted bytes to persist_pending_bytes
%% so the next persist will cover them. The bytes_in_persist gauge is
%% unchanged (the bytes are still in stage 3, just back in the pending
%% bucket rather than the in-flight slot).
return_persisting_bytes(#state{persisting_bytes = 0} = State) ->
    State;
return_persisting_bytes(
    #state{persisting_bytes = Snapshot, persist_pending_bytes = Pending} = State
) ->
    State#state{persisting_bytes = 0, persist_pending_bytes = Pending + Snapshot}.

on_manifest_resolved(#manifest{next_offset = 0}, State) ->
    inc(State, ?C_MANIFESTS_RESOLVED_EMPTY, 1),
    update_manifest_gauges(#manifest{}, State),
    State;
on_manifest_resolved(#manifest{} = Manifest, State) ->
    inc(State, ?C_MANIFESTS_RESOLVED, 1),
    update_manifest_gauges(Manifest, State),
    State.

on_remote_retention_deleted(Refs, State) ->
    Counts = lists:foldl(
        fun
            (#fragment_ref{}, Acc) ->
                maps:update_with(?C_FRAGMENTS_DELETED, fun(N) -> N + 1 end, 1, Acc);
            (#group_ref{kind = ?MANIFEST_KIND_GROUP}, Acc) ->
                maps:update_with(?C_GROUPS_DELETED, fun(N) -> N + 1 end, 1, Acc);
            (#group_ref{kind = ?MANIFEST_KIND_KILO_GROUP}, Acc) ->
                maps:update_with(?C_KILO_GROUPS_DELETED, fun(N) -> N + 1 end, 1, Acc);
            (#group_ref{kind = ?MANIFEST_KIND_MEGA_GROUP}, Acc) ->
                maps:update_with(?C_MEGA_GROUPS_DELETED, fun(N) -> N + 1 end, 1, Acc)
        end,
        #{},
        Refs
    ),
    maps:foreach(fun(Idx, N) -> inc(State, Idx, N) end, Counts),
    ok.
