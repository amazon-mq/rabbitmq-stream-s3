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

-export([start_link/1, format_state/1, status/1, status/2]).
-export([evaluate_local_retention/1, evaluate_local_retention/2]).
-export([evaluate_remote_retention/1, evaluate_remote_retention/2]).
-export([force_fragment_cut/1, force_fragment_cut/2]).
-export([resolve_stream_id/2]).
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
-define(C_REMOTE_TIER_RETENTION_FAILURES, 22).
-define(C_MANIFEST_RESOLUTION_FAILURES, 23).
-define(C_REMOTE_TIER_AHEAD_RECOVERIES, 24).
-define(C_TRANSFER_RETRIES, 25).
-define(C_NONTRANSIENT_TRANSFER_RETRIES, 26).
%% Counters above; gauges below. The macro ?NODE_COUNTERS filters by
%% type, and seshat requires counter indexes to be 1..N sequential. Add
%% new counters before this divider and renumber the gauges that follow.
-define(C_TRANSFERS_IN_FLIGHT, 27).
-define(C_LAST_PERSIST_TIMESTAMP_MS, 28).
-define(C_MANIFEST_FIRST_OFFSET, 29).
-define(C_MANIFEST_NEXT_OFFSET, 30).
-define(C_MANIFEST_FIRST_TIMESTAMP_MS, 31).
-define(C_REMOTE_BYTES, 32).
-define(C_REMOTE_MESSAGES, 33).
-define(C_BYTES_IN_ASSEMBLY, 34).
-define(C_BYTES_IN_TRANSFER, 35).
-define(C_BYTES_IN_PERSIST, 36).
-define(C_UPLOAD_STALLED_OFFSET, 37).

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
        "Number of times a genuinely empty manifest was resolved on startup (a new "
        "stream, or one whose metadata node exists but that has not persisted a "
        "manifest). A transient resolution failure no longer counts here: it retries "
        "instead, see manifest_resolution_failures"},
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
    {remote_tier_ahead_recoveries, ?C_REMOTE_TIER_AHEAD_RECOVERIES, counter,
        "Times the replica reader discarded the remote manifest because the remote tier "
        "was ahead of the local log (the manifest's next_offset was beyond the local "
        "log's last offset after a leader election or data-directory loss)"},
    {bytes_drained_total, ?C_BYTES_DRAINED, counter,
        "Cumulative bytes the replica reader has drained from osiris (sum of chunk "
        "byte sizes read via osiris_log:read_header)"},
    {bytes_persisted_total, ?C_BYTES_PERSISTED, counter,
        "Cumulative bytes whose fragment manifest update has succeeded (each persist "
        "covers one or more uploaded fragments)"},
    {remote_tier_retention_failures, ?C_REMOTE_TIER_RETENTION_FAILURES, counter,
        "Number of remote tier retention evaluations that failed (crashed or timed "
        "out). Distinct from evaluations that ran and found nothing to remove."},
    {manifest_resolution_failures, ?C_MANIFEST_RESOLUTION_FAILURES, counter,
        "Number of times manifest resolution could not reach the metadata store or "
        "object store and scheduled a retry, rather than treating the remote tier as "
        "empty. A sustained non-zero rate means a stream cannot start tiering."},
    {transfer_retries, ?C_TRANSFER_RETRIES, counter,
        "Fragment upload retries scheduled (both transient and non-transient errors). "
        "A sustained non-zero rate means uploads are not succeeding on the first try."},
    {nontransient_transfer_retries, ?C_NONTRANSIENT_TRANSFER_RETRIES, counter,
        "Fragment upload retries scheduled for a confirmed-but-non-transient error "
        "(for example a checksum mismatch or an unexpected 4xx). The pipeline stalls "
        "at the offset until the fragment is durable, so a sustained non-zero rate "
        "means a stream's tiering is wedged."},
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
        "succeeded. Pipeline stage 3."},
    {upload_stalled_offset, ?C_UPLOAD_STALLED_OFFSET, gauge,
        "The offset at which the upload pipeline is currently stalled on a "
        "non-transient error, or 0 when not stalled. The pipeline retries this "
        "fragment with a backoff and makes no forward progress until it is durable."}
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
    %% Pure model of the async-task lifecycle (persist, group, retention,
    %% transfer). Owns the correlation decisions (deliver vs drop a result) and
    %% the pipeline gauges; see rabbitmq_stream_s3_replica_reader_tasks. The
    %% generation it carries is the join key with task_io below.
    tasks = rabbitmq_stream_s3_replica_reader_tasks:init() ::
        rabbitmq_stream_s3_replica_reader_tasks:tasks(),
    %% Runtime I/O handles for the in-flight tasks the model decides about: the
    %% monitor refs, pids and timer refs the shell needs to demonitor/kill/cancel.
    %% Single-in-flight families are keyed by family atom; transfer deadline
    %% timers are keyed by the transfer Ref. The model says which family/Ref to
    %% tear down; these are the handles to do it with.
    task_io = #{} :: #{persist | group | retention => #{atom() => term()}},
    transfer_timers = #{} :: #{reference() => reference()},
    %% Nodes registered for manifest broadcast.
    replicas = #{} :: #{node() => reference()},
    %% User-configured retention specs for remote tier evaluation.
    retention = [] :: [osiris:retention_spec()],
    %% Periodic commit-tick timer reference. Drives the core's persist
    %% scheduling; distinct from the async persist task (which lives in the task
    %% model and task_io).
    persist_timer :: reference() | undefined,
    %% Per-stream seshat counter ref. The id used to register this counter
    %% in the rabbitmq_stream_s3 seshat group, used for cleanup on terminate.
    metrics :: counters:counters_ref() | undefined,
    metrics_id :: term() | undefined,
    %% Keys queued for deletion after the next successful persist.
    %% Retention computes which objects to delete but we defer the actual
    %% S3 DELETE until the manifest persist that records their removal
    %% completes. This eliminates the race where a reader sees a stale
    %% manifest pointing to already-deleted objects (issue #166).
    deferred_deletions = [] :: [#fragment_ref{} | #group_ref{}],
    %% Fragments currently being retried after a non-transient (confirmed-fatal)
    %% upload error, keyed by transfer Ref with their first offset. The pipeline
    %% stalls head-of-line, but several fragments can each hit a non-transient
    %% error, so this is a set; the upload_stalled_offset gauge reflects the
    %% lowest (head) stalled offset, or 0 when empty.
    stalled_transfers = #{} :: #{reference() => osiris:offset()},
    %% Set when the core asks to shut down (the stream's metadata node was
    %% deleted). The handler that executed the effect returns {stop, normal}.
    stopping = false :: boolean()
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

-doc "Return the formatted status of the replica reader for a stream.".
-spec status(stream_id()) -> {ok, map()} | {error, term()}.
status(StreamId) ->
    case call(StreamId, status) of
        {error, _} = Err -> Err;
        Result -> {ok, with_bucket_status(Result)}
    end.

-doc "Return the formatted status by vhost and queue name.".
-spec status(rabbit_types:vhost(), binary()) -> {ok, map()} | {error, term()}.
status(VHost, QueueName) ->
    case call(VHost, QueueName, status) of
        {error, _} = Err -> Err;
        Result -> {ok, with_bucket_status(Result)}
    end.

%% Fold the node-level bucket accessibility into the per-stream status. This is
%% a local call on the node already serving the status request, so the CLI
%% avoids a second round-trip. A missing monitor (disabled or restarting) yields
%% `undefined`, which the CLI renders as "unknown".
with_bucket_status(Result) ->
    Bucket =
        try
            rabbitmq_stream_s3_bucket_monitor:status()
        catch
            _:_ -> undefined
        end,
    Result#{bucket => Bucket}.

-doc "Trigger local retention evaluation for a stream on the current node.".
-spec evaluate_local_retention(stream_id()) -> ok | {error, term()}.
evaluate_local_retention(StreamId) ->
    case rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}) of
        Pid when is_pid(Pid) ->
            safe_call(Pid, evaluate_local_retention);
        undefined ->
            rabbitmq_stream_s3_manifest_replica:evaluate_local_retention(StreamId)
    end.

-doc "Trigger local retention evaluation by vhost and queue name.".
-spec evaluate_local_retention(rabbit_types:vhost(), binary()) -> ok | {error, term()}.
evaluate_local_retention(VHost, QueueName) ->
    case resolve_stream_id(VHost, QueueName) of
        {ok, StreamId} -> evaluate_local_retention(StreamId);
        {error, _} = Err -> Err
    end.

-doc "Trigger remote retention evaluation for a stream on the current node.".
-spec evaluate_remote_retention(stream_id()) -> ok | {error, term()}.
evaluate_remote_retention(StreamId) ->
    call(StreamId, evaluate_remote_retention).

-doc "Trigger remote retention evaluation by vhost and queue name.".
-spec evaluate_remote_retention(rabbit_types:vhost(), binary()) -> ok | {error, term()}.
evaluate_remote_retention(VHost, QueueName) ->
    call(VHost, QueueName, evaluate_remote_retention).

-doc "Force the current in-progress fragment to cut and upload immediately.".
-spec force_fragment_cut(stream_id()) -> ok | {error, term()}.
force_fragment_cut(StreamId) ->
    call(StreamId, force_fragment_cut).

-doc "Force fragment cut by vhost and queue name.".
-spec force_fragment_cut(rabbit_types:vhost(), binary()) -> ok | {error, term()}.
force_fragment_cut(VHost, QueueName) ->
    call(VHost, QueueName, force_fragment_cut).

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
        rabbitmq_stream_s3_config:fragment_target_size()
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
    %% Mark this node's manifest cache row pending before returning: init runs
    %% inside the on_init(writer) hook, synchronously within osiris_log:init,
    %% so the row exists before the member finishes starting and therefore
    %% before any consumer can attach a reader. Readers fail closed on pending
    %% until resolve_manifest seeds the resolved manifest. Insert-if-absent: a
    %% row surviving a transient reader restart is never downgraded.
    ok = rabbitmq_stream_s3_manifest_replica:mark_pending(StreamId),
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
handle_call(status, _From, State) ->
    {reply, format_state(State), State};
handle_call(evaluate_local_retention, _From, #state{core = undefined} = State) ->
    {reply, {error, manifest_not_resolved}, State};
handle_call(evaluate_local_retention, _From, #state{core = Core} = State) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    maybe_evaluate_retention(Manifest, State),
    {reply, ok, State};
handle_call(evaluate_remote_retention, _From, #state{core = undefined} = State) ->
    {reply, {error, manifest_not_resolved}, State};
handle_call(
    evaluate_remote_retention,
    _From,
    #state{core = Core, retention = Retention, cfg = #cfg{stream = StreamId}} = State
) ->
    case rabbitmq_stream_s3_replica_reader_core:pending_prefix_rewrite(Core) of
        none ->
            Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
            State1 = maybe_evaluate_remote_retention(Manifest, Retention, StreamId, State),
            {reply, ok, State1};
        Blocker ->
            %% A manifest-prefix rewrite (an async retention evaluation or a
            %% rebalance) is already in flight. Spawning a second evaluation
            %% would capture the same pre-retention snapshot and recompute the
            %% identical prefix-truncation edit, which would be applied twice.
            %% Refuse and name the blocker so the operator can retry once it
            %% settles.
            {reply, {error, {in_progress, Blocker}}, State}
    end;
handle_call(force_fragment_cut, _From, #state{assembly = undefined} = State) ->
    {reply, {error, no_assembly}, State};
handle_call(
    force_fragment_cut,
    _From,
    #state{assembly = Assembly0, core = Core0, cfg = #cfg{fragment_target_size = TargetSize}} =
        State
) ->
    Meta = rabbitmq_stream_s3_fragment_assembly:metadata(Assembly0),
    case maps:get(num_chunks, Meta) of
        0 ->
            {reply, {error, empty_assembly}, State};
        _ ->
            IdxRecords = rabbitmq_stream_s3_fragment_assembly:index_records(Assembly0),
            {Core, _Ref, Effects} =
                rabbitmq_stream_s3_replica_reader_core:fragment_cut(
                    Meta#{index_records => IdxRecords}, Core0
                ),
            FragmentSize = maps:get(size, Meta),
            dec(State, ?C_BYTES_IN_ASSEMBLY, FragmentSize),
            Assembly1 = rabbitmq_stream_s3_fragment_assembly:new(TargetSize),
            State1 = execute_effects(
                Effects, State#state{assembly = Assembly1, core = Core}
            ),
            {reply, ok, State1}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast({register_acceptor, _Node}, #state{core = undefined} = State) ->
    {noreply, State};
handle_cast(
    {register_acceptor, Node},
    #state{
        replicas = Replicas,
        core = Core,
        cfg = #cfg{stream = StreamId, epoch = Epoch}
    } = State
) ->
    case maps:is_key(Node, Replicas) of
        true ->
            {noreply, State};
        false ->
            MonRef = monitor(process, {rabbitmq_stream_s3_manifest_replica, Node}),
            %% Sync the *persisted* manifest, not the live one: sync_manifest/4
            %% tags the sync with the manifest's revision as the sequence number
            %% (see the {broadcast, _} effect), and that revision only advances
            %% when a persist commits. Pairing the live manifest with the
            %% persisted revision would cache an edit the replica was never told
            %% about, which the next broadcast then re-delivers in-sequence so
            %% the replica double-applies it (silent, unhealable divergence: no
            %% gap is ever observed). Do not change this back to manifest/1.
            Manifest = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(Core),
            sync_manifest(StreamId, Epoch, Manifest, Node),
            {noreply, State#state{replicas = Replicas#{Node => MonRef}}}
    end;
handle_cast(
    {retention_updated, Retention}, #state{core = Core, cfg = #cfg{stream = StreamId}} = State
) ->
    UserRetention = [S || S <- Retention, element(1, S) =/= 'fun'],
    State1 = State#state{retention = UserRetention},
    case Core of
        undefined ->
            {noreply, State1};
        _ ->
            Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
            {noreply, maybe_evaluate_remote_retention(Manifest, UserRetention, StreamId, State1)}
    end;
handle_cast({resync, _Node}, #state{core = undefined} = State) ->
    {noreply, State};
handle_cast(
    {resync, Node},
    #state{
        core = Core,
        cfg = #cfg{stream = StreamId, epoch = Epoch}
    } = State
) ->
    %% Persisted manifest, not live: its revision is the sequence number the
    %% replica gap-detects against. See the register_acceptor handler.
    Manifest = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(Core),
    sync_manifest(StreamId, Epoch, Manifest, Node),
    {noreply, State};
handle_cast(reconcile_replicas, #state{cfg = #cfg{writer_pid = WriterPid}} = State) ->
    %% Re-seed the writer node's own manifest cache if it was lost (its
    %% manifest_replica restarted) and no persist has refilled it - otherwise an
    %% idle stream's remote tier becomes invisible to consumers on this node.
    %% reconcile_replicas covers other nodes but never node() itself.
    maybe_reseed_local_cache(State),
    %% Re-sync any replica node the writer currently has that this reader is no
    %% longer broadcasting to - a replica drops out of the map when its
    %% manifest_replica restarts (the monitored DOWN removes it), losing its
    %% manifest cache. Driven periodically by the reconciler. register_replicas/2
    %% skips nodes already registered, so this is a no-op once every replica is
    %% synced. Going through the writer's replication state avoids any
    %% queue-record or coordinator lookup.
    case is_process_alive(WriterPid) of
        true ->
            RepState = osiris_writer:query_replication_state(WriterPid),
            ReplicaNodes = maps:keys(RepState) -- [node()],
            {noreply, register_replicas(ReplicaNodes, State)};
        false ->
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_continue(resolve_manifest, State0) ->
    {noreply, resolve_and_start(State0)}.

handle_info({osiris_offset, _Ref, _Offset}, State0) ->
    State = drain(State0),
    {noreply, State};
handle_info(retry_resolve, State0) ->
    {noreply, resolve_and_start(State0)};
handle_info({transfer_result, Ref, Result}, #state{tasks = Tasks0} = State0) ->
    %% Read the transferred byte count before the step removes the transfer; it
    %% is needed for the cumulative bytes-transferred counter on success.
    Size = transfer_byte_size(Ref, Tasks0),
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {transfer_result, Ref, Result}, Tasks0
    ),
    apply_transfer_decisions(Decisions, Size, State0#state{tasks = Tasks});
handle_info({transfer_deadline, Ref, Token}, #state{tasks = Tasks0} = State0) ->
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {transfer_deadline, Ref, Token}, Tasks0
    ),
    apply_transfer_decisions(Decisions, 0, State0#state{tasks = Tasks});
handle_info({retry_transfer, Ref, Dir, Meta}, #state{tasks = Tasks0} = State0) ->
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {retry_transfer, Ref}, Tasks0
    ),
    apply_retry_decisions(Decisions, Dir, Meta, State0#state{tasks = Tasks});
handle_info({group_upload_result, Gen, Result}, #state{tasks = Tasks0} = State0) ->
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {group_result, Gen, Result}, Tasks0
    ),
    apply_group_decisions(Decisions, State0#state{tasks = Tasks});
handle_info({retention_result, Gen, Result}, #state{tasks = Tasks0} = State0) ->
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {retention_result, Gen, Result}, Tasks0
    ),
    apply_retention_decisions(Decisions, State0#state{tasks = Tasks});
handle_info({retention_timeout, Gen}, #state{tasks = Tasks0} = State0) ->
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {retention_timeout, Gen}, Tasks0
    ),
    apply_retention_timeout_decisions(Decisions, State0#state{tasks = Tasks});
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
handle_info({persist_result, Gen, Result}, #state{tasks = Tasks0} = State0) ->
    %% Read the bytes this persist covered before the step frees the slot; needed
    %% for the cumulative bytes-persisted counter on success.
    PersistedBytes = persisting_snapshot(Tasks0),
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {persist_result, Gen, Result}, Tasks0
    ),
    apply_persist_decisions(Decisions, PersistedBytes, State0#state{tasks = Tasks});
handle_info({anchor_write_result, ok}, #state{core = Core0} = State0) ->
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:anchor_write_complete(Core0),
    {noreply, execute_effects(Effects, State0#state{core = Core})};
handle_info(
    {anchor_write_result, {error, Reason}},
    #state{cfg = #cfg{stream = StreamId}} = State
) ->
    Delay = rabbitmq_stream_s3_config:upload_retry_delay_ms(),
    ?LOG_WARNING(
        "~ts anchor write failed (~p); retrying in ~bms. Fragment uploads are "
        "held until the anchor is durable.",
        [StreamId, Reason, Delay]
    ),
    erlang:send_after(Delay, self(), retry_anchor_write),
    {noreply, State};
handle_info(retry_anchor_write, #state{core = Core0} = State0) ->
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:anchor_write_failed(Core0),
    {noreply, execute_effects(Effects, State0#state{core = Core})};
handle_info({'DOWN', Mon, process, _Pid, Reason}, State) ->
    %% A monitored async task crashed. Its 'DOWN' carries the monitor ref, which
    %% identifies the family (and, via the model's slot, its generation): a crash
    %% is delivered to the task model as a failure of the live task, exactly as an
    %% error result would be. A 'DOWN' whose monitor matches no tracked task (a
    %% task already torn down by recovery, flushed) falls through to a no-op.
    case task_family_for_mon(Mon, State) of
        {ok, Family} -> apply_task_crash(Family, Reason, State);
        error -> {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(
    _Reason,
    #state{
        cfg = #cfg{stream = StreamId},
        task_io = Io,
        metrics_id = MetricsId,
        stopping = Stopping
    } = State
) ->
    %% Kill any in-flight commit task to prevent orphaned Khepri writes.
    %% An orphaned write advances the revision, causing conflicts for the
    %% next incarnation of this replica reader.
    case Io of
        #{persist := #{mon := Mon, pid := CommitPid}} ->
            demonitor(Mon, [flush]),
            exit(CommitPid, kill);
        _ ->
            ok
    end,
    %% Close the open data reader so its segment file descriptors are released
    %% promptly rather than at process teardown.
    _ = close_log(State),
    ok = delete_metrics(MetricsId),
    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    rabbitmq_stream_s3_manifest:evict_group_cache(StreamId),
    %% On the writer node the manifest cache row is written by put_manifest and
    %% has no osiris member registered to monitor it, so it is released here
    %% when the stream is being torn down. `stopping` is set only when the
    %% stream's metadata node was deleted; a transient reader restart leaves it
    %% false so the cache survives (and is reseeded), avoiding a window where a
    %% consumer fails closed on a manifest this writer already knows.
    case Stopping of
        true -> rabbitmq_stream_s3_manifest_replica:forget(StreamId);
        false -> ok
    end,
    ok.

format_status(#{state := State} = Status) ->
    Status#{state := format_state(State)}.

format_state(#state{
    cfg = #cfg{stream = StreamId, fragment_target_size = Target},
    log = Log,
    assembly = Assembly,
    core = Core,
    transfer_timers = Timers
}) ->
    #{
        stream => StreamId,
        node => node(),
        fragment_target_size => Target,
        transfer_deadlines_armed => maps:size(Timers),
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

call(StreamId, Msg) ->
    case find_reader(StreamId) of
        {ok, Pid} -> safe_call(Pid, Msg);
        undefined -> {error, {not_found, StreamId}}
    end.

%% gen_server:call exits the caller on timeout and if the reader is already
%% gone (noproc) or on a remote node that is unreachable (the pid can be on any
%% node, see find_reader/1). These admin/retention entry points are specced
%% ok | {error, term()} and reached over rabbit_misc:rpc_call from the CLI, so a
%% raw exit would escape the contract (surfacing as {badrpc, {'EXIT', _}} that
%% the command output/1 clauses do not match). Convert the exit into a named
%% error so it stays within the contract. Mirrors remote_reader:read/5.
safe_call(Pid, Msg) ->
    safe_call(Pid, Msg, 5000).

safe_call(Pid, Msg, Timeout) ->
    try
        gen_server:call(Pid, Msg, Timeout)
    catch
        exit:{timeout, _} ->
            {error, timeout};
        exit:Reason ->
            {error, {reader_down, Reason}}
    end.

call(VHost, QueueName, Msg) ->
    case resolve_stream_id(VHost, QueueName) of
        {ok, StreamId} ->
            call(StreamId, Msg);
        {error, _} = Err ->
            Err
    end.

-spec find_reader(stream_id()) -> {ok, pid()} | undefined.
find_reader(StreamId) ->
    find_reader(StreamId, [node() | nodes()]).

find_reader(_StreamId, []) ->
    undefined;
find_reader(StreamId, [Node | Rest]) ->
    case rabbitmq_stream_s3_registry:whereis_name({StreamId, Node}) of
        undefined -> find_reader(StreamId, Rest);
        Pid -> {ok, Pid}
    end.

-doc "Resolve a vhost and queue name to the internal stream id.".
-spec resolve_stream_id(rabbit_types:vhost(), binary()) ->
    {ok, stream_id()} | {error, {not_found, binary()}}.
resolve_stream_id(VHost, QueueName) ->
    QName = rabbit_misc:r(VHost, queue, QueueName),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            #{name := StreamId} = amqqueue:get_type_state(Q),
            {ok, rabbitmq_stream_s3:ensure_stream_id(StreamId)};
        {error, not_found} ->
            {error, {not_found, QueueName}}
    end.

identity_formatter(Evt) -> Evt.

%% Re-seed the writer node's own manifest cache from the committed manifest when
%% it is missing. The cache is normally filled by each persist's put_manifest,
%% but if the node's manifest_replica restarts and the stream is then idle (no
%% further persist), the cache stays empty and consumers on this node fail
%% closed on it (retrying) instead of resolving the already-known manifest.
%% Only repairs a genuine cache miss; a no-op once seeded.
maybe_reseed_local_cache(#state{core = undefined}) ->
    ok;
maybe_reseed_local_cache(#state{core = Core, cfg = #cfg{stream = StreamId, epoch = Epoch}}) ->
    case rabbitmq_stream_s3_replica_reader_core:persisted_manifest(Core) of
        #manifest{next_offset = 0} ->
            %% No remote tier yet; nothing to cache.
            ok;
        #manifest{} = Manifest ->
            %% A missing row means the manifest_replica restarted and lost it;
            %% a pending row is a marker leaked from an incarnation that died
            %% between init and resolution. Both leave readers fail-closed on
            %% a manifest this writer already knows, so re-seed.
            Reseed = fun() ->
                ?LOG_INFO(
                    "Reconciliation: re-seeding the local manifest cache for "
                    "stream ~ts after a manifest_replica restart",
                    [StreamId]
                ),
                ok = rabbitmq_stream_s3_manifest_replica:put_manifest(
                    StreamId, Manifest, Epoch
                )
            end,
            rabbitmq_stream_s3_manifest_replica:with_manifest(StreamId, #{
                resolved => fun(_) -> ok end,
                pending => Reseed
            })
    end.

%% Proactively register replica nodes for manifest broadcast.
%% Idempotent: skips nodes already in the replicas map.
register_replicas(Nodes, State) ->
    lists:foldl(fun register_replica/2, State, Nodes).

register_replica(_Node, #state{core = undefined} = State) ->
    State;
register_replica(
    Node,
    #state{
        replicas = Replicas,
        core = Core,
        cfg = #cfg{stream = StreamId, epoch = Epoch}
    } = State
) ->
    case maps:is_key(Node, Replicas) of
        true ->
            State;
        false ->
            MonRef = monitor(process, {rabbitmq_stream_s3_manifest_replica, Node}),
            %% Persisted manifest, not live: its revision is the sequence number.
            %% See the register_acceptor handler.
            Manifest = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(Core),
            sync_manifest(StreamId, Epoch, Manifest, Node),
            State#state{replicas = Replicas#{Node => MonRef}}
    end.

%% Force every registered replica to drop its cached manifest and adopt the
%% given one via a full sync, tagged with that manifest's revision as the
%% sequence number. Used after a manifest reset (local-log-ahead and
%% remote-tier-ahead recovery), which installs a fresh manifest carrying the
%% discarded manifest's revision: subsequent broadcasts continue at the next
%% revision and stay in sequence relative to this sync. Casts to a given node
%% are FIFO from this process, so each replica observes the sync before the
%% next apply_edits.
-spec sync_all_replicas(#manifest{}, #state{}) -> ok.
sync_all_replicas(
    Manifest,
    #state{
        replicas = Replicas,
        cfg = #cfg{stream = StreamId, epoch = Epoch}
    }
) ->
    maps:foreach(
        fun(Node, _MonRef) ->
            sync_manifest(StreamId, Epoch, Manifest, Node)
        end,
        Replicas
    ).

%% Send a full sync of Manifest to a replica node, tagged with the manifest's
%% revision as the broadcast sequence number. The revision (the stream's Khepri
%% payload_version) is the single source of the sequence: it advances by exactly
%% one per persist, the persist's CAS rejects any competing same-epoch write so
%% consecutive revisions are contiguous, and it is durable so a restarted reader
%% resumes the sequence where the previous incarnation left off rather than
%% restarting at zero (which a replica would reject as stale).
-spec sync_manifest(stream_id(), non_neg_integer(), #manifest{}, node()) -> ok.
sync_manifest(StreamId, Epoch, #manifest{revision = Revision} = Manifest, Node) ->
    rabbitmq_stream_s3_manifest_replica:sync(StreamId, Revision, Epoch, Manifest, Node).

gc_stream_async(StreamId, WriterEpoch) ->
    spawn(fun() ->
        logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
        %% Pass this writer's epoch so the sweep skips when a consistent read of
        %% the committed epoch shows this writer has been deposed. Runs in the
        %% spawned task, not the reader, so the quorum-requiring read cannot stall
        %% the reader on a partition.
        rabbitmq_stream_s3_gc:run_stream(StreamId, #{mode => delete, writer_epoch => WriterEpoch})
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
                    delete_old_manifest(StreamId, Ref, OldRef),
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

delete_old_manifest(StreamId, NewRef, OldRef) ->
    case stale_manifest_ref(NewRef, OldRef) of
        {delete, Ref} ->
            Key = rabbitmq_stream_s3:ref_key(StreamId, Ref),
            rabbitmq_stream_s3_reaper:delete_objects(StreamId, [Key]);
        skip ->
            ok
    end.

%% The prior manifest ref to delete after a commit, or `skip` when nothing can be
%% safely deleted. The uid is a fresh 32-bit random value per commit
%% (rabbitmq_stream_s3:uid/0), so it can collide with the immediately-prior
%% commit's uid at the same epoch. A colliding ref maps to the same S3 key as the
%% manifest just written, so deleting "the old" object would delete the live
%% committed manifest. Skip the delete in that case.
-spec stale_manifest_ref(#manifest_ref{}, #manifest_ref{} | undefined) ->
    {delete, #manifest_ref{}} | skip.
stale_manifest_ref(_NewRef, undefined) ->
    skip;
stale_manifest_ref(Ref, Ref) ->
    skip;
stale_manifest_ref(_NewRef, #manifest_ref{} = OldRef) ->
    {delete, OldRef}.

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

execute_effect({write_anchor, StreamId, Reference}, State) ->
    %% Write the per-stream anchor asynchronously, then feed the result back into
    %% the core. The core holds the fragments' submit_transfer effects until
    %% anchor_write_complete is fed in, so the anchor commits before the first
    %% fragment PUT. The spawned process always reports a result (failures included)
    %% so the core is never left waiting.
    Self = self(),
    _ = spawn(fun() ->
        Result =
            try
                rabbitmq_stream_s3_db:put_anchor(StreamId, Reference)
            catch
                Class:Reason -> {error, {Class, Reason}}
            end,
        Self ! {anchor_write_result, Result}
    end),
    State;
execute_effect({submit_transfer, Ref, _StreamId, Dir, Meta}, #state{cfg = Cfg} = State0) ->
    StreamId = Cfg#cfg.stream,
    submit_upload(Ref, Dir, StreamId, Meta),
    on_transfer_submitted(Ref, maps:get(size, Meta), State0);
execute_effect({resubmit_transfer, Ref, _StreamId, Dir, Meta, Attempt}, #state{cfg = Cfg} = State0) ->
    StreamId = Cfg#cfg.stream,
    inc(State0, ?C_TRANSFER_RETRIES, 1),
    Delay = transient_retry_delay(Attempt),
    _ =
        case Delay of
            0 ->
                %% First transient retry: resubmit immediately, preserving the
                %% prior behaviour for a one-off blip.
                submit_upload(Ref, Dir, StreamId, Meta);
            _ ->
                erlang:send_after(Delay, self(), {retry_transfer, Ref, Dir, Meta})
        end,
    %% The failure already removed Ref from the model. Re-add it (a fresh slot
    %% with a new deadline token) so the eventual completion is accounted for.
    on_transfer_submitted(Ref, maps:get(size, Meta), State0);
execute_effect(
    {resubmit_transfer_delayed, Ref, _StreamId, Dir, Meta, Reason, Attempt},
    #state{cfg = Cfg} = State0
) ->
    StreamId = Cfg#cfg.stream,
    inc(State0, ?C_TRANSFER_RETRIES, 1),
    inc(State0, ?C_NONTRANSIENT_TRANSFER_RETRIES, 1),
    Offset = maps:get(first_offset, Meta),
    State1 = mark_transfer_stalled(Ref, Offset, State0),
    Delay = nontransient_retry_delay(Attempt),
    ?LOG_WARNING(
        "~ts fragment upload at offset ~b failed with non-transient error ~p "
        "(attempt ~b); retrying in ~bms. The upload pipeline and local-tier "
        "cleanup are stalled at this offset until the fragment is durable in S3.",
        [StreamId, Offset, Reason, Attempt, Delay]
    ),
    erlang:send_after(Delay, self(), {retry_transfer, Ref, Dir, Meta}),
    %% The failure already removed Ref from the model. Re-add it now (the fragment
    %% is still pending and its bytes are still in the pipeline); the retry_transfer
    %% timer above triggers the actual re-upload after the backoff.
    on_transfer_submitted(Ref, maps:get(size, Meta), State1);
execute_effect(
    {upload_group, StreamId, Kind, Entries, _Pos, _Len},
    #state{tasks = Tasks0, task_io = Io} = State0
) ->
    Self = self(),
    Gen = rabbitmq_stream_s3_replica_reader_tasks:generation(Tasks0),
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
        Self ! {group_upload_result, Gen, Result}
    end),
    {Tasks, []} = rabbitmq_stream_s3_replica_reader_tasks:step({spawn_group, Kind}, Tasks0),
    State0#state{tasks = Tasks, task_io = Io#{group => #{mon => MonRef}}};
execute_effect(
    {start_persist, Manifest, Epoch, Reference, ExpectedRevision, _Edits},
    #state{cfg = #cfg{stream = StreamId}, tasks = Tasks0, task_io = Io} = State
) ->
    Self = self(),
    %% The result is tagged with the generation the task was spawned in so a
    %% result that outlives a recovery is correlated to the right incarnation by
    %% the task model rather than by the monitor field alone.
    Gen = rabbitmq_stream_s3_replica_reader_tasks:generation(Tasks0),
    {CommitPid, MonRef} = spawn_monitor(fun() ->
        logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
        Result = do_commit(StreamId, Manifest, Epoch, Reference, ExpectedRevision),
        Self ! {persist_result, Gen, Result}
    end),
    %% step(spawn_persist) snapshots the model's pending bytes into the persist
    %% slot (the bytes this commit will cover) and zeroes pending; bytes_in_persist
    %% is unchanged. New transfer completions during the persist accumulate in the
    %% model's pending pool and are covered by the next persist.
    {Tasks, []} = rabbitmq_stream_s3_replica_reader_tasks:step(spawn_persist, Tasks0),
    derive_gauges(State#state{
        tasks = Tasks,
        task_io = Io#{persist => #{mon => MonRef, pid => CommitPid}}
    });
execute_effect({update_range, _FirstOffset, _NextOffset}, #state{cfg = Cfg, core = Core} = State) ->
    %% Publish the *persisted* manifest, not the live one. This effect is
    %% emitted by persist_complete, and the cache + range table it updates gate
    %% local retention (get_range reads next_offset). Persist runs in a spawned
    %% task, so more completions can advance the live manifest past what is
    %% durably committed in Khepri while the persist is in flight. Publishing
    %% the live manifest here would let local retention reclaim segments
    %% covering offsets that are not yet durable; a crash before the next
    %% persist then loses those offsets from both tiers (the #206 durability
    %% hole). persisted_manifest/1 is the manifest that was just committed; its
    %% first/next offsets equal the _FirstOffset/_NextOffset this effect carries
    %% by construction (persist_complete sets last_persisted_manifest and emits
    %% these offsets in the same batch), so the carried offsets are redundant
    %% and intentionally ignored. Do not change this back to manifest/1.
    Manifest = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(Core),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(Cfg#cfg.stream, Manifest, Cfg#cfg.epoch),
    on_persist_completed(Manifest, State);
execute_effect(
    {broadcast, StreamId, Edits},
    #state{replicas = Replicas, core = Core, cfg = #cfg{epoch = Epoch}} = State
) ->
    %% Tag the broadcast with the persisted manifest's revision as the sequence
    %% number. This effect is emitted by persist_complete, which has already
    %% stamped the core's persisted manifest with the new revision (the Khepri
    %% payload_version), so persisted_manifest/1 carries it here. The revision
    %% advances by exactly one per persist, and the persist's CAS rejects any
    %% competing same-epoch write, so consecutive broadcasts are contiguous
    %% (revision N then N+1) - the replica's gap detection holds without a
    %% separate counter, and a restarted reader resumes the sequence from the
    %% durable revision instead of restarting at zero.
    #manifest{revision = Seq} = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(Core),
    maps:foreach(
        fun(Node, _MonRef) ->
            rabbitmq_stream_s3_manifest_replica:apply_edits(StreamId, Edits, Seq, Epoch, Node)
        end,
        Replicas
    ),
    State;
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
    resolve_and_start(reset_for_recovery(State0));
execute_effect(stop, #state{tasks = Tasks} = State) ->
    %% The stream's metadata node was deleted (the queue was removed). Cancel
    %% every outstanding fragment upload in one batch so it stops occupying
    %% upload-pool capacity for a stream that no longer exists, then mark the
    %% reader for shutdown; the handler that ran this effect returns
    %% {stop, normal} via maybe_stop/1.
    Refs = maps:keys(rabbitmq_stream_s3_replica_reader_tasks:transfers(Tasks)),
    rabbitmq_stream_s3_governor:cancel(Refs),
    State#state{stopping = true}.

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

%% Return the gen_server result for a handler, honouring a stop requested by
%% the core (e.g. on stream deletion). transient restart means a normal stop
%% is not restarted by the supervisor.
maybe_stop(#state{stopping = true} = State) ->
    {stop, normal, State};
maybe_stop(State) ->
    {noreply, State}.

%% ------------------------------------------------------------------
%% Retention
%% ------------------------------------------------------------------

-spec maybe_evaluate_retention(#manifest{}, #state{}) -> ok.
maybe_evaluate_retention(
    #manifest{first_offset = ManifestFirstOffset, next_offset = ManifestNextOffset},
    #state{cfg = Cfg} = State
) when ManifestNextOffset > 0 ->
    #cfg{stream = StreamId, dir = Dir, shared = Shared, counter = Cnt} = Cfg,
    Spec = [{'fun', rabbitmq_stream_s3_hooks:local_retention_fun(StreamId)}],
    inc(State, ?C_LOCAL_TIER_RETENTION_EVALUATIONS, 1),
    EvalFun = fun
        ({{FstOff, _}, _FstTs, NumSegLeft}) when is_integer(FstOff) ->
            osiris_log_shared:set_first_chunk_id(Shared, FstOff),
            update_counter(Cnt, min(FstOff, ManifestFirstOffset), NumSegLeft);
        (_) ->
            ok
    end,
    osiris_retention:eval(StreamId, Dir, Spec, EvalFun);
maybe_evaluate_retention(#manifest{}, #state{}) ->
    ok.

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
    case rabbitmq_stream_s3_replica_reader_core:pending_prefix_rewrite(Core0) of
        none ->
            inc(State, ?C_REMOTE_TIER_RETENTION_EVALUATIONS, 1),
            Now = erlang:system_time(millisecond),
            %% First try without group download (handles the fragments-only
            %% case synchronously).
            case rabbitmq_stream_s3_manifest:evaluate_remote_retention(Manifest, Retention, Now) of
                unchanged ->
                    %% No leading fragments to remove. If the first entry is a
                    %% group, spawn an async task to download it and evaluate
                    %% retention within.
                    maybe_spawn_group_retention(Manifest, Retention, Now, StreamId, State);
                {Edit, Refs} ->
                    on_remote_retention(Edit, Refs, StreamId, Core0, State)
            end;
        Blocker ->
            %% A manifest-prefix rewrite is already in flight. Remote retention
            %% rewrites that same prefix, so evaluating it now would compute an
            %% edit against a manifest that is about to change (racing the
            %% in-flight writer: Single mutator). A second remote-retention task
            %% in particular would capture the same pre-retention snapshot and
            %% recompute the identical prefix-truncation edit, which would then
            %% be applied a second time: splicing out live entries and
            %% double-counting total_size. The corrupt edit is persisted and
            %% broadcast in-sequence, so every replica applies it with no gap
            %% detected. Skip; retention is re-evaluated by the next
            %% persist_complete once the in-flight work finishes. This guards the
            %% manual CLI and retention_updated triggers; the automatic
            %% post-persist path is already gated by persist_complete.
            ?LOG_DEBUG(
                "~ts skipping remote retention evaluation: ~ts in flight",
                [StreamId, Blocker]
            ),
            State
    end.

%% Handle a retention result (synchronous fragments-only case or async completion).
-spec on_remote_retention(#edit{}, [#fragment_ref{} | #group_ref{}], stream_id(), term(), #state{}) ->
    #state{}.
on_remote_retention(Edit, Refs, StreamId, Core0, State) ->
    on_remote_retention_deleted(Refs, StreamId, State),
    %% Defer deletion until the persist that includes this retention edit
    %% completes. Deleting now would leave a window where the manifest cache
    %% still lists these objects but S3 has already removed them (issue #166).
    Deferred = Refs ++ State#state.deferred_deletions,
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_complete(Edit, Core0),
    execute_effects(Effects, State#state{core = Core, deferred_deletions = Deferred}).

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
    #state{tasks = Tasks0, task_io = Io} = State,
    Gen = rabbitmq_stream_s3_replica_reader_tasks:generation(Tasks0),
    GetGroupFun = rabbitmq_stream_s3_manifest:get_cached_group_fun(StreamId),
    {Pid, MonRef} = spawn_monitor(fun() ->
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
                    {failed, {Class, Reason}}
            end,
        Self ! {retention_result, Gen, Result}
    end),
    Core = rabbitmq_stream_s3_replica_reader_core:retention_started(State#state.core),
    Timeout = rabbitmq_stream_s3_config:retention_task_timeout(),
    TRef = erlang:send_after(Timeout, Self, {retention_timeout, Gen}),
    {Tasks, []} = rabbitmq_stream_s3_replica_reader_tasks:step(spawn_retention, Tasks0),
    State#state{
        core = Core,
        tasks = Tasks,
        task_io = Io#{retention => #{mon => MonRef, pid => Pid, timer => TRef}}
    };
maybe_spawn_group_retention(_Manifest, _Retention, _Now, _StreamId, State) ->
    State.

%% ------------------------------------------------------------------
%% Reading
%% ------------------------------------------------------------------

-spec resolve_manifest(stream_id()) -> {ok, #manifest{}} | {retry, term()}.
resolve_manifest(StreamId) ->
    %% A pending row is this incarnation's own init marker (or a
    %% predecessor's), same as a missing row entirely: there is no cached
    %% manifest to trust either way, so resolve authoritatively.
    FromStore = fun() -> resolve_manifest_from_store(StreamId) end,
    rabbitmq_stream_s3_manifest_replica:with_manifest(StreamId, #{
        resolved => fun(M) ->
            case classify_cache_result(M, catch rabbitmq_stream_s3_db:get(StreamId)) of
                {ok, _} = Ok -> Ok;
                resolve_from_store -> resolve_manifest_from_store(StreamId)
            end
        end,
        pending => FromStore
    }).

%% Decide whether the locally cached manifest may be trusted on resolution
%% (startup, promotion, reinitialize, recovery). Split out like
%% classify_store_result/2 so the trust decision is unit-testable without a real
%% metadata store.
%%
%% The cache may be trusted only when its revision already matches the
%% authoritative Khepri revision. The previous code instead stamped the current
%% revision onto the cached manifest unconditionally (M#manifest{revision =
%% Rev}). That defeated the persist CAS: ExpectedRevision is taken from
%% last_persisted_manifest.revision, so a stale cached manifest stamped with the
%% current revision would pass the CAS, and the next persist would overwrite the
%% committed manifest with stale content, regressing next_offset and skipping
%% committed offsets. This is reachable on promotion and, most acutely, on
%% reinitialize after a persist conflict, where another writer has already
%% advanced the committed revision and this node's cache is known stale.
%%
%% A replica's cached revision tracks only its last full sync (broadcast edits
%% do not bump it), so a promoted replica almost always falls through to the
%% authoritative store resolution, which is correct. The writer's own node
%% keeps a current cached revision (persist_complete stamps it), so its
%% legitimate cache hits are still served without an object-store GET.
-spec classify_cache_result(#manifest{}, term()) -> {ok, #manifest{}} | resolve_from_store.
classify_cache_result(#manifest{revision = Rev} = M, {ok, #{revision := Rev}}) ->
    %% Cache revision matches the committed revision: trust it.
    {ok, M};
classify_cache_result(#manifest{}, {ok, #{revision := _Stale}}) ->
    %% Cache lags (or leads) the committed revision: resolve authoritatively.
    resolve_from_store;
classify_cache_result(#manifest{}, _MetadataUnavailable) ->
    %% Metadata store unavailable: we cannot confirm the cache is current, so do
    %% not trust it. resolve_manifest_from_store/1 returns {retry, _} on a
    %% transient error rather than assuming the cache or an empty manifest
    %% (Local authority).
    resolve_from_store.

%% Resolve the manifest from the metadata store and object store on a cache
%% miss. An empty manifest is returned only when the stream is genuinely empty
%% (no metadata node, or a node with no manifest object yet). A transient store
%% or object-store error returns `{retry, Reason}` instead of an empty manifest:
%% treating a hidden manifest as empty would make the writer re-tier from the
%% local log and orphan the real remote objects (Local authority).
-spec resolve_manifest_from_store(stream_id()) -> {ok, #manifest{}} | {retry, term()}.
resolve_manifest_from_store(StreamId) ->
    classify_store_result(catch rabbitmq_stream_s3_db:get(StreamId), StreamId).

%% Decide what a metadata-store lookup means for manifest resolution. Split out
%% (with classify_object_result/3) so the classification is unit-testable
%% without a real store.
-spec classify_store_result(term(), stream_id()) -> {ok, #manifest{}} | {retry, term()}.
classify_store_result({ok, #{uid := Uid, epoch := Epoch, revision := Rev}}, StreamId) ->
    Ref = #manifest_ref{epoch = Epoch, uid = Uid},
    Key = rabbitmq_stream_s3:manifest_key(StreamId, Ref),
    classify_object_result(rabbitmq_stream_s3_api:get(Key, #{}), Rev, Key);
classify_store_result({error, not_found}, _StreamId) ->
    %% No metadata node: a genuinely new stream. Empty is correct.
    {ok, #manifest{}};
classify_store_result({error, Reason}, _StreamId) ->
    %% The metadata store errored. We cannot tell whether this stream is new or
    %% has a manifest, so we must not assume empty.
    {retry, {metadata_unavailable, Reason}};
classify_store_result(Other, _StreamId) ->
    %% A `catch`-ed exception from the store lookup.
    {retry, {metadata_unavailable, Other}}.

%% Decide what an object-store GET of the manifest object means, given the
%% Khepri revision that referenced it.
-spec classify_object_result(term(), non_neg_integer(), binary()) ->
    {ok, #manifest{}} | {retry, term()}.
classify_object_result({ok, Data}, Rev, _Key) ->
    {ok, (parse_manifest_root(Data))#manifest{revision = Rev}};
classify_object_result({error, not_found}, Rev, _Key) when Rev =< 0 ->
    %% No committed revision: a genuinely new stream that has not persisted a
    %% manifest. Empty is correct.
    {ok, #manifest{}};
classify_object_result({error, not_found}, _Rev, Key) ->
    %% Khepri references a manifest at a committed revision (> 0) but the object
    %% is gone. The object is always PUT before the Khepri CAS, so this is not a
    %% new stream: it is typically a stale local Khepri read still pointing at a
    %% manifest object that a newer committed revision already deleted. Fail
    %% closed and retry rather than misclassifying the stream as empty and
    %% re-tiering over the real remote objects (Local authority). Resolution
    %% retries until the local Khepri replica catches up to the newer revision.
    {retry, {manifest_object_missing, Key}};
classify_object_result({error, Reason}, _Rev, Key) ->
    %% Khepri references a manifest at this revision but the object store could
    %% not be read (a transient error, not a 404). Do not assume empty.
    {retry, {manifest_object_fetch_failed, Key, Reason}}.

%% Resolve the manifest and continue startup. On a transient resolution failure,
%% log, count, and schedule a retry rather than starting with an empty manifest:
%% proceeding empty would risk re-tiering over the real remote objects (Local
%% authority). Reuses the existing `retry_resolve` self-message.
-spec resolve_and_start(#state{}) -> #state{}.
resolve_and_start(#state{cfg = #cfg{stream = StreamId}} = State0) ->
    case resolve_manifest(StreamId) of
        {ok, Manifest} ->
            State1 = on_manifest_resolved(Manifest, State0),
            {Core0, _Effects} = rabbitmq_stream_s3_replica_reader_core:init(
                Manifest, State1#state.config
            ),
            %% Carry any await_offset waiters from the discarded core so a caller
            %% blocked across the rebuild is not dropped without a reply.
            {Core, WaiterEffects} = rabbitmq_stream_s3_replica_reader_core:carry_waiters(
                State1#state.core, Core0
            ),
            start_reading(execute_effects(WaiterEffects, State1#state{core = Core}));
        {retry, Reason} ->
            %% Logged at WARNING (not ERROR) like the sibling init_data_reader
            %% retry in start_reading: this retries once a second, so a
            %% sustained store outage must not flood the log. The
            %% manifest_resolution_failures counter is the alertable signal.
            ?LOG_WARNING(
                "~ts could not resolve its manifest (~p); retrying rather than "
                "treating the remote tier as empty",
                [StreamId, Reason]
            ),
            inc(State0, ?C_MANIFEST_RESOLUTION_FAILURES, 1),
            erlang:send_after(1000, self(), retry_resolve),
            State0
    end.

%% Build the empty manifest the local-log-ahead recovery (the `reset` operation)
%% installs at the local floor. first_offset = next_offset = LocalFirst so an
%% empty manifest (Frag = empty) carries f = n, as the Coverage and Accounting
%% invariants require, and so first_offset only moves forward: it feeds the
%% first_offset counter and GC. See the Reset safety invariant in
%% docs/invariants.md.
-spec reset_manifest(osiris:offset(), rabbitmq_stream_s3_db:revision()) -> #manifest{}.
reset_manifest(LocalFirst, Revision) ->
    #manifest{first_offset = LocalFirst, next_offset = LocalFirst, revision = Revision}.

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

%% Close the open osiris data reader, if any, and clear it from the state.
%% The osiris_log state holds open file descriptors to local segment files.
%% Abandoning it (setting log = undefined) without closing leaks those
%% descriptors: when retention then unlinks the segments, the kernel cannot
%% reclaim the disk space until the descriptors are closed. Every path that
%% restarts the reader (manifest reinitialize, trimmed-segment recovery) must
%% close the old log first.
-spec close_log(#state{}) -> #state{}.
close_log(#state{log = undefined} = State) ->
    State;
close_log(#state{log = Log} = State) ->
    ok = osiris_log:close(Log),
    State#state{log = undefined}.

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
            restart_at_local_floor(LocalFirst, Manifest, State1);
        {error, {offset_out_of_range, {LocalFirst, _LastOffset}}} ->
            %% The manifest's next_offset is beyond the local log's last offset
            %% (LocalFirst =< StartOffset, so this is not the local-ahead case
            %% above): a leader election or a power-loss timeline change left the
            %% local log shorter than the committed manifest. This is the "remote
            %% tier ahead of local" case in failure-modes.md. Local data is
            %% authoritative, so discard the remote manifest and restart from the
            %% local log's first offset. Without this the open at StartOffset
            %% fails identically on every retry_resolve, wedging tiering forever
            %% and growing local disk unbounded (local retention only reclaims
            %% offsets below the pinned next_offset). The empty-local-log shape
            %% (offset_out_of_range = empty) is intentionally left to the retry
            %% path below: an empty local log can be a transient writer-recovery
            %% state, and resetting then would prematurely discard recoverable
            %% remote data.
            ?LOG_WARNING(
                "~ts remote tier ahead of local log "
                "(manifest_next=~b, local_first=~b). "
                "Discarding remote manifest and restarting from the local log.",
                [StreamId, StartOffset, LocalFirst]
            ),
            inc(State1, ?C_REMOTE_TIER_AHEAD_RECOVERIES, 1),
            restart_at_local_floor(LocalFirst, Manifest, State1);
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
            start_reading((close_log(State1))#state{core = Core, assembly = undefined})
    end.

%% Discard the remote manifest and restart tiering from the local log's first
%% offset. Shared by both manifest/local divergence directions handled in
%% start_reading0/1: local-ahead (local retention trimmed the local log past the
%% manifest) and remote-ahead (the manifest's next_offset is beyond the local
%% log after a timeline change). In both, local data is authoritative.
-spec restart_at_local_floor(osiris:offset(), #manifest{}, #state{}) -> #state{}.
restart_at_local_floor(
    LocalFirst,
    #manifest{revision = Revision},
    #state{cfg = #cfg{stream = StreamId, epoch = Epoch}} = State
) ->
    FreshManifest = reset_manifest(LocalFirst, Revision),
    {Core0, _} = rabbitmq_stream_s3_replica_reader_core:init(FreshManifest, State#state.config),
    %% Carry any await_offset waiters from the discarded core (see resolve_and_start/1).
    {Core1, WaiterEffects} = rabbitmq_stream_s3_replica_reader_core:carry_waiters(
        State#state.core, Core0
    ),
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, FreshManifest, Epoch),
    %% Propagate the reset to replicas. The fresh manifest carries the discarded
    %% manifest's revision (the broadcast sequence number), so the sync is not
    %% rejected as stale and subsequent broadcasts continue in sequence. Replicas
    %% still hold the pre-reset manifest; without this resync the next broadcast
    %% would land in-sequence and splice an edit onto a manifest the writer has
    %% already discarded, corrupting every replica (the same silent divergence as
    %% an unsynced register).
    ok = sync_all_replicas(FreshManifest, State),
    gc_stream_async(StreamId, Epoch),
    start_reading(execute_effects(WaiterEffects, State#state{core = Core1})).

%% Whether the live local log has been trimmed past the manifest's next_offset.
%% When true, the segment backing the stalled head fragment is permanently gone
%% (user retention outran the upload), so no upload retry can ever succeed.
%% next_offset is the only offset that can be stalled at the head of the
%% in-flight queue: the contiguity invariant (assert_contiguous/2) guarantees
%% every uploaded fragment begins exactly where the manifest ends, so the head
%% fragment's first offset equals next_offset. Comparing against next_offset is
%% therefore equivalent to comparing against the stalled fragment's offset, and
%% mirrors the check start_reading0/1 makes against StartOffset at reader init.
-spec local_log_ahead(#state{}) ->
    {true, osiris:offset(), osiris:offset()} | false.
local_log_ahead(#state{cfg = #cfg{shared = Shared}, core = Core}) ->
    NextOffset = (rabbitmq_stream_s3_replica_reader_core:manifest(Core))#manifest.next_offset,
    LocalFirst = osiris_log_shared:first_chunk_id(Shared),
    case LocalFirst > NextOffset of
        true -> {true, LocalFirst, NextOffset};
        false -> false
    end.

%% Recover from a permanently-trimmed segment by re-resolving the manifest and
%% restarting the read. The manifest is still stalled at next_offset, so
%% start_reading0/1 re-opens the data reader there, hits its offset_out_of_range
%% branch (LocalFirst > StartOffset), and performs the manifest discard, the
%% jump to LocalFirst, and the recovery-counter increment. Resetting the
%% in-flight bookkeeping here (matching the reinitialize effect) abandons the
%% transfers that were queued behind the stall; their late results are dropped
%% by the stale-Ref clause of handle_info/2. The fragments they uploaded are
%% left in S3 for orphan garbage collection. resolve_and_start/1 handles a
%% transient resolution failure by scheduling a retry, so recovery is not lost
%% to a store blip.
-spec handle_local_log_ahead(osiris:offset(), osiris:offset(), term(), #state{}) -> #state{}.
handle_local_log_ahead(
    LocalFirst, NextOffset, Reason, #state{cfg = #cfg{stream = StreamId}} = State0
) ->
    ?LOG_WARNING(
        "~ts fragment upload at offset ~b failed because its segment was "
        "deleted by stream retention (~p). The local log first offset (~b) is "
        "ahead of the manifest next offset (~b), so the range is gone from both "
        "tiers. Discarding the remote manifest and restarting from the local "
        "log first offset; the trimmed range will not be available in the "
        "remote tier.",
        [StreamId, NextOffset, Reason, LocalFirst, NextOffset]
    ),
    %% Count the mid-stream (upload-path) recovery too, not just the reader-init
    %% path (start_reading0/1), so the #225 trimmed-segment recovery is visible
    %% in the local_log_ahead_recoveries metric and not only in the logs.
    inc(State0, ?C_LOCAL_LOG_AHEAD_RECOVERIES, 1),
    resolve_and_start(reset_for_recovery(State0)).

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
    Size = maps:get(size, Meta),
    NumChunks = maps:get(num_chunks, Meta),
    FirstOffset = maps:get(first_offset, Meta),

    IndexSize = NumChunks * ?INDEX_RECORD_B,
    ContentLength = ?SEGMENT_HEADER_B + Size + IndexSize,
    Key = rabbitmq_stream_s3:fragment_key(StreamId, FirstOffset, Uid),

    maybe
        {ok, Stream0} ?= rabbitmq_stream_s3_api:stream_put(Key, ContentLength, #{}),
        %% From here Stream0 owns a pooled connection and the active-request
        %% gauge (acquired by stream_put). stream_finish releases them on its
        %% own paths, but if streaming the body fails first (e.g. a segment was
        %% deleted by local retention between the drain and this pread, so the
        %% open/pread returns an error) stream_finish is never reached and both
        %% would leak. stream_body/3 aborts the request on any such failure, so
        %% by the time it returns {ok, ...} the only remaining release is the
        %% stream_finish below.
        {ok, Stream3, Crc} ?= stream_body(Stream0, Dir, Meta),
        ok ?= rabbitmq_stream_s3_api:stream_finish(Stream3, Crc),
        {ok, Uid}
    end.

%% Stream the fragment header, segment-span bodies, and index records into the
%% open PUT identified by Stream0. Returns the final stream handle and CRC ready
%% for stream_finish, or {error, _} after aborting the request (releasing the
%% connection and active-request gauge that stream_put acquired). Every failure
%% here happens before stream_finish, so aborting is unconditionally correct and
%% cannot double-release.
-spec stream_body(
    rabbitmq_stream_s3_api:async_state(),
    directory(),
    rabbitmq_stream_s3_fragment_assembly:fragment_meta()
) ->
    {ok, rabbitmq_stream_s3_api:async_state(), non_neg_integer()} | {error, term()}.
stream_body(Stream0, Dir, Meta) ->
    Spans = maps:get(spans, Meta),
    IdxRecords = maps:get(index_records, Meta),
    Header = <<"OSIF", ?FRAGMENT_VERSION:32/unsigned>>,
    Result =
        try
            Stream1 = rabbitmq_stream_s3_api:stream_data(Stream0, Header),
            Crc0 = erlang:crc32(Header),
            case stream_spans(Stream1, Crc0, Dir, Spans) of
                {ok, Stream2, Crc1} ->
                    Stream3 = rabbitmq_stream_s3_api:stream_data(Stream2, IdxRecords),
                    Crc = erlang:crc32(Crc1, IdxRecords),
                    {ok, Stream3, Crc};
                {error, _} = Err ->
                    Err
            end
        catch
            Class:Reason ->
                {error, {upload_body_crashed, Class, Reason}}
        end,
    case Result of
        {ok, _, _} ->
            Result;
        {error, _} = Error ->
            rabbitmq_stream_s3_api:stream_abort(Stream0),
            Error
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
            %% file:pread/3 on a raw file may return fewer bytes than requested:
            %% a short read is permitted in raw mode. Advance Pos and Remaining
            %% by the number of bytes actually read, never by ReadSize.
            %% Advancing by ReadSize on a short read would skip the unread bytes,
            %% so the streamed body would be shorter than the Content-Length
            %% declared to stream_put/3 and S3 would reject the PUT (the
            %% upload fails and retries rather than committing a corrupt object).
            case byte_size(Data) of
                0 ->
                    %% A zero-byte read that is not eof would spin forever;
                    %% treat it as an unexpected truncation.
                    {error, {unexpected_eof, Pos, ReadSize}};
                Got ->
                    Stream1 = rabbitmq_stream_s3_api:stream_data(Stream0, Data),
                    Crc1 = erlang:crc32(Crc0, Data),
                    stream_span(Stream1, Crc1, Fd, Pos + Got, Remaining - Got)
            end;
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

%% Backoff (with equal jitter) before resubmitting a transient upload failure.
%% Attempt 1 retries immediately, preserving the prior behaviour for a one-off
%% blip; attempts 2+ back off exponentially from task_retry_delay_constant up to
%% task_retry_delay_max_ms.
transient_retry_delay(1) ->
    0;
transient_retry_delay(Attempt) when Attempt > 1 ->
    Base = rabbitmq_stream_s3_config:task_retry_delay_constant(),
    Exp = rabbitmq_stream_s3_config:task_retry_delay_exponent(),
    Max = rabbitmq_stream_s3_config:task_retry_delay_max_ms(),
    %% Attempt - 1 so the first backed-off retry (attempt 2) uses Base.
    rabbitmq_stream_s3_util:equal_jitter(
        rabbitmq_stream_s3_util:backoff_delay(Attempt - 1, Base, Exp, Max)
    ).

%% Backoff (with equal jitter) before resubmitting a non-transient upload
%% failure. A confirmed-fatal error is unlikely to clear on a tight retry, so
%% the backoff starts at upload_retry_delay_ms and grows to a higher ceiling.
nontransient_retry_delay(Attempt) when Attempt >= 1 ->
    Base = rabbitmq_stream_s3_config:upload_retry_delay_ms(),
    Exp = rabbitmq_stream_s3_config:task_retry_delay_exponent(),
    Max = rabbitmq_stream_s3_config:upload_retry_delay_max_ms(),
    rabbitmq_stream_s3_util:equal_jitter(
        rabbitmq_stream_s3_util:backoff_delay(Attempt, Base, Exp, Max)
    ).

%% Submit a fragment upload to the governor. Shared by the initial submit
%% and the immediate/delayed retry paths.
submit_upload(Ref, Dir, StreamId, Meta) ->
    Self = self(),
    Size = maps:get(size, Meta),
    Fun = fun() -> upload_fragment(Dir, StreamId, Meta) end,
    rabbitmq_stream_s3_governor:submit(Fun, Size, Self, Ref).

%% Arm the per-transfer liveness deadline. The timer message carries a fresh
%% Token; on expiry the {transfer_deadline, ...} handler only acts if the
%% Token still matches the one stored for Ref, which rules out acting on a
%% timer that fired into the mailbox just before it was cancelled.
%% Called from execute_effect/2 for {submit_transfer, ...} and the resubmit
%% effects. The single point at which a transfer becomes outstanding (initial
%% submit and every resubmit): occupy a slot in the task model with a fresh
%% deadline token, arm the liveness deadline timer, and re-derive the pipeline
%% gauges. The model removes the Ref on every path that ends the transfer
%% (result, deadline, recover), so a resubmit always finds the Ref absent.
on_transfer_submitted(Ref, Size, #state{tasks = Tasks0, transfer_timers = Timers} = State) ->
    Token = make_ref(),
    Deadline = rabbitmq_stream_s3_config:transfer_deadline_ms(),
    TimerRef = erlang:send_after(Deadline, self(), {transfer_deadline, Ref, Token}),
    {Tasks, []} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {spawn_transfer, Ref, Size, Token}, Tasks0
    ),
    derive_gauges(State#state{tasks = Tasks, transfer_timers = Timers#{Ref => TimerRef}}).

%% Cancel and forget the liveness deadline timer for a single transfer.
%% cancel_timer does not remove an already-delivered message, so a stale deadline
%% message may still arrive; the model's token check discards it.
cancel_transfer_timer(Ref, #state{transfer_timers = Timers} = State) ->
    case maps:take(Ref, Timers) of
        {TimerRef, Timers1} ->
            _ = cancel_timer(TimerRef),
            State#state{transfer_timers = Timers1};
        error ->
            State
    end.

%% Byte size of an outstanding transfer (for the cumulative bytes-transferred
%% counter), read from the model before the step removes it.
transfer_byte_size(Ref, Tasks) ->
    case maps:find(Ref, rabbitmq_stream_s3_replica_reader_tasks:transfers(Tasks)) of
        {ok, {Size, _Token}} -> Size;
        error -> 0
    end.

%% Bytes covered by the in-flight persist (for the cumulative bytes-persisted
%% counter), read from the model before the step frees the slot.
persisting_snapshot(Tasks) ->
    case rabbitmq_stream_s3_replica_reader_tasks:persist_slot(Tasks) of
        {in_flight, _Gen, Bytes} -> Bytes;
        idle -> 0
    end.

%% Publish the pipeline gauges from the task model. They are pure functions of
%% the model's state rather than independently mutated counters, so they cannot
%% drift from the in-flight tasks.
derive_gauges(#state{tasks = Tasks} = State) ->
    set(
        State,
        ?C_TRANSFERS_IN_FLIGHT,
        rabbitmq_stream_s3_replica_reader_tasks:transfers_in_flight(Tasks)
    ),
    set(
        State,
        ?C_BYTES_IN_TRANSFER,
        rabbitmq_stream_s3_replica_reader_tasks:bytes_in_transfer(Tasks)
    ),
    set(
        State, ?C_BYTES_IN_PERSIST, rabbitmq_stream_s3_replica_reader_tasks:bytes_in_persist(Tasks)
    ),
    State.

%% Record a fragment as stalled on a non-transient error and republish the
%% stalled-offset gauge (the lowest stalled offset is the head-of-line stall).
mark_transfer_stalled(Ref, Offset, #state{stalled_transfers = Stalled} = State) ->
    State1 = State#state{stalled_transfers = Stalled#{Ref => Offset}},
    set_stalled_gauge(State1),
    State1.

%% Clear a fragment's stall once it is durable and republish the gauge. A Ref
%% that was never stalled (the common case) is a no-op.
clear_transfer_stalled(Ref, #state{stalled_transfers = Stalled} = State) ->
    case maps:is_key(Ref, Stalled) of
        false ->
            State;
        true ->
            State1 = State#state{stalled_transfers = maps:remove(Ref, Stalled)},
            set_stalled_gauge(State1),
            State1
    end.

%% The gauge is the lowest stalled offset (the fragment blocking the pipeline
%% head), or 0 when nothing is stalled.
set_stalled_gauge(#state{stalled_transfers = Stalled} = State) ->
    Offset =
        case maps:values(Stalled) of
            [] -> 0;
            Offsets -> lists:min(Offsets)
        end,
    set(State, ?C_UPLOAD_STALLED_OFFSET, Offset).

%% Carry out the transfer task model's decision. The transferred byte size is
%% read before the step for the cumulative counter on success.
apply_transfer_decisions(
    [{deliver, transfer_complete, {Ref, Uid}}], Size, #state{core = Core0} = State0
) ->
    State1 = cancel_transfer_timer(Ref, State0),
    inc(State1, ?C_TRANSFERS_COMPLETED, 1),
    inc(State1, ?C_BYTES_TRANSFERRED, Size),
    %% This fragment is durable; if it was being retried after a non-transient
    %% error, clear its stall (the gauge falls to the next stalled offset, or 0).
    State2 = clear_transfer_stalled(Ref, State1),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, Uid, Core0),
    {noreply, derive_gauges(execute_effects(Effects, State2#state{core = Core}))};
apply_transfer_decisions([{deliver, transfer_failed, {Ref, Reason}}], _Size, State0) ->
    handle_transfer_failure(Ref, Reason, State0);
apply_transfer_decisions([{drop, _Reason}], _Size, State) ->
    {noreply, State}.

%% A transfer failed (an error result, or its liveness deadline elapsed). Account
%% it off the pipeline and either recover (local retention trimmed past
%% next_offset, so no retry can make the range durable - issue #225) or drive the
%% core's retry path.
handle_transfer_failure(Ref, Reason, #state{core = Core0} = State0) ->
    maybe_log_transfer_deadline(Reason, State0),
    State1 = cancel_transfer_timer(Ref, State0),
    inc(State1, ?C_TRANSFERS_FAILED, 1),
    case local_log_ahead(State1) of
        {true, LocalFirst, NextOffset} ->
            {noreply,
                derive_gauges(handle_local_log_ahead(LocalFirst, NextOffset, Reason, State1))};
        false ->
            {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(
                Ref, Reason, Core0
            ),
            {noreply, derive_gauges(execute_effects(Effects, State1#state{core = Core}))}
    end.

maybe_log_transfer_deadline(transfer_deadline, #state{cfg = #cfg{stream = StreamId}}) ->
    ?LOG_WARNING(
        "~ts no result for an in-flight fragment transfer within the transfer "
        "deadline. The governor may have lost the submission (crash with a queued "
        "item), the upload task may have been killed before replying, or the "
        "result message was lost. Resubmitting to keep the upload pipeline live.",
        [StreamId]
    );
maybe_log_transfer_deadline(_Reason, _State) ->
    ok.

%% Carry out the retry task model's decision: re-submit if the transfer is still
%% outstanding, drop a retry left over from before a recovery.
apply_retry_decisions([{resubmit, Ref}], Dir, Meta, #state{cfg = #cfg{stream = StreamId}} = State) ->
    submit_upload(Ref, Dir, StreamId, Meta),
    {noreply, State};
apply_retry_decisions([{drop, _Reason}], _Dir, _Meta, State) ->
    {noreply, State}.

on_group_upload_completed(Kind, State) ->
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
    State.

%% Called from execute_effect/2 for {update_range, ...}, which the core
%% only emits after a successful manifest persist.
on_persist_completed(#manifest{} = Manifest, State) ->
    inc(State, ?C_PERSISTS_COMPLETED, 1),
    inc(State, ?C_ROOTS_CREATED, 1),
    set(State, ?C_LAST_PERSIST_TIMESTAMP_MS, erlang:system_time(millisecond)),
    %% The bytes_in_persist gauge is derived from the task model, which freed the
    %% persist slot when this persist completed; the cumulative bytes-persisted
    %% counter is bumped in apply_persist_decisions where the snapshot is known.
    update_manifest_gauges(Manifest, State),
    %% Persist succeeded. Now safe to delete objects that retention removed
    %% from the manifest. The manifest cache is updated, so readers will no
    %% longer reference these objects.
    %%
    %% Flushing all deferred deletions is intentional even when some refs
    %% belong to retention edits that arrived after this persist's snapshot
    %% and are still queued in `edits_since_persist`. Those objects are
    %% deleted from S3 here, but the persisted manifest does not reflect
    %% their removal until persist N+1 lands. If we crash in this window,
    %% on restart the persisted manifest will list fragments whose S3
    %% objects are gone. Readers hitting those fragments get 404s, which
    %% `rabbitmq_stream_s3_remote_reader` handles by refreshing the
    %% iterator past the missing offset. End-state is consistent: the
    %% next retention pass will re-emit the edit and a subsequent persist
    %% will reconcile the manifest.
    flush_deferred_deletions(State),
    State#state{deferred_deletions = []}.

flush_deferred_deletions(#state{deferred_deletions = []}) ->
    ok;
flush_deferred_deletions(#state{deferred_deletions = Refs, cfg = #cfg{stream = StreamId}}) ->
    Keys = [rabbitmq_stream_s3:ref_key(StreamId, Ref) || Ref <- Refs],
    rabbitmq_stream_s3_reaper:delete_objects(StreamId, Keys).

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

%% The snapshotted bytes of a failed persist are returned to the model's pending
%% pool by step/2, so the next persist covers them and bytes_in_persist is
%% unchanged. These helpers only bump the cumulative failure counters.
on_persist_failed(conflict, State0) ->
    inc(State0, ?C_PERSISTS_FAILED, 1),
    inc(State0, ?C_PERSIST_CONFLICTS, 1),
    State0;
on_persist_failed(_Reason, State0) ->
    inc(State0, ?C_PERSISTS_FAILED, 1),
    State0.

%% Reset all in-flight bookkeeping for a manifest-recovery restart, returning
%% the cleared state for the caller to re-resolve via resolve_and_start/1.
-spec reset_for_recovery(#state{}) -> #state{}.
reset_for_recovery(
    #state{transfer_timers = Timers0, persist_timer = PersistTimer, tasks = Tasks0} = State0
) ->
    set(State0, ?C_BYTES_IN_ASSEMBLY, 0),
    %% Recovery replaces the core, so no stalled fragment carries over; clear the
    %% stall set and its gauge.
    set(State0, ?C_UPLOAD_STALLED_OFFSET, 0),
    %% Cancel the per-transfer liveness deadline timers (a late deadline message
    %% is dropped by the model after recover clears the transfer map) and the
    %% persist tick (the core is about to be replaced).
    maps:foreach(fun(_Ref, TimerRef) -> erlang:cancel_timer(TimerRef) end, Timers0),
    _ = cancel_timer(PersistTimer),
    %% Recovery abandons these transfers: `recover` below clears the transfer
    %% map, so their results are dropped as stale. This reader stays alive, so
    %% the governor's requester monitor will not reap them either. Cancel them
    %% here, while their Refs are still known, rather than leaving uploads
    %% running whose results nothing will accept.
    rabbitmq_stream_s3_governor:cancel(
        maps:keys(rabbitmq_stream_s3_replica_reader_tasks:transfers(Tasks0))
    ),
    State1 = close_log(State0),
    %% Tear down every in-flight async task before recovery replaces the core: a
    %% task's result is an ordinary message that demonitor's flush does not
    %% remove, so it can still be queued. The model's recover bumps the generation
    %% and clears its slots, so any such late result carries the old generation
    %% and is dropped by step/2 rather than applied to the freshly resolved
    %% manifest (which would crash the core or splice in a stale edit).
    State2 = kill_task_io(State1),
    {Tasks, []} = rabbitmq_stream_s3_replica_reader_tasks:step(recover, Tasks0),
    derive_gauges(State2#state{
        tasks = Tasks,
        assembly = undefined,
        transfer_timers = #{},
        persist_timer = undefined,
        deferred_deletions = [],
        stalled_transfers = #{}
    }).

%% Demonitor (flushing the queued 'DOWN') and kill the task process if its pid
%% is known. A result message already in the mailbox is intentionally not
%% removed; the model's recovered generation makes the result handlers drop it.
kill_task(undefined, _Pid) ->
    ok;
kill_task(Mon, Pid) ->
    demonitor(Mon, [flush]),
    case Pid of
        undefined -> ok;
        _ -> exit(Pid, kill)
    end,
    ok.

%%----------------------------------------------------------------------------
%% Task-model I/O. The task model (#state.tasks) decides which family's result
%% to deliver or drop; these helpers carry out the matching I/O on the runtime
%% handles in #state.task_io, which the model itself cannot hold.
%%----------------------------------------------------------------------------

%% Find which single-in-flight family a 'DOWN' monitor ref belongs to.
-spec task_family_for_mon(reference(), #state{}) -> {ok, persist | group | retention} | error.
task_family_for_mon(Mon, #state{task_io = Io}) ->
    maps:fold(
        fun
            (Family, #{mon := M}, _Acc) when M =:= Mon -> {ok, Family};
            (_Family, _Handle, Acc) -> Acc
        end,
        error,
        Io
    ).

%% On normal completion of a single-in-flight task: demonitor (flushing the
%% queued 'DOWN') and cancel its timeout timer if any. The task has finished, so
%% its pid is not killed.
-spec clear_task_io(persist | group | retention, #state{}) -> #state{}.
clear_task_io(Family, #state{task_io = Io} = State) ->
    case maps:take(Family, Io) of
        {Handle, Io1} ->
            _ = demonitor(maps:get(mon, Handle), [flush]),
            _ = cancel_timer(maps:get(timer, Handle, undefined)),
            State#state{task_io = Io1};
        error ->
            State
    end.

%% Tear down one single-in-flight task that is still running (a timeout): kill
%% its process, demonitor (flush its 'DOWN'), and cancel its timer.
-spec discard_task_io(persist | group | retention, #state{}) -> #state{}.
discard_task_io(Family, #state{task_io = Io} = State) ->
    case maps:take(Family, Io) of
        {Handle, Io1} ->
            kill_task(maps:get(mon, Handle), maps:get(pid, Handle, undefined)),
            _ = cancel_timer(maps:get(timer, Handle, undefined)),
            State#state{task_io = Io1};
        error ->
            State
    end.

%% On recovery: tear down every single-in-flight task - demonitor (flush its
%% 'DOWN'), cancel its timer, and kill its process if the pid was retained.
-spec kill_task_io(#state{}) -> #state{}.
kill_task_io(#state{task_io = Io} = State) ->
    maps:foreach(
        fun(_Family, Handle) ->
            kill_task(maps:get(mon, Handle), maps:get(pid, Handle, undefined)),
            _ = cancel_timer(maps:get(timer, Handle, undefined))
        end,
        Io
    ),
    State#state{task_io = #{}}.

%% Carry out the persist task model's decision: a deliver runs the matching core
%% call, a drop ignores a stale result. PersistedBytes is the byte count the
%% commit covered, for the cumulative bytes-persisted counter on success.
apply_persist_decisions([{deliver, persist_complete, Revision}], PersistedBytes, State0) ->
    State1 = clear_task_io(persist, State0),
    inc(State1, ?C_BYTES_PERSISTED, PersistedBytes),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(
        Revision, State1#state.core
    ),
    {noreply, derive_gauges(execute_effects(Effects, State1#state{core = Core}))};
apply_persist_decisions([{deliver, persist_failed, Reason}], _PersistedBytes, State0) ->
    State1 = clear_task_io(persist, State0),
    State2 = on_persist_failed(Reason, State1),
    %% persist_failed/2 can return a stop effect, so route through maybe_stop/1.
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(
        Reason, State2#state.core
    ),
    maybe_stop(derive_gauges(execute_effects(Effects, State2#state{core = Core})));
apply_persist_decisions(
    [{drop, _Reason}], _PersistedBytes, #state{cfg = #cfg{stream = StreamId}} = State
) ->
    ?LOG_DEBUG("~ts ignoring stale persist result", [StreamId]),
    {noreply, State}.

%% Carry out the group-upload task model's decision. The kind is carried in the
%% completion so the per-kind "created" metric is attributed without re-reading
%% the slot the model just freed.
apply_group_decisions([{deliver, group_complete, {Kind, Uid}}], State0) ->
    State1 = clear_task_io(group, State0),
    State2 = on_group_upload_completed(Kind, State1),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(
        Uid, State2#state.core
    ),
    {noreply, execute_effects(Effects, State2#state{core = Core})};
apply_group_decisions(
    [{deliver, group_failed, Reason}], #state{cfg = #cfg{stream = StreamId}} = State0
) ->
    State1 = clear_task_io(group, State0),
    ?LOG_WARNING("~ts group upload failed: ~p", [StreamId, Reason]),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:group_upload_failed(
        Reason, State1#state.core
    ),
    {noreply, execute_effects(Effects, State1#state{core = Core})};
apply_group_decisions([{drop, _Reason}], #state{cfg = #cfg{stream = StreamId}} = State) ->
    ?LOG_DEBUG("~ts ignoring stale group upload result", [StreamId]),
    {noreply, State}.

%% Carry out the retention task model's decision for a retention *result*. The
%% task has finished, so its monitor is demonitored and its timeout timer
%% cancelled (clear_task_io); a timeout is handled separately below because it
%% must kill a still-running task. unchanged is a no-op evaluation and is not
%% counted as a failure.
apply_retention_decisions(
    [{deliver, retention_complete, {Edit, Refs}}], #state{cfg = #cfg{stream = StreamId}} = State0
) ->
    State1 = clear_task_io(retention, State0),
    {noreply, on_remote_retention(Edit, Refs, StreamId, State1#state.core, State1)};
apply_retention_decisions([{deliver, retention_failed, unchanged}], State0) ->
    State1 = clear_task_io(retention, State0),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(
        unchanged, State1#state.core
    ),
    {noreply, execute_effects(Effects, State1#state{core = Core})};
apply_retention_decisions([{deliver, retention_failed, Reason}], State0) ->
    State1 = clear_task_io(retention, State0),
    inc(State1, ?C_REMOTE_TIER_RETENTION_FAILURES, 1),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(
        Reason, State1#state.core
    ),
    {noreply, execute_effects(Effects, State1#state{core = Core})};
apply_retention_decisions([{drop, _Reason}], #state{cfg = #cfg{stream = StreamId}} = State) ->
    ?LOG_DEBUG("~ts ignoring stale retention result", [StreamId]),
    {noreply, State}.

%% Carry out the model's decision for a retention *timeout*. The task is still
%% running, so it is killed (discard_task_io) rather than merely demonitored.
apply_retention_timeout_decisions(
    [{deliver, retention_failed, timeout}], #state{cfg = #cfg{stream = StreamId}} = State0
) ->
    ?LOG_WARNING("~ts retention evaluation task timed out, killing", [StreamId]),
    State1 = discard_task_io(retention, State0),
    inc(State1, ?C_REMOTE_TIER_RETENTION_FAILURES, 1),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(
        timeout, State1#state.core
    ),
    {noreply, execute_effects(Effects, State1#state{core = Core})};
apply_retention_timeout_decisions([{drop, _Reason}], State) ->
    %% Stale timeout after normal completion or a recovery; ignore.
    {noreply, State}.

%% Translate a monitored task's crash into a failure result for the live task.
apply_task_crash(persist, Reason, #state{tasks = Tasks0, cfg = #cfg{stream = StreamId}} = State0) ->
    ?LOG_WARNING("~ts commit task crashed: ~p", [StreamId, Reason]),
    Gen = rabbitmq_stream_s3_replica_reader_tasks:generation(Tasks0),
    PersistedBytes = persisting_snapshot(Tasks0),
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {persist_result, Gen, {error, Reason}}, Tasks0
    ),
    apply_persist_decisions(Decisions, PersistedBytes, State0#state{tasks = Tasks});
apply_task_crash(group, Reason, #state{tasks = Tasks0, cfg = #cfg{stream = StreamId}} = State0) ->
    ?LOG_WARNING("~ts group upload task crashed: ~p", [StreamId, Reason]),
    Gen = rabbitmq_stream_s3_replica_reader_tasks:generation(Tasks0),
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {group_result, Gen, {error, Reason}}, Tasks0
    ),
    apply_group_decisions(Decisions, State0#state{tasks = Tasks});
apply_task_crash(retention, Reason, #state{tasks = Tasks0, cfg = #cfg{stream = StreamId}} = State0) ->
    ?LOG_WARNING("~ts retention evaluation task crashed: ~p", [StreamId, Reason]),
    Gen = rabbitmq_stream_s3_replica_reader_tasks:generation(Tasks0),
    %% A crash is reported as a failure (mirrors the {failed, _} result), so it is
    %% counted as a retention failure by apply_retention_decisions.
    {Tasks, Decisions} = rabbitmq_stream_s3_replica_reader_tasks:step(
        {retention_result, Gen, {failed, Reason}}, Tasks0
    ),
    apply_retention_decisions(Decisions, State0#state{tasks = Tasks}).

on_manifest_resolved(
    #manifest{next_offset = 0} = Manifest,
    #state{cfg = #cfg{stream = StreamId, epoch = Epoch}} = State
) ->
    inc(State, ?C_MANIFESTS_RESOLVED_EMPTY, 1),
    update_manifest_gauges(#manifest{}, State),
    %% Seed the cache row even when the resolved manifest is empty: the row
    %% replaces the pending marker written at init, telling readers the remote
    %% tier is resolved (and empty) so they stop failing closed and serve
    %% locally. Without this a brand-new stream would fail closed until its
    %% first persist.
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest, Epoch),
    State;
on_manifest_resolved(
    #manifest{first_offset = ManifestFirst, first_timestamp = ManifestFirstTs} = Manifest,
    #state{cfg = #cfg{stream = StreamId, epoch = Epoch, counter = Cnt}} = State
) ->
    inc(State, ?C_MANIFESTS_RESOLVED, 1),
    update_manifest_gauges(Manifest, State),
    %% Seed this node's manifest cache row from the just-resolved manifest,
    %% replacing the pending marker written at init, so consumers attaching at
    %% 'first' (or any offset below the local floor) can see the remote tier
    %% without waiting for a publish-driven persist or the reconciler's
    %% periodic reseed.
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest, Epoch),
    %% Seed the osiris first-offset and first-timestamp counters immediately so
    %% the management UI reflects the remote tier's range without waiting for the
    %% first retention evaluation or manifest edit. Without this, an idle stream
    %% (or one that hasn't persisted yet) reports only the local segment window.
    LocalFirst = counters:get(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET),
    counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET, min(LocalFirst, ManifestFirst)),
    LocalFirstTs = counters:get(Cnt, ?C_OSIRIS_LOG_FIRST_TIMESTAMP),
    counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_TIMESTAMP, min(LocalFirstTs, ManifestFirstTs)),
    %% Evaluate local retention once against the just-resolved manifest. Local
    %% retention is otherwise only triggered by a persist (which a new publish
    %% drives), so a stream that resolves a non-empty manifest and then stays
    %% idle - for example a replica that was offline for an upgrade, caught up
    %% from the writer, and whose stream then stopped publishing - would never
    %% reclaim the local segments already durable in the remote tier. This
    %% one-shot pass reclaims them. It only deletes segments fully below the
    %% manifest's next_offset, which is where the reader opens, so it cannot
    %% trim data the reader is about to read (issue #75).
    maybe_evaluate_retention(Manifest, State),
    State.

on_remote_retention_deleted(Refs, StreamId, State) ->
    Counts = lists:foldl(
        fun
            (#fragment_ref{}, Acc) ->
                maps:update_with(?C_FRAGMENTS_DELETED, fun(N) -> N + 1 end, 1, Acc);
            (#group_ref{kind = ?MANIFEST_KIND_GROUP} = GroupRef, Acc) ->
                rabbitmq_stream_s3_manifest:clear_group_cache(StreamId, GroupRef),
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

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% A fresh-random uid can collide with the immediately-prior commit's uid at the
%% same epoch, so the "old" manifest ref maps to the same S3 key as the manifest
%% just written. Deleting it would delete the live committed manifest, so the
%% colliding ref must be skipped; an absent or genuinely distinct ref behaves as
%% before (skip / delete respectively).
stale_manifest_ref_skips_self_collision_test() ->
    Ref = #manifest_ref{epoch = 7, uid = 42},
    ?assertEqual(skip, stale_manifest_ref(Ref, undefined)),
    ?assertEqual(skip, stale_manifest_ref(Ref, Ref)),
    ?assertEqual(
        {delete, #manifest_ref{epoch = 7, uid = 41}},
        stale_manifest_ref(Ref, #manifest_ref{epoch = 7, uid = 41})
    ),
    ?assertEqual(
        {delete, #manifest_ref{epoch = 6, uid = 42}},
        stale_manifest_ref(Ref, #manifest_ref{epoch = 6, uid = 42})
    ).

%% A transient metadata-store error must schedule a retry, not resolve empty.
classify_store_result_transient_error_retries_test() ->
    ?assertMatch(
        {retry, {metadata_unavailable, timeout}},
        classify_store_result({error, timeout}, <<"s">>)
    ),
    %% A caught exception (the `catch` returns a non-tuple or {'EXIT', _}).
    ?assertMatch(
        {retry, {metadata_unavailable, _}},
        classify_store_result({'EXIT', {badarg, []}}, <<"s">>)
    ).

%% A genuinely absent metadata node resolves to an empty manifest.
classify_store_result_not_found_is_empty_test() ->
    ?assertEqual({ok, #manifest{}}, classify_store_result({error, not_found}, <<"s">>)).

%% Before the manifest resolves (or while resolution retries after a store
%% outage) the core is undefined. The CLI retention commands must reply with an
%% error rather than crash the reader with a function_clause on the undefined
%% core.
evaluate_retention_on_unresolved_manifest_replies_error_test() ->
    State = #state{core = undefined},
    From = {self(), make_ref()},
    ?assertEqual(
        {reply, {error, manifest_not_resolved}, State},
        handle_call(evaluate_local_retention, From, State)
    ),
    ?assertEqual(
        {reply, {error, manifest_not_resolved}, State},
        handle_call(evaluate_remote_retention, From, State)
    ).

%% A call to a dead reader (noproc) must be converted to a named error, not let
%% the raw OTP exit escape the ok | {error, term()} contract of the admin and
%% retention entry points (which are reached over rabbit_misc:rpc_call from the
%% CLI).
safe_call_dead_reader_returns_reader_down_test() ->
    Dead = spawn(fun() -> ok end),
    %% Ensure the process has exited so the call sees noproc.
    Ref = monitor(process, Dead),
    receive
        {'DOWN', Ref, process, Dead, _} -> ok
    after 1000 -> error(process_did_not_exit)
    end,
    ?assertMatch({error, {reader_down, _}}, safe_call(Dead, status)).

%% A call that outlives its timeout must be converted to {error, timeout}. The
%% stub server never replies, so the short call timeout below fires.
safe_call_timeout_returns_timeout_test() ->
    Server = spawn(fun() ->
        receive
            never -> ok
        end
    end),
    try
        ?assertEqual({error, timeout}, safe_call(Server, status, 50))
    after
        exit(Server, kill)
    end.

%% A retry_transfer that fires after a reset cleared the in-flight transfer
%% tracking must be dropped, not re-submitted as a phantom upload (would
%% otherwise touch the governor against the discarded core).
retry_transfer_after_reset_is_dropped_test() ->
    %% Default task model has an empty transfer map, as after a recovery.
    State = #state{cfg = #cfg{stream = <<"s">>}},
    Ref = make_ref(),
    Meta = #{first_offset => 0, size => 0},
    ?assertEqual(
        {noreply, State},
        handle_info({retry_transfer, Ref, <<"/tmp">>, Meta}, State)
    ).

%% Recovery must tear down every in-flight async task's I/O handle: clear the
%% task_io map and kill the tasks whose pid is retained (a late result is dropped
%% by the recovered model generation rather than applied to the fresh core).
kill_task_io_tears_down_all_tasks_test() ->
    Idle = fun() ->
        receive
            stop -> ok
        end
    end,
    PersistPid = spawn(Idle),
    RetentionPid = spawn(Idle),
    GroupPid = spawn(Idle),
    State0 = #state{
        task_io = #{
            persist => #{mon => monitor(process, PersistPid), pid => PersistPid},
            group => #{mon => monitor(process, GroupPid)},
            retention => #{
                mon => monitor(process, RetentionPid),
                pid => RetentionPid,
                timer => erlang:send_after(60000, self(), {retention_timeout, 0})
            }
        }
    },
    State1 = kill_task_io(State0),
    %% All I/O handles are cleared.
    ?assertEqual(#{}, State1#state.task_io),
    %% Tasks whose pid is retained (persist, retention) are killed; the group
    %% task whose pid is not retained is left to finish on its own.
    ?assert(wait_dead(PersistPid, 100)),
    ?assert(wait_dead(RetentionPid, 100)),
    ?assert(is_process_alive(GroupPid)),
    GroupPid ! stop.

wait_dead(_Pid, 0) ->
    false;
wait_dead(Pid, N) ->
    case is_process_alive(Pid) of
        false ->
            true;
        true ->
            timer:sleep(5),
            wait_dead(Pid, N - 1)
    end.

%% A transient object-store error (not a 404) must schedule a retry: Khepri
%% references a manifest at this revision, so the tier is not empty.
classify_object_result_transient_error_retries_test() ->
    Key = <<"k">>,
    ?assertEqual(
        {retry, {manifest_object_fetch_failed, Key, slow_down}},
        classify_object_result({error, slow_down}, 7, Key)
    ).

%% A 404 on the manifest object means it has not been written yet: empty.
classify_object_result_not_found_is_empty_test() ->
    %% A 404 with no committed revision is a genuinely new stream: empty.
    ?assertEqual({ok, #manifest{}}, classify_object_result({error, not_found}, 0, <<"k">>)).

%% A 404 on a manifest object referenced by a committed revision (> 0) is a
%% stale local Khepri read pointing at a since-deleted object, not an empty
%% stream. Fail closed and retry rather than re-tiering over the real objects.
classify_object_result_not_found_with_revision_retries_test() ->
    ?assertEqual(
        {retry, {manifest_object_missing, <<"k">>}},
        classify_object_result({error, not_found}, 7, <<"k">>)
    ).

%% A fetched manifest object parses and carries the Khepri revision.
classify_object_result_ok_parses_with_revision_test() ->
    Data = ?MANIFEST(0, 0, 0, 0, 0, <<>>),
    {ok, Manifest} = classify_object_result({ok, Data}, 42, <<"k">>),
    ?assertEqual(42, Manifest#manifest.revision).

%% The cached manifest is trusted (and returned unchanged) only when its
%% revision already matches the committed Khepri revision.
classify_cache_result_current_revision_trusted_test() ->
    M = #manifest{next_offset = 50, revision = 7},
    ?assertEqual({ok, M}, classify_cache_result(M, {ok, #{revision => 7}})).

%% A cache behind the committed revision must be discarded: trusting it and
%% stamping the current revision would defeat the persist CAS and regress the
%% committed manifest.
classify_cache_result_stale_revision_resolves_from_store_test() ->
    M = #manifest{next_offset = 50, revision = 7},
    ?assertEqual(resolve_from_store, classify_cache_result(M, {ok, #{revision => 9}})).

%% A cache ahead of the committed revision is equally untrustworthy.
classify_cache_result_ahead_revision_resolves_from_store_test() ->
    M = #manifest{next_offset = 50, revision = 9},
    ?assertEqual(resolve_from_store, classify_cache_result(M, {ok, #{revision => 7}})).

%% If the metadata store cannot be read, the cache cannot be confirmed current,
%% so it must not be trusted.
classify_cache_result_metadata_unavailable_resolves_from_store_test() ->
    M = #manifest{next_offset = 50, revision = 7},
    ?assertEqual(resolve_from_store, classify_cache_result(M, {error, timeout})),
    ?assertEqual(resolve_from_store, classify_cache_result(M, {'EXIT', {badarg, []}})).

%% Build a minimal state whose live local log first offset and manifest
%% next_offset are set to the given values, for exercising local_log_ahead/1.
local_log_ahead_state(LocalFirst, NextOffset) ->
    Shared = osiris_log_shared:new(),
    ok = osiris_log_shared:set_first_chunk_id(Shared, LocalFirst),
    Opts = #{stream => <<"s">>, dir => <<"/tmp">>, epoch => 1, reference => <<"s">>},
    {Core, _} = rabbitmq_stream_s3_replica_reader_core:init(
        #manifest{next_offset = NextOffset}, Opts
    ),
    #state{cfg = #cfg{stream = <<"s">>, shared = Shared}, core = Core, config = Opts}.

%% The local log trimmed past next_offset: the stalled segment is permanently
%% gone, so recovery must fire.
local_log_ahead_true_when_local_past_next_test() ->
    ?assertMatch(
        {true, 100, 10}, local_log_ahead(local_log_ahead_state(100, 10))
    ).

%% The local log first offset equals next_offset: the segment is still present
%% (or the failure is a transient roll). Must not recover; retry instead.
local_log_ahead_false_when_equal_test() ->
    ?assertEqual(false, local_log_ahead(local_log_ahead_state(10, 10))).

%% The manifest is ahead of the local log (normal steady state). Must not
%% recover.
local_log_ahead_false_when_local_behind_test() ->
    ?assertEqual(false, local_log_ahead(local_log_ahead_state(5, 10))).

%% A transfer result for a reference the shell no longer tracks (e.g. a
%% transfer abandoned by a manifest recovery) must be dropped without touching
%% the core, which would otherwise crash get_meta on the unknown reference or
%% append a non-contiguous orphan. The state is returned unchanged.
stale_transfer_result_dropped_test() ->
    %% Default task model has an empty transfer map.
    State = #state{},
    Ref = make_ref(),
    ?assertEqual({noreply, State}, handle_info({transfer_result, Ref, {ok, 1}}, State)),
    ?assertEqual(
        {noreply, State}, handle_info({transfer_result, Ref, {error, boom}}, State)
    ).

%% close_log/1 is a no-op when there is no open data reader, so the restart
%% paths can call it unconditionally.
close_log_without_open_log_is_noop_test() ->
    State = #state{log = undefined},
    ?assertEqual(State, close_log(State)).

%% The reset installs an empty manifest carrying f = n = the local floor, so an
%% empty manifest satisfies Coverage and Accounting and first_offset only moves
%% forward (Reset safety invariant).
reset_manifest_carries_floor_as_first_and_next_test() ->
    LocalFirst = 149324677,
    M = reset_manifest(LocalFirst, 7),
    ?assertEqual(LocalFirst, M#manifest.first_offset),
    ?assertEqual(LocalFirst, M#manifest.next_offset),
    ?assertEqual(M#manifest.first_offset, M#manifest.next_offset),
    ?assertEqual(<<>>, M#manifest.entries),
    ?assertEqual(7, M#manifest.revision).

%% The transient retry profile retries the first attempt immediately and backs
%% off (within the configured ceiling, jitter included) on later attempts. The
%% non-transient profile starts at its base delay even on the first attempt.
retry_delay_profiles_test() ->
    %% Use defaults: transient base 10ms, max 5000ms; non-transient base 1000ms,
    %% max 30000ms; exponent 2.
    ?assertEqual(0, transient_retry_delay(1)),
    %% Attempt 2 uses the base (10ms) before jitter; equal jitter keeps it in
    %% [base/2, base].
    D2 = transient_retry_delay(2),
    ?assert(D2 >= 5 andalso D2 =< 10),
    %% A very high attempt is capped at the transient ceiling (5000ms), jitter
    %% keeping it within [max/2, max].
    DHigh = transient_retry_delay(100),
    ?assert(DHigh >= 2500 andalso DHigh =< 5000),
    %% Non-transient starts at its base (1000ms) on attempt 1, never immediate.
    DN1 = nontransient_retry_delay(1),
    ?assert(DN1 >= 500 andalso DN1 =< 1000),
    DNHigh = nontransient_retry_delay(100),
    ?assert(DNHigh >= 15000 andalso DNHigh =< 30000).

%% The stalled-offset gauge reflects the lowest stalled offset (head-of-line),
%% so an out-of-order completion of a later fragment does not prematurely clear
%% it; only draining the head does.
stalled_offset_gauge_tracks_head_of_line_test() ->
    {ok, _} = application:ensure_all_started(seshat),
    _ = seshat:new_group(rabbitmq_stream_s3),
    Cnt = seshat:new(rabbitmq_stream_s3, {?MODULE, test_stalled}, ?STREAM_COUNTERS, #{}),
    State0 = #state{cfg = #cfg{stream = <<"s">>}, metrics = Cnt},
    Gauge = fun() -> counters:get(Cnt, ?C_UPLOAD_STALLED_OFFSET) end,
    ?assertEqual(0, Gauge()),
    RefA = make_ref(),
    RefB = make_ref(),
    %% Two fragments stall; the gauge tracks the lower offset.
    State1 = mark_transfer_stalled(RefA, 100, State0),
    ?assertEqual(100, Gauge()),
    State2 = mark_transfer_stalled(RefB, 200, State1),
    ?assertEqual(100, Gauge()),
    %% The later fragment (B) becoming durable does not clear the head stall.
    State3 = clear_transfer_stalled(RefB, State2),
    ?assertEqual(100, Gauge()),
    %% Only when the head (A) drains does the gauge fall to 0.
    _State4 = clear_transfer_stalled(RefA, State3),
    ?assertEqual(0, Gauge()),
    seshat:delete(rabbitmq_stream_s3, {?MODULE, test_stalled}).

-endif.
