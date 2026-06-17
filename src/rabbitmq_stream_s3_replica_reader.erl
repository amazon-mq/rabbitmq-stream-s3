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
%% Counters above; gauges below. The macro ?NODE_COUNTERS filters by
%% type, and seshat requires counter indexes to be 1..N sequential. Add
%% new counters before this divider and renumber the gauges that follow.
-define(C_TRANSFERS_IN_FLIGHT, 24).
-define(C_LAST_PERSIST_TIMESTAMP_MS, 25).
-define(C_MANIFEST_FIRST_OFFSET, 26).
-define(C_MANIFEST_NEXT_OFFSET, 27).
-define(C_MANIFEST_FIRST_TIMESTAMP_MS, 28).
-define(C_REMOTE_BYTES, 29).
-define(C_REMOTE_MESSAGES, 30).
-define(C_BYTES_IN_ASSEMBLY, 31).
-define(C_BYTES_IN_TRANSFER, 32).
-define(C_BYTES_IN_PERSIST, 33).

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
    %% PID of the in-flight retention evaluation task.
    retention_pid :: pid() | undefined,
    %% Timer ref for the retention task timeout.
    retention_timer :: reference() | undefined,
    %% Kind of the group upload currently in flight. Cleared when the
    %% group_upload_result message arrives. Only one rebalance is in
    %% flight at a time so a single slot suffices.
    pending_group_kind :: rabbitmq_stream_s3:kind() | undefined,
    %% Keys queued for deletion after the next successful persist.
    %% Retention computes which objects to delete but we defer the actual
    %% S3 DELETE until the manifest persist that records their removal
    %% completes. This eliminates the race where a reader sees a stale
    %% manifest pointing to already-deleted objects (issue #166).
    deferred_deletions = [] :: [#fragment_ref{} | #group_ref{}],
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
        Result -> {ok, Result}
    end.

-doc "Return the formatted status by vhost and queue name.".
-spec status(rabbit_types:vhost(), binary()) -> {ok, map()} | {error, term()}.
status(VHost, QueueName) ->
    case call(VHost, QueueName, status) of
        {error, _} = Err -> Err;
        Result -> {ok, Result}
    end.

-doc "Trigger local retention evaluation for a stream on the current node.".
-spec evaluate_local_retention(stream_id()) -> ok | {error, term()}.
evaluate_local_retention(StreamId) ->
    call(StreamId, evaluate_local_retention).

-doc "Trigger local retention evaluation by vhost and queue name.".
-spec evaluate_local_retention(rabbit_types:vhost(), binary()) -> ok | {error, term()}.
evaluate_local_retention(VHost, QueueName) ->
    call(VHost, QueueName, evaluate_local_retention).

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
handle_call(status, _From, State) ->
    {reply, format_state(State), State};
handle_call(evaluate_local_retention, _From, #state{core = Core} = State) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    maybe_evaluate_retention(Manifest, State),
    {reply, ok, State};
handle_call(
    evaluate_remote_retention,
    _From,
    #state{core = Core, retention = Retention, cfg = #cfg{stream = StreamId}} = State
) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    State1 = maybe_evaluate_remote_retention(Manifest, Retention, StreamId, State),
    {reply, ok, State1};
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
handle_cast(
    {retention_updated, Retention}, #state{core = Core, cfg = #cfg{stream = StreamId}} = State
) ->
    UserRetention = [S || S <- Retention, element(1, S) =/= 'fun'],
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(Core),
    State1 = State#state{retention = UserRetention},
    {noreply, maybe_evaluate_remote_retention(Manifest, UserRetention, StreamId, State1)};
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

handle_continue(resolve_manifest, State0) ->
    {noreply, resolve_and_start(State0)}.

handle_info({osiris_offset, _Ref, _Offset}, State0) ->
    State = drain(State0),
    {noreply, State};
handle_info(retry_resolve, State0) ->
    {noreply, resolve_and_start(State0)};
handle_info({transfer_result, Ref, _Result}, State0) when
    not is_map_key(Ref, State0#state.transfer_sizes)
->
    %% Stale result from a transfer submitted before a manifest recovery reset
    %% the in-flight queue (see handle_local_log_ahead/3). The fragment is no
    %% longer tracked by the shell or the core, so feeding it to the core would
    %% crash get_meta or, for a success, append a non-contiguous orphan that
    %% trips assert_contiguous. Drop it.
    {noreply, State0};
handle_info({transfer_result, Ref, {ok, Uid}}, #state{core = Core0} = State0) ->
    State1 = on_transfer_result(Ref, ok, State0),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, Uid, Core0),
    State2 = State1#state{core = Core},
    State3 = execute_effects(Effects, State2),
    {noreply, State3};
handle_info({transfer_result, Ref, {error, Reason}}, #state{core = Core0} = State0) ->
    State1 = on_transfer_result(Ref, {error, Reason}, State0),
    case local_log_ahead(State1) of
        {true, LocalFirst, NextOffset} ->
            %% Local retention trimmed past the manifest's next_offset, so the
            %% segment backing the stalled fragment is permanently gone and no
            %% retry can ever make it durable. Without this the core would keep
            %% the fragment at the head of the in-flight queue and retry it
            %% forever (issue #225), wedging the manifest and making the bulk of
            %% the stream inaccessible. Recover the same way start_reading0/1
            %% does at reader init: discard the remote manifest and restart from
            %% the local log's first offset, accepting the lost range.
            {noreply, handle_local_log_ahead(LocalFirst, NextOffset, Reason, State1)};
        false ->
            {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(
                Ref, Reason, Core0
            ),
            {noreply, execute_effects(Effects, State1#state{core = Core})}
    end;
handle_info({retry_transfer, Ref, Dir, Meta}, #state{cfg = #cfg{stream = StreamId}} = State) ->
    %% A previously failed fragment upload is being retried after its backoff
    %% delay. The in-flight gauges were already restored when the delayed
    %% resubmit effect was executed, so only re-submit the upload here.
    submit_upload(Ref, Dir, StreamId, Meta),
    {noreply, State};
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
handle_info(
    {retention_result, unchanged},
    #state{core = Core0, retention_mon = Mon, retention_timer = TRef} = State0
) ->
    %% Evaluation ran and found nothing to remove: not a failure, not counted.
    demonitor(Mon, [flush]),
    _ = cancel_timer(TRef),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(unchanged, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core,
            retention_mon = undefined,
            retention_pid = undefined,
            retention_timer = undefined
        })};
handle_info(
    {retention_result, {failed, Reason}},
    #state{core = Core0, retention_mon = Mon, retention_timer = TRef} = State0
) ->
    %% The async retention task caught a crash and reported it as a failure
    %% (distinct from unchanged, which means it ran and found nothing). The
    %% task already logged the crash with a stack trace; record the failure
    %% as a metric so it is visible separately from no-op evaluations.
    demonitor(Mon, [flush]),
    _ = cancel_timer(TRef),
    inc(State0, ?C_REMOTE_TIER_RETENTION_FAILURES, 1),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(Reason, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core,
            retention_mon = undefined,
            retention_pid = undefined,
            retention_timer = undefined
        })};
handle_info(
    {retention_result, {Edit, Refs}},
    #state{core = Core0, retention_mon = Mon, retention_timer = TRef, cfg = #cfg{stream = StreamId}} =
        State0
) ->
    demonitor(Mon, [flush]),
    _ = cancel_timer(TRef),
    State = on_remote_retention(Edit, Refs, StreamId, Core0, State0#state{
        retention_mon = undefined, retention_pid = undefined, retention_timer = undefined
    }),
    {noreply, State};
handle_info(
    retention_timeout,
    #state{retention_mon = Mon, retention_pid = Pid, core = Core0, cfg = #cfg{stream = StreamId}} =
        State0
) when Mon =/= undefined ->
    ?LOG_WARNING("~ts retention evaluation task timed out, killing", [StreamId]),
    exit(Pid, kill),
    demonitor(Mon, [flush]),
    inc(State0, ?C_REMOTE_TIER_RETENTION_FAILURES, 1),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(timeout, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core,
            retention_mon = undefined,
            retention_pid = undefined,
            retention_timer = undefined
        })};
handle_info(retention_timeout, State) ->
    %% Stale timeout after normal completion; ignore.
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
    maybe_stop(
        execute_effects(Effects, State1#state{
            core = Core, persist_mon = undefined, persist_pid = undefined
        })
    );
handle_info(
    {'DOWN', Mon, process, _, Reason},
    #state{persist_mon = Mon, core = Core0, cfg = #cfg{stream = StreamId}} = State0
) ->
    ?LOG_WARNING("~ts commit task crashed: ~p", [StreamId, Reason]),
    State1 = on_persist_failed(Reason, State0),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(Reason, Core0),
    %% Route through maybe_stop/1 like the {persist_result, {error, _}} handler:
    %% persist_failed/2 can return a stop effect, and a crashed persist task
    %% must honour it rather than leaving the reader marked stopping but alive.
    maybe_stop(
        execute_effects(Effects, State1#state{
            core = Core, persist_mon = undefined, persist_pid = undefined
        })
    );
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
    #state{retention_mon = Mon, retention_timer = TRef, core = Core0, cfg = #cfg{stream = StreamId}} =
        State0
) ->
    ?LOG_WARNING("~ts retention evaluation task crashed: ~p", [StreamId, Reason]),
    _ = cancel_timer(TRef),
    inc(State0, ?C_REMOTE_TIER_RETENTION_FAILURES, 1),
    {Core, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(Reason, Core0),
    {noreply,
        execute_effects(Effects, State0#state{
            core = Core,
            retention_mon = undefined,
            retention_pid = undefined,
            retention_timer = undefined
        })};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(
    _Reason,
    #state{
        cfg = #cfg{stream = StreamId},
        persist_mon = Mon,
        persist_pid = CommitPid,
        metrics_id = MetricsId
    } = State
) ->
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
    %% Close the open data reader so its segment file descriptors are released
    %% promptly rather than at process teardown.
    _ = close_log(State),
    ok = delete_metrics(MetricsId),
    rabbitmq_stream_s3_registry:unregister_name({StreamId, node()}),
    rabbitmq_stream_s3_manifest:evict_group_cache(StreamId),
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

call(StreamId, Msg) ->
    case rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}) of
        undefined -> {error, {not_found, StreamId}};
        Pid -> gen_server:call(Pid, Msg)
    end.

call(VHost, QueueName, Msg) ->
    QName = rabbit_misc:r(VHost, queue, QueueName),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            #{name := StreamId} = amqqueue:get_type_state(Q),
            call(iolist_to_binary(StreamId), Msg);
        {error, not_found} ->
            {error, {not_found, QueueName}}
    end.

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
        Keys = [rabbitmq_stream_s3:ref_key(StreamId, Ref) || Ref <- Refs],
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
    Key = rabbitmq_stream_s3:ref_key(StreamId, Ref),
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
    submit_upload(Ref, Dir, StreamId, Meta),
    on_transfer_submitted(Ref, maps:get(size, Meta), State0);
execute_effect({resubmit_transfer, Ref, _StreamId, Dir, Meta}, #state{cfg = Cfg} = State0) ->
    StreamId = Cfg#cfg.stream,
    submit_upload(Ref, Dir, StreamId, Meta),
    %% on_transfer_result already decremented the gauges and removed Ref
    %% from transfer_sizes. Restore them so the eventual completion is
    %% accounted for correctly.
    on_transfer_submitted(Ref, maps:get(size, Meta), State0);
execute_effect(
    {resubmit_transfer_delayed, Ref, _StreamId, Dir, Meta, Reason}, #state{cfg = Cfg} = State0
) ->
    StreamId = Cfg#cfg.stream,
    Delay = rabbitmq_stream_s3_config:upload_retry_delay_ms(),
    ?LOG_WARNING(
        "~ts fragment upload at offset ~b failed with non-transient error ~p; "
        "retrying in ~bms. The upload pipeline and local-tier cleanup are "
        "stalled at this offset until the fragment is durable in S3.",
        [StreamId, maps:get(first_offset, Meta), Reason, Delay]
    ),
    erlang:send_after(Delay, self(), {retry_transfer, Ref, Dir, Meta}),
    %% on_transfer_result already decremented the gauges. Restore them: the
    %% fragment is still pending and its bytes are still in the pipeline.
    on_transfer_submitted(Ref, maps:get(size, Meta), State0);
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
    State1 = (close_log(State0))#state{
        assembly = undefined,
        transfer_sizes = #{},
        persist_pending_bytes = 0,
        persisting_bytes = 0,
        %% Discard deferred deletions: the retention edit was not persisted,
        %% so the objects must remain. The new writer will re-evaluate
        %% retention and delete them after its own persist.
        deferred_deletions = []
    },
    resolve_and_start(State1);
execute_effect(stop, State) ->
    %% The stream's metadata node was deleted (the queue was removed). Mark the
    %% reader for shutdown; the handler that ran this effect returns
    %% {stop, normal} via maybe_stop/1.
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
    case rabbitmq_stream_s3_replica_reader_core:rebalance_in_flight(Core0) of
        true ->
            %% A rebalance is rewriting the manifest's leading entries. Remote
            %% retention rewrites the same prefix, so evaluating it now would
            %% compute an edit against a manifest the rebalance is about to
            %% change, and applying it would race the rebalance over that
            %% prefix (Single mutator). Skip; retention is re-evaluated by the
            %% next persist_complete once the rebalance has finished. This
            %% guards the manual CLI trigger; the automatic post-persist path
            %% is already gated by persist_complete.
            ?LOG_DEBUG(
                "~ts skipping remote retention evaluation: rebalance in flight",
                [StreamId]
            ),
            State;
        false ->
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
            end
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
        Self ! {retention_result, Result}
    end),
    Core = rabbitmq_stream_s3_replica_reader_core:retention_started(State#state.core),
    Timeout = rabbitmq_stream_s3_config:retention_task_timeout(),
    TRef = erlang:send_after(Timeout, Self, retention_timeout),
    State#state{core = Core, retention_mon = MonRef, retention_pid = Pid, retention_timer = TRef};
maybe_spawn_group_retention(_Manifest, _Retention, _Now, _StreamId, State) ->
    State.

%% ------------------------------------------------------------------
%% Reading
%% ------------------------------------------------------------------

-spec resolve_manifest(stream_id()) -> {ok, #manifest{}} | {retry, term()}.
resolve_manifest(StreamId) ->
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{} = M ->
            %% Cache hit: a real manifest. Refresh its revision from Khepri if
            %% we can; a stale revision is caught by the persist CAS, so a
            %% failed refresh just keeps the cached one.
            case catch rabbitmq_stream_s3_db:get(StreamId) of
                {ok, #{revision := Rev}} -> {ok, M#manifest{revision = Rev}};
                _ -> {ok, M}
            end;
        undefined ->
            resolve_manifest_from_store(StreamId)
    end.

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
classify_object_result({error, not_found}, _Rev, _Key) ->
    %% The metadata node exists but no manifest object does yet: a stream that
    %% has not persisted a manifest. Empty is correct.
    {ok, #manifest{}};
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
            {Core, _Effects} = rabbitmq_stream_s3_replica_reader_core:init(
                Manifest, State1#state.config
            ),
            start_reading(State1#state{core = Core});
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
            start_reading((close_log(State1))#state{core = Core, assembly = undefined})
    end.

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
    resolve_and_start((close_log(State0))#state{
        assembly = undefined,
        transfer_sizes = #{},
        persist_pending_bytes = 0,
        persisting_bytes = 0,
        deferred_deletions = []
    }).

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

%% Submit a fragment upload to the governor. Shared by the initial submit
%% and the immediate/delayed retry paths.
submit_upload(Ref, Dir, StreamId, Meta) ->
    Self = self(),
    Size = maps:get(size, Meta),
    Fun = fun() -> upload_fragment(Dir, StreamId, Meta) end,
    rabbitmq_stream_s3_governor:submit(Fun, Size, Self, Ref).

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
    State#state{persisting_bytes = 0, deferred_deletions = []}.

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
    ?assertEqual({ok, #manifest{}}, classify_object_result({error, not_found}, 7, <<"k">>)).

%% A fetched manifest object parses and carries the Khepri revision.
classify_object_result_ok_parses_with_revision_test() ->
    Data = ?MANIFEST(0, 0, 0, 0, 0, <<>>),
    {ok, Manifest} = classify_object_result({ok, Data}, 42, <<"k">>),
    ?assertEqual(42, Manifest#manifest.revision).

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
    State = #state{transfer_sizes = #{}},
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

-endif.
