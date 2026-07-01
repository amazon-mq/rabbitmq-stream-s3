%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_replica_reader_core).
-moduledoc """
Pure functional core for the remote replica reader.

Takes events + state, returns `{NewState, [Effect]}`. No processes,
no I/O, no process dictionary, no message passing. Independently
testable without mocks or timing.
""".

-include("include/rabbitmq_stream_s3.hrl").

-export([
    init/2,
    manifest/1,
    persisted_manifest/1,
    pending_prefix_rewrite/1,
    format_state/1,
    fragment_cut/2,
    anchor_write_complete/1,
    anchor_write_failed/1,
    transfer_complete/3,
    transfer_failed/3,
    group_upload_complete/2,
    group_upload_failed/2,
    persist_complete/2,
    persist_failed/2,
    tick/2,
    await_offset/3,
    retention_started/1,
    retention_complete/2,
    retention_failed/2
]).

-export_type([state/0, cfg/0, core_effect/0]).

-type fragment_meta() :: rabbitmq_stream_s3_fragment_assembly:fragment_meta().

-record(cfg, {
    stream :: stream_id(),
    dir :: directory(),
    epoch :: non_neg_integer(),
    reference :: term(),
    persist_threshold :: non_neg_integer(),
    persist_interval_ms :: non_neg_integer(),
    rebalance_threshold :: non_neg_integer()
}).

-record(state, {
    cfg :: #cfg{},
    manifest :: #manifest{},
    %% Ordered queue of in-flight transfers (cut order).
    in_flight :: queue:queue({reference(), fragment_meta()}),
    %% Completions that arrived out of order.
    pending_completions :: #{reference() => {rabbitmq_stream_s3:uid(), fragment_meta()}},
    %% Number of edits applied since last durable persist.
    since_persist :: non_neg_integer(),
    %% Number of edits included in the current in-flight persist.
    in_persist_count = 0 :: non_neg_integer(),
    %% Timestamp (milliseconds) of last durable persist.
    last_persist_ts :: integer(),
    %% Whether a durable persist is currently in flight.
    persist_in_flight :: boolean(),
    %% Manifest state at last successful durable persist.
    last_persisted_manifest :: #manifest{},
    %% Manifest being persisted (set when persist starts, used on completion).
    persisting_manifest :: #manifest{} | undefined,
    %% Edits computed at persist start time, used for broadcast on completion.
    persisting_edits = [] :: [#edit{}],
    %% Callers blocked on await_offset.
    waiters :: [{osiris:offset(), gen_server:from()}],
    %% Whether a group upload is currently in flight.
    rebalance_in_flight = false :: boolean(),
    %% Whether a retention evaluation is currently in flight (async group download).
    retention_in_flight = false :: boolean(),
    %% All edits (appends, rebalance, retention) since last persist, in order.
    edits_since_persist = [] :: [#edit{}],
    %% Consecutive failed upload attempts per in-flight transfer, keyed by Ref.
    %% Incremented on each transfer_failed and used to drive the resubmit
    %% backoff; cleared when the transfer is drained (success) or on reinit.
    transfer_attempts = #{} :: #{reference() => pos_integer()},
    %% Whether the per-stream anchor (written before the first remote-tier
    %% fragment) has been committed. Fragment uploads are held until it is `done`,
    %% so no S3 object can exist under a prefix whose anchor is absent:
    %%   pending  - no fragment cut yet, anchor not requested
    %%   writing  - first fragment cut, write_anchor emitted, uploads held
    %%   done     - anchor committed, uploads released and flowing normally
    anchor = pending :: pending | writing | done
}).

-opaque state() :: #state{}.
-type cfg() :: #cfg{}.

-type core_effect() ::
    {write_anchor, stream_id(), term()}
    | {submit_transfer, reference(), stream_id(), directory(), fragment_meta()}
    | {start_persist, #manifest{}, non_neg_integer(), term(), rabbitmq_stream_s3_db:revision(), [
        #edit{}
    ]}
    | {upload_group, stream_id(), rabbitmq_stream_s3:kind(), binary(), non_neg_integer(),
        non_neg_integer()}
    | {start_retention_eval, stream_id(), directory()}
    | {update_range, osiris:offset(), osiris:offset()}
    | {broadcast, stream_id(), [#edit{}]}
    | {evaluate_retention, stream_id(), directory()}
    | {reply_waiters, [{gen_server:from(), ok}]}
    | {start_persist_timer, non_neg_integer()}
    | cancel_persist_timer
    | {resubmit_transfer, reference(), stream_id(), directory(), fragment_meta(),
        Attempt :: pos_integer()}
    | {resubmit_transfer_delayed, reference(), stream_id(), directory(), fragment_meta(),
        Reason :: term(), Attempt :: pos_integer()}
    | reinitialize
    %% Stop the reader: the stream's metadata node was deleted, so there is
    %% nothing left to persist to.
    | stop.

%% ------------------------------------------------------------------
%% API
%% ------------------------------------------------------------------

-spec init(#manifest{}, map()) -> {state(), [core_effect()]}.
init(Manifest, Opts) ->
    Cfg = #cfg{
        stream = maps:get(stream, Opts),
        dir = maps:get(dir, Opts),
        epoch = maps:get(epoch, Opts),
        reference = maps:get(reference, Opts),
        persist_threshold = maps:get(
            persist_threshold,
            Opts,
            rabbitmq_stream_s3_config:persist_threshold()
        ),
        persist_interval_ms = maps:get(
            persist_interval_ms,
            Opts,
            rabbitmq_stream_s3_config:persist_interval_ms()
        ),
        rebalance_threshold = maps:get(
            rebalance_threshold,
            Opts,
            rabbitmq_stream_s3_config:rebalance_threshold()
        )
    },
    State = #state{
        cfg = Cfg,
        manifest = Manifest,
        in_flight = queue:new(),
        pending_completions = #{},
        since_persist = 0,
        last_persist_ts = erlang:system_time(millisecond),
        persist_in_flight = false,
        last_persisted_manifest = Manifest,
        persisting_manifest = undefined,
        waiters = [],
        %% A non-empty resolved manifest witnesses that a fragment was already
        %% tiered, so the anchor already exists (it precedes the first fragment):
        %% start `done` and skip re-writing it, e.g. on a reader restart. Only a
        %% brand-new stream (empty manifest) starts `pending` and writes the anchor
        %% on its genuine first fragment. Tests may override.
        anchor = maps:get(anchor, Opts, initial_anchor(Manifest))
    },
    {State, []}.

%% The anchor already exists iff the stream has already tiered a fragment, which a
%% non-empty manifest witnesses. A re-write would be idempotent but adds a Khepri
%% round-trip to the restart path, so skip it when the manifest already has data.
initial_anchor(#manifest{entries = <<>>}) -> pending;
initial_anchor(#manifest{}) -> done.

-spec manifest(state()) -> #manifest{}.
manifest(#state{manifest = Manifest}) ->
    Manifest.

-spec persisted_manifest(state()) -> #manifest{}.
persisted_manifest(#state{last_persisted_manifest = Manifest}) ->
    Manifest.

-doc """
Which manifest-prefix rewrite, if any, is in flight: a `rebalance` (factoring
leading entries into a group object) or a remote `retention` evaluation
(truncating leading entries). Both rewrite the manifest's leading entries, so a
remote-retention trigger must stand down while either runs: evaluating against a
prefix that is about to change races the in-flight writer (Single mutator), and
a second retention task in particular would capture the same pre-retention
snapshot, recompute the identical prefix-truncation edit, and apply it twice,
deleting live entries and double-counting total_size. Returns the specific
blocker so the caller can report which one is in progress.
""".
-spec pending_prefix_rewrite(state()) -> none | rebalance | retention.
pending_prefix_rewrite(#state{rebalance_in_flight = true}) -> rebalance;
pending_prefix_rewrite(#state{retention_in_flight = true}) -> retention;
pending_prefix_rewrite(#state{}) -> none.

-spec format_state(state()) -> map().
format_state(#state{
    manifest = #manifest{first_offset = FirstOff, next_offset = NextOff, total_size = TotalSize},
    last_persisted_manifest = #manifest{next_offset = PersistedNextOff},
    in_flight = Q,
    pending_completions = PC,
    since_persist = SincePersist,
    in_persist_count = InPersistCount,
    last_persist_ts = LastPersistTs,
    persist_in_flight = PersistInFlight,
    rebalance_in_flight = RebalanceInFlight,
    retention_in_flight = RetentionInFlight,
    waiters = Waiters,
    anchor = Anchor
}) ->
    #{
        anchor => Anchor,
        manifest_first_offset => FirstOff,
        manifest_next_offset => NextOff,
        manifest_total_size => TotalSize,
        persisted_next_offset => PersistedNextOff,
        transfers_in_flight => queue:len(Q),
        transfers_pending_order => maps:size(PC),
        since_persist => SincePersist,
        in_persist_count => InPersistCount,
        persist_in_flight => PersistInFlight,
        %% Age rather than the raw timestamp: last_persist_ts is on this node's
        %% clock, but the CLI renders on a different node, so the duration must
        %% be computed here against the same clock.
        last_persist_age_ms => erlang:system_time(millisecond) - LastPersistTs,
        rebalance_in_flight => RebalanceInFlight,
        retention_in_flight => RetentionInFlight,
        waiters => length(Waiters)
    }.

-spec fragment_cut(fragment_meta(), state()) -> {state(), reference(), [core_effect()]}.
fragment_cut(Meta, #state{anchor = done} = State) ->
    fragment_cut_submit(Meta, State);
fragment_cut(Meta, #state{anchor = AnchorStatus, cfg = Cfg} = State0) ->
    %% The anchor must commit before the first fragment is uploaded. Track the
    %% fragment in in_flight as usual, but hold its submit_transfer: it is
    %% re-derived from in_flight in anchor_write_complete/1 once the anchor is
    %% durable. On the very first fragment also emit the write_anchor effect.
    {State1, Ref, Effects0} = fragment_cut_submit(Meta, State0),
    Held = [E || E <- Effects0, not is_submit_transfer(E)],
    AnchorEffect =
        case AnchorStatus of
            pending -> [{write_anchor, Cfg#cfg.stream, Cfg#cfg.reference}];
            writing -> []
        end,
    {State1#state{anchor = writing}, Ref, AnchorEffect ++ Held}.

fragment_cut_submit(Meta, #state{cfg = Cfg, in_flight = Q, since_persist = 0} = State) ->
    case queue:is_empty(Q) of
        true ->
            %% First fragment since last persist. Start the timer.
            Ref = make_ref(),
            Q1 = queue:in({Ref, Meta}, Q),
            Effects = [
                {submit_transfer, Ref, Cfg#cfg.stream, Cfg#cfg.dir, Meta},
                {start_persist_timer, Cfg#cfg.persist_interval_ms}
            ],
            {State#state{in_flight = Q1}, Ref, Effects};
        false ->
            do_fragment_cut(Meta, State)
    end;
fragment_cut_submit(Meta, State) ->
    do_fragment_cut(Meta, State).

is_submit_transfer({submit_transfer, _Ref, _Stream, _Dir, _Meta}) -> true;
is_submit_transfer(_) -> false.

-doc """
The per-stream anchor has been durably written. Release the fragment uploads that
were held while it was being written: each is in in_flight (in cut order) but was
never submitted. Subsequent fragments are submitted as they are cut.
""".
-spec anchor_write_complete(state()) -> {state(), [core_effect()]}.
anchor_write_complete(#state{anchor = writing, cfg = Cfg, in_flight = Q} = State) ->
    Effects = [
        {submit_transfer, Ref, Cfg#cfg.stream, Cfg#cfg.dir, Meta}
     || {Ref, Meta} <- queue:to_list(Q)
    ],
    {State#state{anchor = done}, Effects};
anchor_write_complete(State) ->
    {State, []}.

-doc """
The per-stream anchor write failed. Re-emit the write_anchor effect so the shell
retries it; uploads stay held until the anchor commits.
""".
-spec anchor_write_failed(state()) -> {state(), [core_effect()]}.
anchor_write_failed(#state{anchor = writing, cfg = Cfg} = State) ->
    {State, [{write_anchor, Cfg#cfg.stream, Cfg#cfg.reference}]};
anchor_write_failed(State) ->
    {State, []}.

do_fragment_cut(Meta, #state{cfg = Cfg, in_flight = Q} = State) ->
    Ref = make_ref(),
    Q1 = queue:in({Ref, Meta}, Q),
    Effects = [{submit_transfer, Ref, Cfg#cfg.stream, Cfg#cfg.dir, Meta}],
    {State#state{in_flight = Q1}, Ref, Effects}.

-spec transfer_complete(reference(), rabbitmq_stream_s3:uid(), state()) ->
    {state(), [core_effect()]}.
transfer_complete(Ref, Uid, #state{pending_completions = PC} = State0) ->
    State1 = State0#state{pending_completions = PC#{Ref => {Uid, get_meta(Ref, State0)}}},
    drain_completions(State1).

-spec transfer_failed(reference(), term(), state()) -> {state(), [core_effect()]}.
transfer_failed(Ref, Reason, #state{cfg = Cfg, transfer_attempts = Attempts} = State0) ->
    Meta = get_meta(Ref, State0),
    %% Count this failure as the Nth consecutive attempt for the fragment. The
    %% count survives resubmits (Ref is stable) and drives the backoff; it is
    %% cleared when the fragment is drained on success, or on reinit.
    Attempt = maps:get(Ref, Attempts, 0) + 1,
    State = State0#state{transfer_attempts = Attempts#{Ref => Attempt}},
    case is_retriable(Reason) of
        true ->
            %% Transient error. Retry with a (jittered) backoff applied by the
            %% shell from the attempt count.
            {State, [{resubmit_transfer, Ref, Cfg#cfg.stream, Cfg#cfg.dir, Meta, Attempt}]};
        false ->
            %% A confirmed fragment must never be abandoned. Dropping it would
            %% let drain_completions advance next_offset over the subsequent
            %% (already durable) fragments, past a range that is not durable in
            %% S3. The manifest would then claim that range as tiered, the
            %% local-tier cleanup (which keys off next_offset) would free the
            %% only remaining copy, and the result is a silent, permanent hole
            %% in the middle of the stream (see issue #206). Instead, keep the
            %% fragment at its place in the queue and retry it with a backoff
            %% delay, stalling the pipeline until it is durable.
            {State, [
                {resubmit_transfer_delayed, Ref, Cfg#cfg.stream, Cfg#cfg.dir, Meta, Reason, Attempt}
            ]}
    end.

-spec persist_complete(rabbitmq_stream_s3_db:revision(), state()) -> {state(), [core_effect()]}.
persist_complete(
    Revision,
    #state{
        cfg = #cfg{persist_interval_ms = PersistInterval} = Cfg,
        persisting_manifest = CommittingManifest,
        in_persist_count = Committed,
        since_persist = N
    } = State0
) ->
    Manifest = CommittingManifest#manifest{revision = Revision},
    %% edits_since_persist is stored reversed (newest first). The first
    %% (N - Committed) elements are edits that arrived *after* start_persist
    %% captured its snapshot; those must be preserved.
    Remaining = N - Committed,
    RemainingEdits = lists:sublist(State0#state.edits_since_persist, Remaining),
    State1 = State0#state{
        persist_in_flight = false,
        last_persisted_manifest = Manifest,
        persisting_manifest = undefined,
        since_persist = N - Committed,
        in_persist_count = 0,
        last_persist_ts = erlang:system_time(millisecond),
        edits_since_persist = RemainingEdits
    },
    %% Don't evaluate retention while a rebalance is in flight. Retention
    %% could delete entries that the rebalance is factoring into a group,
    %% causing group_upload_complete to crash. Retention will be triggered
    %% by the next persist_complete after the rebalance finishes.
    RetentionEffects =
        case State0#state.rebalance_in_flight of
            true -> [];
            false -> [{evaluate_retention, Cfg#cfg.stream, Cfg#cfg.dir}]
        end,
    Effects0 =
        [
            {update_range, Manifest#manifest.first_offset, Manifest#manifest.next_offset},
            {broadcast, Cfg#cfg.stream, State0#state.persisting_edits}
            | RetentionEffects
        ] ++ [cancel_persist_timer],
    {State2, WaiterEffects} = notify_waiters(State1),
    Effects1 = Effects0 ++ WaiterEffects,
    %% If more edits applied while persist was in flight, trigger another.
    case State2#state.since_persist > 0 of
        true ->
            {State3, CommitEffects} = maybe_start_persist(State2),
            %% If the remainder is below the persist threshold, maybe_start_persist
            %% produces no start_persist. We emitted cancel_persist_timer above, so
            %% without re-arming here the remainder would sit unpersisted until the
            %% next publish: await_offset waiters block, the range table stalls, and
            %% local retention cannot reclaim those segments. Re-arm so tick/1 flushes
            %% it. Mirrors the same guard in drain_completions/1.
            TimerEffects =
                case CommitEffects of
                    [] ->
                        [{start_persist_timer, PersistInterval}];
                    _ ->
                        []
                end,
            {State3, Effects1 ++ CommitEffects ++ TimerEffects};
        false ->
            {State2, Effects1}
    end.

-spec persist_failed(term(), state()) -> {state(), [core_effect()]}.
persist_failed(conflict, State) ->
    %% Khepri conflict. The shell must re-resolve the manifest externally
    %% and re-init the core. We signal this by returning a special effect.
    {State#state{persist_in_flight = false}, [reinitialize]};
persist_failed(not_found, #state{last_persisted_manifest = #manifest{revision = Rev}} = State) when
    Rev > 0
->
    %% not_found with a non-zero expected revision means the stream's metadata
    %% node was deleted out from under this persist (the queue was removed; the
    %% node is removed by Khepri's keep-while condition). The stream is gone, so
    %% the reader has nothing left to do: stop. Retrying would re-PUT an orphan
    %% manifest forever, and reinitializing could resurrect the deleted stream
    %% (an empty manifest resolves to revision 0, whose create-if-absent put
    %% would re-create the node). Rev is the ExpectedRevision the failed put
    %% used: start_persist takes it from last_persisted_manifest.revision, and a
    %% non-zero value can only come from a prior successful upload.
    {State#state{persist_in_flight = false}, [stop]};
persist_failed(not_found, State) ->
    %% Revision 0 is a first-ever persist, which uses a create-if-absent
    %% condition and cannot legitimately return not_found. If we somehow reach
    %% here, do not stop a possibly-live new stream and do not fall through to
    %% the retry-forever path below: reinitialize once to re-resolve.
    {State#state{persist_in_flight = false}, [reinitialize]};
persist_failed(_Reason, #state{cfg = Cfg} = State0) ->
    %% S3 or transient error. Retry the persist.
    State1 = State0#state{persist_in_flight = false},
    {State2, Effects} = maybe_start_persist(State1),
    case Effects of
        [] ->
            %% Fallback: use timer to retry.
            {State2, [{start_persist_timer, Cfg#cfg.persist_interval_ms}]};
        _ ->
            {State2, Effects}
    end.

-spec tick(integer(), state()) -> {state(), [core_effect()]}.
tick(
    Now,
    #state{
        since_persist = SinceCommit,
        persist_in_flight = false,
        rebalance_in_flight = false,
        retention_in_flight = false,
        last_persist_ts = LastTs,
        cfg = #cfg{persist_interval_ms = Interval}
    } = State
) when SinceCommit > 0, (Now - LastTs) >= Interval ->
    start_persist(State);
tick(_Now, State) ->
    {State, []}.

-spec await_offset(osiris:offset(), gen_server:from(), state()) ->
    {state(), [core_effect()]}.
await_offset(Offset, From, #state{last_persisted_manifest = Committed} = State) ->
    case Committed#manifest.next_offset >= Offset of
        true ->
            {State, [{reply_waiters, [{From, ok}]}]};
        false ->
            {State#state{waiters = [{Offset, From} | State#state.waiters]}, []}
    end.

-doc """
A group upload completed. Apply the rebalance edit to the in-memory manifest,
then check for recursive rebalancing (groups→kilo-group, etc.). Once no more
rebalancing is needed, trigger persist if warranted.
""".
-spec group_upload_complete(rabbitmq_stream_s3:uid(), state()) -> {state(), [core_effect()]}.
group_upload_complete(
    Uid, #state{cfg = Cfg, manifest = Manifest0} = State0
) ->
    %% The pending rebalance info is encoded in the effect that was emitted.
    %% We reconstruct the edit from the current manifest state: the leading
    %% entries of the kind being factored are the ones to replace.
    {Kind, Pos, Len} = pending_rebalance(Manifest0, Cfg#cfg.rebalance_threshold),
    %% Build the group entry. First offset and timestamps come from the
    %% first entry being factored out.
    <<FirstOffset:64/unsigned, FirstTs:64/signed, _:64/signed, _:8, _:40, _:32, _/binary>> =
        binary:part(Manifest0#manifest.entries, Pos, ?ENTRY_B),
    %% Last timestamp comes from the last entry being factored out.
    <<_:64, _:64/signed, LastTs:64/signed, _:8, _:40, _:32, _/binary>> =
        binary:part(Manifest0#manifest.entries, Pos + Len - ?ENTRY_B, ?ENTRY_B),
    GroupKind = Kind + 1,
    GroupEntry = ?ENTRY(FirstOffset, FirstTs, LastTs, GroupKind, 0, Uid),
    Edit = #edit{
        first_offset = Manifest0#manifest.first_offset,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = undefined,
        size = 0,
        entries = GroupEntry,
        pos = Pos,
        len = Len
    },
    Manifest = rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest0),
    State1 = State0#state{
        manifest = Manifest,
        rebalance_in_flight = false,
        since_persist = State0#state.since_persist + 1,
        edits_since_persist = [Edit | State0#state.edits_since_persist]
    },
    %% Check for recursive rebalancing (e.g. too many groups → kilo-group).
    {State2, RebalanceEffects} = maybe_start_rebalance(State1),
    {State3, PersistEffects} = maybe_start_persist(State2),
    %% Re-arm the persist timer if this rebalance edit left the manifest below
    %% the persist threshold and no persist/rebalance fired. Otherwise the edit
    %% sits unpersisted until the next publish. Same guard as
    %% drain_completions/1.
    TimerEffects = rearm_persist_timer_if_pending(PersistEffects, State3),
    {State3, RebalanceEffects ++ PersistEffects ++ TimerEffects}.

-doc """
A group upload failed. On retriable errors, re-emit the upload effect.
On fatal errors, abandon this rebalance attempt (the root stays oversized).
The next drain_completions cycle will re-detect the threshold and try again.
""".
-spec group_upload_failed(term(), state()) -> {state(), [core_effect()]}.
group_upload_failed(Reason, #state{cfg = Cfg, manifest = Manifest} = State) ->
    case is_retriable(Reason) of
        true ->
            %% Re-emit the upload_group effect with the same parameters.
            {Kind, Pos, Len} = pending_rebalance(Manifest, Cfg#cfg.rebalance_threshold),
            Entries = binary:part(Manifest#manifest.entries, Pos, Len),
            GroupKind = Kind + 1,
            {State, [{upload_group, Cfg#cfg.stream, GroupKind, Entries, Pos, Len}]};
        false ->
            %% Abandon. Clear rebalance_in_flight so persist can proceed.
            %% The threshold will be re-detected on the next drain cycle.
            State1 = State#state{rebalance_in_flight = false},
            {State2, PersistEffects} = maybe_start_persist(State1),
            {State2, PersistEffects}
    end.

-doc """
Mark that an async retention evaluation has been spawned.
""".
-spec retention_started(state()) -> state().
retention_started(State) ->
    State#state{retention_in_flight = true}.

-doc """
Apply a remote retention edit to the manifest. The edit is tracked in
edits_since_persist and will be broadcast on the next persist_complete.
""".
-spec retention_complete(#edit{}, state()) -> {state(), [core_effect()]}.
retention_complete(Edit, #state{manifest = Manifest0} = State0) ->
    Manifest = rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest0),
    State1 = State0#state{
        manifest = Manifest,
        retention_in_flight = false,
        since_persist = State0#state.since_persist + 1,
        edits_since_persist = [Edit | State0#state.edits_since_persist]
    },
    %% Retention is now clear, so a rebalance deferred while it was in flight
    %% can proceed. Re-check rebalance before persist (the same order as
    %% drain_completions and group_upload_complete); the retention edit just
    %% shrank the root, so this usually finds nothing to do.
    {State2, RebalanceEffects} = maybe_start_rebalance(State1),
    {State3, PersistEffects} = maybe_start_persist(State2),
    %% Re-arm the persist timer if the retention edit left the manifest below
    %% the persist threshold and no persist/rebalance fired. Otherwise the edit
    %% sits unpersisted until the next publish. Same guard as
    %% drain_completions/1.
    TimerEffects = rearm_persist_timer_if_pending(PersistEffects, State3),
    {State3, RebalanceEffects ++ PersistEffects ++ TimerEffects}.

-doc """
A retention evaluation finished without producing an edit: it either failed
(group download crashed or timed out) or found nothing to remove. Either way,
clear the in-flight flag so the next persist_complete can re-trigger
evaluation. Failures are counted by the shell; the core does not distinguish
them here because both outcomes mean the same thing to the core.
""".
-spec retention_failed(term(), state()) -> {state(), [core_effect()]}.
retention_failed(_Reason, State0) ->
    %% Retention cleared without changing the manifest. Mirror the wrap-up of
    %% retention_complete/2 (minus the edit): re-check a rebalance deferred while
    %% retention was in flight, then re-evaluate persist and re-arm the persist
    %% timer.
    %%
    %% The timer re-arm is the important part. While retention_in_flight was
    %% true, both maybe_start_persist and tick/2 were suppressed. A fragment that
    %% completed during that window armed a one-shot persist timer, but the tick
    %% that timer eventually drives is a no-op (still in flight) that consumes the
    %% one-shot without rescheduling. If we do not re-arm here, that pending edit
    %% is stranded unpersisted until the next publish: on an idle low-throughput
    %% stream await_offset waiters block and local retention cannot reclaim the
    %% segments. Every other flag-clearing path already re-arms; this was the one
    %% exit that did not.
    State1 = State0#state{retention_in_flight = false},
    {State2, RebalanceEffects} = maybe_start_rebalance(State1),
    {State3, PersistEffects} = maybe_start_persist(State2),
    TimerEffects = rearm_persist_timer_if_pending(PersistEffects, State3),
    {State3, RebalanceEffects ++ PersistEffects ++ TimerEffects}.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

-spec drain_completions(state()) -> {state(), [core_effect()]}.
drain_completions(#state{in_flight = Q, pending_completions = PC} = State0) ->
    case queue:peek(Q) of
        {value, {Ref, _Meta}} ->
            case maps:get(Ref, PC, undefined) of
                undefined ->
                    %% Front not yet complete. Stop draining.
                    {State0, []};
                {Uid, Meta} ->
                    State1 = apply_fragment(Uid, Meta, State0),
                    State2 = State1#state{
                        in_flight = queue:drop(State1#state.in_flight),
                        pending_completions = maps:remove(Ref, State1#state.pending_completions),
                        %% The fragment is durable; forget its retry history so a
                        %% later transfer reusing this Ref starts at attempt 1.
                        transfer_attempts = maps:remove(Ref, State1#state.transfer_attempts)
                    },
                    %% Continue draining contiguous completions.
                    {State3, Effects} = drain_completions(State2),
                    %% After all draining, check rebalance then persist trigger.
                    {State4, RebalanceEffects} = maybe_start_rebalance(State3),
                    {State5, CommitEffects} = maybe_start_persist(State4),
                    %% If persist didn't fire but fragments are pending, ensure
                    %% a timer is running so the tick will flush them. Without
                    %% this, bytes can sit in the persist stage indefinitely
                    %% when publishing stops and the threshold is not met.
                    TimerEffects = rearm_persist_timer_if_pending(CommitEffects, State5),
                    {State5, Effects ++ RebalanceEffects ++ CommitEffects ++ TimerEffects}
            end;
        empty ->
            {State0, []}
    end.

-spec apply_fragment(rabbitmq_stream_s3:uid(), fragment_meta(), state()) -> state().
apply_fragment(
    Uid,
    Meta,
    #state{manifest = Manifest0, since_persist = N, edits_since_persist = Edits} = State
) ->
    #{
        first_offset := FirstOffset,
        first_timestamp := FirstTs,
        last_timestamp := LastTs,
        next_offset := NextOffset,
        size := Size
    } = Meta,
    Entry = ?ENTRY(FirstOffset, FirstTs, LastTs, ?MANIFEST_KIND_FRAGMENT, Size, Uid),
    Edit = #edit{
        first_offset = Manifest0#manifest.first_offset,
        first_timestamp = Manifest0#manifest.first_timestamp,
        first_last_timestamp = Manifest0#manifest.first_last_timestamp,
        next_offset = NextOffset,
        size = Size,
        entries = Entry,
        pos = byte_size(Manifest0#manifest.entries),
        len = 0
    },
    Edit1 =
        case Manifest0#manifest.entries of
            <<>> ->
                %% Empty manifest (fresh stream or fully retained). The first
                %% fragment sets the manifest's position metadata.
                Edit#edit{
                    first_offset = FirstOffset,
                    first_timestamp = FirstTs,
                    first_last_timestamp = LastTs
                };
            _ ->
                %% Belt-and-suspenders guard for the invariant that protects
                %% next_offset from advancing past a non-durable range.
                assert_contiguous(Manifest0#manifest.next_offset, FirstOffset),
                Edit
        end,
    Manifest = rabbitmq_stream_s3_manifest:apply_edit(Edit1, Manifest0),
    State#state{
        manifest = Manifest,
        since_persist = N + 1,
        edits_since_persist = [Edit1 | Edits]
    }.

%% Arm the persist timer when edits are pending (since_persist > 0) but no
%% persist started, so an idle stream still flushes them via tick/1. Every
%% edit-producing path needs this guard: drain_completions/1 (fragment
%% appends), group_upload_complete/2 (rebalance edits), and retention_complete/2
%% (retention edits). Without it, a completion that leaves the manifest below
%% persist_threshold strands the edit unpersisted until the next publish, which
%% on a low-throughput stream may never come (the uploaded fragments are then
%% not durably referenced). The start_persist_timer effect replaces any running
%% timer, so re-arming is idempotent.
-spec rearm_persist_timer_if_pending([core_effect()], state()) -> [core_effect()].
rearm_persist_timer_if_pending(PersistEffects, #state{since_persist = N, cfg = Cfg}) ->
    case PersistEffects of
        [] when N > 0 -> [{start_persist_timer, Cfg#cfg.persist_interval_ms}];
        _ -> []
    end.

-spec maybe_start_persist(state()) -> {state(), [core_effect()]}.
maybe_start_persist(#state{persist_in_flight = true} = State) ->
    {State, []};
maybe_start_persist(#state{rebalance_in_flight = true} = State) ->
    {State, []};
maybe_start_persist(#state{retention_in_flight = true} = State) ->
    {State, []};
maybe_start_persist(#state{since_persist = 0} = State) ->
    {State, []};
maybe_start_persist(
    #state{
        cfg = Cfg,
        since_persist = N,
        persist_in_flight = false
    } = State
) when N >= Cfg#cfg.persist_threshold ->
    start_persist(State);
maybe_start_persist(State) ->
    {State, []}.

-spec start_persist(state()) -> {state(), [core_effect()]}.
start_persist(
    #state{
        cfg = Cfg,
        manifest = Manifest,
        last_persisted_manifest = LastManifest,
        since_persist = N,
        edits_since_persist = EditsRev
    } =
        State
) ->
    %% edits_since_persist is stored in reverse (newest first) for O(1) prepend.
    %% Reverse to temporal order for broadcast and persist.
    Edits = lists:reverse(EditsRev),
    Effect =
        {start_persist, Manifest, Cfg#cfg.epoch, Cfg#cfg.reference, LastManifest#manifest.revision,
            Edits},
    {
        State#state{
            persist_in_flight = true,
            in_persist_count = N,
            persisting_manifest = Manifest,
            persisting_edits = Edits
        },
        [Effect]
    }.

%% Check if the manifest root needs rebalancing. If the leading entries
%% exceed the threshold for any kind, emit an upload_group effect.
-spec maybe_start_rebalance(state()) -> {state(), [core_effect()]}.
maybe_start_rebalance(#state{rebalance_in_flight = true} = State) ->
    {State, []};
maybe_start_rebalance(#state{retention_in_flight = true} = State) ->
    %% A remote retention evaluation is in flight. It rewrites the same
    %% leading entries a rebalance would factor out, so the two must never run
    %% concurrently (Single mutator). Defer: the rebalance is re-checked when
    %% retention completes (retention_complete) and on the next drain.
    {State, []};
maybe_start_rebalance(#state{cfg = Cfg, manifest = Manifest} = State) ->
    case needs_rebalance(Manifest#manifest.entries, Cfg#cfg.rebalance_threshold) of
        false ->
            {State, []};
        {Kind, Pos, Len} ->
            %% Factor out the entries into a group of the next-higher kind.
            Entries = binary:part(Manifest#manifest.entries, Pos, Len),
            GroupKind = Kind + 1,
            Effect = {upload_group, Cfg#cfg.stream, GroupKind, Entries, Pos, Len},
            {State#state{rebalance_in_flight = true}, [Effect]}
    end.

%% Determine if the root has >= threshold entries of the same kind at any
%% position. Scans from the beginning, counting contiguous runs of each kind.
%% Returns {Kind, Pos, Len} where Pos is byte offset and Len is byte length
%% of the entries to factor out, or false.
-spec needs_rebalance(binary(), non_neg_integer()) ->
    {rabbitmq_stream_s3:kind(), non_neg_integer(), non_neg_integer()} | false.
needs_rebalance(Entries, Threshold) ->
    count_leading_kind(Entries, 0, Threshold).

count_leading_kind(<<>>, _Pos, _Threshold) ->
    false;
count_leading_kind(Entries, Pos, Threshold) ->
    <<_:64, _:64/signed, _:64/signed, Kind:8, _:40, _:32, _/binary>> =
        binary:part(Entries, Pos, ?ENTRY_B),
    Count = count_kind(Entries, Pos, Kind, 0),
    case Count >= Threshold of
        true ->
            {Kind, Pos, Threshold * ?ENTRY_B};
        false ->
            %% Skip past this run and check the next kind.
            NextPos = Pos + Count * ?ENTRY_B,
            case NextPos >= byte_size(Entries) of
                true -> false;
                false -> count_leading_kind(Entries, NextPos, Threshold)
            end
    end.

count_kind(Entries, Pos, Kind, Count) ->
    case Pos + ?ENTRY_B =< byte_size(Entries) of
        false ->
            Count;
        true ->
            <<_:64, _:64/signed, _:64/signed, K:8, _:40, _:32, _/binary>> =
                binary:part(Entries, Pos, ?ENTRY_B),
            case K of
                Kind -> count_kind(Entries, Pos + ?ENTRY_B, Kind, Count + 1);
                _ -> Count
            end
    end.

%% Find the pending rebalance position in the manifest. This is called from
%% group_upload_complete to reconstruct what was being rebalanced.
-spec pending_rebalance(#manifest{}, non_neg_integer()) ->
    {rabbitmq_stream_s3:kind(), non_neg_integer(), non_neg_integer()}.
pending_rebalance(#manifest{entries = Entries}, Threshold) ->
    %% The rebalance that was in flight must still be detectable.
    {Kind, Pos, Len} = needs_rebalance(Entries, Threshold),
    {Kind, Pos, Len}.

-spec notify_waiters(state()) -> {state(), [core_effect()]}.
notify_waiters(#state{last_persisted_manifest = Committed, waiters = Waiters} = State) ->
    NextOffset = Committed#manifest.next_offset,
    {Satisfied, Remaining} = lists:partition(
        fun({Offset, _From}) -> NextOffset >= Offset end,
        Waiters
    ),
    case Satisfied of
        [] ->
            {State#state{waiters = Remaining}, []};
        _ ->
            Replies = [{From, ok} || {_Offset, From} <- Satisfied],
            {State#state{waiters = Remaining}, [{reply_waiters, Replies}]}
    end.

-spec get_meta(reference(), state()) -> fragment_meta().
get_meta(Ref, #state{in_flight = Q}) ->
    get_meta_from_queue(Ref, queue:to_list(Q)).

get_meta_from_queue(Ref, [{Ref, Meta} | _]) -> Meta;
get_meta_from_queue(Ref, [_ | Rest]) -> get_meta_from_queue(Ref, Rest).

%% A fragment must begin exactly where the manifest currently ends.
%% Appending a non-contiguous fragment would advance next_offset past a
%% range that was never made durable in S3, producing a silent, permanent
%% hole in the stream (issue #206). Fail loudly instead: on restart the
%% reader re-resolves the durable manifest rather than recording the gap.
-spec assert_contiguous(osiris:offset(), osiris:offset()) -> ok.
assert_contiguous(NextOffset, NextOffset) ->
    ok;
assert_contiguous(NextOffset, FragmentFirstOffset) ->
    erlang:error(
        {non_contiguous_fragment, #{
            manifest_next_offset => NextOffset,
            fragment_first_offset => FragmentFirstOffset
        }}
    ).

-spec is_retriable(term()) -> boolean().
%% Transient conditions worth retrying without operator intervention. These are
%% the actual error shapes the S3 API layer produces (see
%% rabbitmq_stream_s3_api_aws): a 500 is reported as `internal_error`, a 503 as
%% `slow_down`, a dropped or oversaturated connection as `connection_error`, and
%% any other non-success status as `#{status => _}`. The earlier `{http, 500}`
%% / `{http, 503}` clauses matched terms that no code ever constructs, so every
%% real transient fell through to the fatal answer. The transfer paths that
%% consult this never abandon a confirmed fragment (the fatal branch retries
%% with a backoff), so a wrong answer only changes retry timing; keep the set to
%% genuinely transient conditions and let everything else fall through.
is_retriable(timeout) ->
    true;
%% A reader-side transfer deadline: the governor never reported a result for a
%% submitted transfer (lost message, externally killed task, or a queued
%% submission dropped by a governor restart). The fragment was never made
%% durable, so retrying immediately under the same reference is safe and
%% correct. See rabbitmq_stream_s3_config:transfer_deadline_ms/0 and the
%% transfer_deadline handler in rabbitmq_stream_s3_replica_reader.
is_retriable(transfer_deadline) ->
    true;
is_retriable(connection_error) ->
    true;
is_retriable(slow_down) ->
    true;
is_retriable(internal_error) ->
    true;
is_retriable(#{status := Status}) ->
    %% 408 request timeout, 429 throttling, and any 5xx are server-side
    %% transients; everything else (e.g. 403, 404) is fatal.
    Status =:= 408 orelse Status =:= 429 orelse (Status >= 500 andalso Status =< 599);
is_retriable(_) ->
    false.
