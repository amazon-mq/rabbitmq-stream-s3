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
    fragment_cut/2,
    transfer_complete/3,
    transfer_failed/3,
    persist_complete/2,
    persist_failed/2,
    tick/2,
    await_offset/3,
    apply_retention_edit/2
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
    %% Fragments applied since last durable commit.
    since_persist :: non_neg_integer(),
    %% Number of fragments included in the current in-flight commit.
    in_persist_count = 0 :: non_neg_integer(),
    %% Timestamp (milliseconds) of last durable commit.
    last_persist_ts :: integer(),
    %% Whether a durable commit is currently in flight.
    persist_in_flight :: boolean(),
    %% Manifest state at last successful durable commit.
    last_persisted_manifest :: #manifest{},
    %% Manifest being committed (set when commit starts, used on completion).
    persisting_manifest :: #manifest{} | undefined,
    %% Callers blocked on await_offset.
    waiters :: [{osiris:offset(), gen_server:from()}]
}).

-opaque state() :: #state{}.
-type cfg() :: #cfg{}.

-type core_effect() ::
    {submit_transfer, reference(), stream_id(), directory(), fragment_meta()}
    | {start_persist, #manifest{}, non_neg_integer(), term(), rabbitmq_stream_s3_db:revision(), [
        #edit{}
    ]}
    | {update_range, osiris:offset(), osiris:offset()}
    | {broadcast, stream_id(), [#edit{}]}
    | {evaluate_retention, stream_id(), directory()}
    | {reply_waiters, [{gen_server:from(), ok}]}
    | {start_persist_timer, non_neg_integer()}
    | cancel_persist_timer
    | {resubmit_transfer, reference(), stream_id(), directory(), fragment_meta()}
    | reinitialize.

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
            application:get_env(rabbitmq_stream_s3, persist_threshold, 5)
        ),
        persist_interval_ms = maps:get(
            persist_interval_ms,
            Opts,
            application:get_env(rabbitmq_stream_s3, persist_interval_ms, 2000)
        ),
        rebalance_threshold = maps:get(rebalance_threshold, Opts, 1024)
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
        waiters = []
    },
    {State, []}.

-spec manifest(state()) -> #manifest{}.
manifest(#state{manifest = Manifest}) ->
    Manifest.

-spec persisted_manifest(state()) -> #manifest{}.
persisted_manifest(#state{last_persisted_manifest = Manifest}) ->
    Manifest.

-spec fragment_cut(fragment_meta(), state()) -> {state(), reference(), [core_effect()]}.
fragment_cut(Meta, #state{cfg = Cfg, in_flight = Q, since_persist = 0} = State) ->
    case queue:is_empty(Q) of
        true ->
            %% First fragment since last commit. Start the timer.
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
fragment_cut(Meta, State) ->
    do_fragment_cut(Meta, State).

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
transfer_failed(Ref, Reason, #state{cfg = Cfg} = State) ->
    case is_retriable(Reason) of
        true ->
            Meta = get_meta(Ref, State),
            {State, [{resubmit_transfer, Ref, Cfg#cfg.stream, Cfg#cfg.dir, Meta}]};
        false ->
            %% Fatal: remove from queue, accept gap, drain subsequent.
            State1 = remove_in_flight(Ref, State),
            drain_completions(State1)
    end.

-spec persist_complete(rabbitmq_stream_s3_db:revision(), state()) -> {state(), [core_effect()]}.
persist_complete(
    Revision,
    #state{
        cfg = Cfg,
        persisting_manifest = CommittingManifest,
        in_persist_count = Committed,
        since_persist = N
    } = State0
) ->
    Manifest = CommittingManifest#manifest{revision = Revision},
    State1 = State0#state{
        persist_in_flight = false,
        last_persisted_manifest = Manifest,
        persisting_manifest = undefined,
        since_persist = N - Committed,
        in_persist_count = 0,
        last_persist_ts = erlang:system_time(millisecond)
    },
    Effects0 = [
        {update_range, Manifest#manifest.first_offset, Manifest#manifest.next_offset},
        {broadcast, Cfg#cfg.stream,
            compute_edits(State0#state.last_persisted_manifest, CommittingManifest)},
        {evaluate_retention, Cfg#cfg.stream, Cfg#cfg.dir},
        cancel_persist_timer
    ],
    {State2, WaiterEffects} = notify_waiters(State1),
    Effects1 = Effects0 ++ WaiterEffects,
    %% If more fragments applied while commit was in flight, trigger another.
    case State2#state.since_persist > 0 of
        true ->
            {State3, CommitEffects} = maybe_start_persist(State2),
            {State3, Effects1 ++ CommitEffects};
        false ->
            {State2, Effects1}
    end.

-spec persist_failed(term(), state()) -> {state(), [core_effect()]}.
persist_failed(conflict, State) ->
    %% Khepri conflict. The shell must re-resolve the manifest externally
    %% and re-init the core. We signal this by returning a special effect.
    {State#state{persist_in_flight = false}, [reinitialize]};
persist_failed(_Reason, #state{cfg = Cfg} = State0) ->
    %% S3 or transient error. Retry the commit.
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
Apply a remote retention edit to the manifest. Updates both the in-memory
and last-committed manifests. Returns effects to broadcast and update range.
""".
-spec apply_retention_edit(#edit{}, state()) -> {state(), [core_effect()]}.
apply_retention_edit(
    Edit, #state{cfg = Cfg, manifest = Manifest0, last_persisted_manifest = Committed0} = State
) ->
    Manifest = rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest0),
    Committed = rabbitmq_stream_s3_manifest:apply_edit(Edit, Committed0),
    State1 = State#state{manifest = Manifest, last_persisted_manifest = Committed},
    Effects = [
        {broadcast, Cfg#cfg.stream, [Edit]},
        {update_range, Manifest#manifest.first_offset, Manifest#manifest.next_offset}
    ],
    {State1, Effects}.

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
                        pending_completions = maps:remove(Ref, State1#state.pending_completions)
                    },
                    %% Continue draining contiguous completions.
                    {State3, Effects} = drain_completions(State2),
                    %% After all draining, check commit trigger.
                    {State4, CommitEffects} = maybe_start_persist(State3),
                    {State4, Effects ++ CommitEffects}
            end;
        empty ->
            {State0, []}
    end.

-spec apply_fragment(rabbitmq_stream_s3:uid(), fragment_meta(), state()) -> state().
apply_fragment(Uid, Meta, #state{manifest = Manifest0, since_persist = N} = State) ->
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
        case Manifest0#manifest.next_offset of
            0 ->
                Edit#edit{
                    first_offset = FirstOffset,
                    first_timestamp = FirstTs,
                    first_last_timestamp = LastTs
                };
            _ ->
                Edit
        end,
    Manifest = rabbitmq_stream_s3_manifest:apply_edit(Edit1, Manifest0),
    State#state{manifest = Manifest, since_persist = N + 1}.

-spec maybe_start_persist(state()) -> {state(), [core_effect()]}.
maybe_start_persist(#state{persist_in_flight = true} = State) ->
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
        cfg = Cfg, manifest = Manifest, last_persisted_manifest = LastManifest, since_persist = N
    } =
        State
) ->
    Edits = edits_since_persist(State),
    Effect =
        {start_persist, Manifest, Cfg#cfg.epoch, Cfg#cfg.reference, LastManifest#manifest.revision,
            Edits},
    {State#state{persist_in_flight = true, in_persist_count = N, persisting_manifest = Manifest}, [
        Effect
    ]}.

-spec edits_since_persist(state()) -> [#edit{}].
edits_since_persist(#state{manifest = Manifest, last_persisted_manifest = Last}) ->
    compute_edits(Last, Manifest).

-spec compute_edits(#manifest{}, #manifest{}) -> [#edit{}].
compute_edits(From, To) ->
    case To#manifest.next_offset =:= From#manifest.next_offset of
        true ->
            [];
        false ->
            OldSize = byte_size(From#manifest.entries),
            NewEntries = binary:part(
                To#manifest.entries,
                OldSize,
                byte_size(To#manifest.entries) - OldSize
            ),
            [
                #edit{
                    first_offset = To#manifest.first_offset,
                    first_timestamp = To#manifest.first_timestamp,
                    first_last_timestamp = To#manifest.first_last_timestamp,
                    next_offset = To#manifest.next_offset,
                    size = To#manifest.total_size - From#manifest.total_size,
                    entries = NewEntries,
                    pos = OldSize,
                    len = 0
                }
            ]
    end.

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

-spec remove_in_flight(reference(), state()) -> state().
remove_in_flight(Ref, #state{in_flight = Q} = State) ->
    Q1 = queue:from_list([E || {R, _} = E <- queue:to_list(Q), R =/= Ref]),
    State#state{in_flight = Q1}.

-spec is_retriable(term()) -> boolean().
is_retriable({http, 500}) -> true;
is_retriable({http, 503}) -> true;
is_retriable(timeout) -> true;
is_retriable(_) -> false.
