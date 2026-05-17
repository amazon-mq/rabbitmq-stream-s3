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
    fragment_cut/2,
    transfer_complete/3,
    transfer_failed/3,
    commit_complete/2,
    commit_failed/2,
    tick/2,
    await_offset/3
]).

-export_type([state/0, cfg/0, core_effect/0]).

-type fragment_meta() :: rabbitmq_stream_s3_fragment_assembly:fragment_meta().

-record(cfg, {
    stream :: stream_id(),
    dir :: directory(),
    epoch :: non_neg_integer(),
    reference :: term(),
    durable_commit_threshold :: non_neg_integer(),
    durable_commit_interval_ms :: non_neg_integer(),
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
    since_commit :: non_neg_integer(),
    %% Number of fragments included in the current in-flight commit.
    in_commit_count = 0 :: non_neg_integer(),
    %% Timestamp (milliseconds) of last durable commit.
    last_commit_ts :: integer(),
    %% Whether a durable commit is currently in flight.
    commit_in_flight :: boolean(),
    %% Manifest state at last successful durable commit.
    last_committed_manifest :: #manifest{},
    %% Manifest being committed (set when commit starts, used on completion).
    committing_manifest :: #manifest{} | undefined,
    %% Callers blocked on await_offset.
    waiters :: [{osiris:offset(), gen_server:from()}]
}).

-opaque state() :: #state{}.
-type cfg() :: #cfg{}.

-type core_effect() ::
    {submit_transfer, reference(), stream_id(), directory(), fragment_meta()}
    | {start_commit, #manifest{}, non_neg_integer(), term(), rabbitmq_stream_s3_db:revision(), [
        #edit{}
    ]}
    | {update_range, osiris:offset(), osiris:offset()}
    | {broadcast, stream_id(), [#edit{}]}
    | {evaluate_retention, stream_id(), directory()}
    | {reply_waiters, [{gen_server:from(), ok}]}
    | {start_commit_timer, non_neg_integer()}
    | {cancel_commit_timer}
    | {resubmit_transfer, reference(), stream_id(), directory(), fragment_meta()}
    | {reinitialize}.

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
        durable_commit_threshold = maps:get(durable_commit_threshold, Opts, 5),
        durable_commit_interval_ms = maps:get(durable_commit_interval_ms, Opts, 2000),
        rebalance_threshold = maps:get(rebalance_threshold, Opts, 1024)
    },
    State = #state{
        cfg = Cfg,
        manifest = Manifest,
        in_flight = queue:new(),
        pending_completions = #{},
        since_commit = 0,
        last_commit_ts = erlang:system_time(millisecond),
        commit_in_flight = false,
        last_committed_manifest = Manifest,
        committing_manifest = undefined,
        waiters = []
    },
    {State, []}.

-spec manifest(state()) -> #manifest{}.
manifest(#state{manifest = Manifest}) ->
    Manifest.

-spec fragment_cut(fragment_meta(), state()) -> {state(), reference(), [core_effect()]}.
fragment_cut(Meta, #state{cfg = Cfg, in_flight = Q, since_commit = 0} = State) ->
    case queue:is_empty(Q) of
        true ->
            %% First fragment since last commit. Start the timer.
            Ref = make_ref(),
            Q1 = queue:in({Ref, Meta}, Q),
            Effects = [
                {submit_transfer, Ref, Cfg#cfg.stream, Cfg#cfg.dir, Meta},
                {start_commit_timer, Cfg#cfg.durable_commit_interval_ms}
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

-spec commit_complete(rabbitmq_stream_s3_db:revision(), state()) -> {state(), [core_effect()]}.
commit_complete(
    Revision,
    #state{
        cfg = Cfg,
        committing_manifest = CommittingManifest,
        in_commit_count = Committed,
        since_commit = N
    } = State0
) ->
    Manifest = CommittingManifest#manifest{revision = Revision},
    State1 = State0#state{
        commit_in_flight = false,
        last_committed_manifest = Manifest,
        committing_manifest = undefined,
        since_commit = N - Committed,
        in_commit_count = 0,
        last_commit_ts = erlang:system_time(millisecond)
    },
    Effects0 = [
        {update_range, Manifest#manifest.first_offset, Manifest#manifest.next_offset},
        {broadcast, Cfg#cfg.stream,
            compute_edits(State0#state.last_committed_manifest, CommittingManifest)},
        {evaluate_retention, Cfg#cfg.stream, Cfg#cfg.dir},
        {cancel_commit_timer}
    ],
    {State2, WaiterEffects} = notify_waiters(State1),
    Effects1 = Effects0 ++ WaiterEffects,
    %% If more fragments applied while commit was in flight, trigger another.
    case State2#state.since_commit > 0 of
        true ->
            {State3, CommitEffects} = maybe_start_commit(State2),
            {State3, Effects1 ++ CommitEffects};
        false ->
            {State2, Effects1}
    end.

-spec commit_failed(term(), state()) -> {state(), [core_effect()]}.
commit_failed(conflict, State) ->
    %% Khepri conflict. The shell must re-resolve the manifest externally
    %% and re-init the core. We signal this by returning a special effect.
    {State#state{commit_in_flight = false}, [{reinitialize}]};
commit_failed(_Reason, #state{cfg = Cfg} = State0) ->
    %% S3 or transient error. Retry the commit.
    State1 = State0#state{commit_in_flight = false},
    {State2, Effects} = maybe_start_commit(State1),
    case Effects of
        [] ->
            %% Fallback: use timer to retry.
            {State2, [{start_commit_timer, Cfg#cfg.durable_commit_interval_ms}]};
        _ ->
            {State2, Effects}
    end.

-spec tick(integer(), state()) -> {state(), [core_effect()]}.
tick(
    Now,
    #state{
        since_commit = SinceCommit,
        commit_in_flight = false,
        last_commit_ts = LastTs,
        cfg = #cfg{durable_commit_interval_ms = Interval}
    } = State
) when SinceCommit > 0, (Now - LastTs) >= Interval ->
    start_commit(State);
tick(_Now, State) ->
    {State, []}.

-spec await_offset(osiris:offset(), gen_server:from(), state()) ->
    {state(), [core_effect()]}.
await_offset(Offset, From, #state{manifest = Manifest} = State) ->
    case Manifest#manifest.next_offset >= Offset of
        true ->
            {State, [{reply_waiters, [{From, ok}]}]};
        false ->
            {State#state{waiters = [{Offset, From} | State#state.waiters]}, []}
    end.

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
                    {State4, CommitEffects} = maybe_start_commit(State3),
                    {State4, Effects ++ CommitEffects}
            end;
        empty ->
            {State0, []}
    end.

-spec apply_fragment(rabbitmq_stream_s3:uid(), fragment_meta(), state()) -> state().
apply_fragment(Uid, Meta, #state{manifest = Manifest0, since_commit = N} = State) ->
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
    State#state{manifest = Manifest, since_commit = N + 1}.

-spec maybe_start_commit(state()) -> {state(), [core_effect()]}.
maybe_start_commit(#state{commit_in_flight = true} = State) ->
    {State, []};
maybe_start_commit(#state{since_commit = 0} = State) ->
    {State, []};
maybe_start_commit(
    #state{
        cfg = Cfg,
        since_commit = N,
        commit_in_flight = false
    } = State
) when N >= Cfg#cfg.durable_commit_threshold ->
    start_commit(State);
maybe_start_commit(State) ->
    {State, []}.

-spec start_commit(state()) -> {state(), [core_effect()]}.
start_commit(
    #state{cfg = Cfg, manifest = Manifest, last_committed_manifest = LastManifest, since_commit = N} =
        State
) ->
    Edits = edits_since_commit(State),
    Effect =
        {start_commit, Manifest, Cfg#cfg.epoch, Cfg#cfg.reference, LastManifest#manifest.revision,
            Edits},
    {State#state{commit_in_flight = true, in_commit_count = N, committing_manifest = Manifest}, [
        Effect
    ]}.

-spec edits_since_commit(state()) -> [#edit{}].
edits_since_commit(#state{manifest = Manifest, last_committed_manifest = Last}) ->
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
notify_waiters(#state{manifest = Manifest, waiters = Waiters} = State) ->
    NextOffset = Manifest#manifest.next_offset,
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
