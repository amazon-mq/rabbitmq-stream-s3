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
    format_state/1,
    fragment_cut/2,
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
    edits_since_persist = [] :: [#edit{}]
}).

-opaque state() :: #state{}.
-type cfg() :: #cfg{}.

-type core_effect() ::
    {submit_transfer, reference(), stream_id(), directory(), fragment_meta()}
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
    waiters = Waiters
}) ->
    #{
        manifest_first_offset => FirstOff,
        manifest_next_offset => NextOff,
        manifest_total_size => TotalSize,
        persisted_next_offset => PersistedNextOff,
        transfers_in_flight => queue:len(Q),
        transfers_pending_order => maps:size(PC),
        since_persist => SincePersist,
        in_persist_count => InPersistCount,
        persist_in_flight => PersistInFlight,
        last_persist_ts => LastPersistTs,
        rebalance_in_flight => RebalanceInFlight,
        retention_in_flight => RetentionInFlight,
        waiters => length(Waiters)
    }.

-spec fragment_cut(fragment_meta(), state()) -> {state(), reference(), [core_effect()]}.
fragment_cut(Meta, #state{cfg = Cfg, in_flight = Q, since_persist = 0} = State) ->
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
    %% Only discard the edits that were captured in the persisting batch.
    %% Edits that arrived during the in-flight persist were appended after
    %% start_persist captured its snapshot; those must be preserved.
    RemainingEdits = lists:nthtail(Committed, State0#state.edits_since_persist),
    State1 = State0#state{
        persist_in_flight = false,
        last_persisted_manifest = Manifest,
        persisting_manifest = undefined,
        since_persist = N - Committed,
        in_persist_count = 0,
        last_persist_ts = erlang:system_time(millisecond),
        edits_since_persist = RemainingEdits
    },
    Effects0 = [
        {update_range, Manifest#manifest.first_offset, Manifest#manifest.next_offset},
        {broadcast, Cfg#cfg.stream, State0#state.persisting_edits},
        {evaluate_retention, Cfg#cfg.stream, Cfg#cfg.dir},
        cancel_persist_timer
    ],
    {State2, WaiterEffects} = notify_waiters(State1),
    Effects1 = Effects0 ++ WaiterEffects,
    %% If more edits applied while persist was in flight, trigger another.
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
        edits_since_persist = State0#state.edits_since_persist ++ [Edit]
    },
    %% Check for recursive rebalancing (e.g. too many groups → kilo-group).
    {State2, RebalanceEffects} = maybe_start_rebalance(State1),
    {State3, PersistEffects} = maybe_start_persist(State2),
    {State3, RebalanceEffects ++ PersistEffects}.

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
        edits_since_persist = State0#state.edits_since_persist ++ [Edit]
    },
    {State2, PersistEffects} = maybe_start_persist(State1),
    {State2, PersistEffects}.

-doc """
A retention evaluation failed (e.g. group download failed). Clear the flag
so the next persist_complete can re-trigger evaluation.
""".
-spec retention_failed(term(), state()) -> {state(), [core_effect()]}.
retention_failed(_Reason, State) ->
    {State#state{retention_in_flight = false}, []}.

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
                    %% After all draining, check rebalance then persist trigger.
                    {State4, RebalanceEffects} = maybe_start_rebalance(State3),
                    {State5, CommitEffects} = maybe_start_persist(State4),
                    %% If persist didn't fire but fragments are pending, ensure
                    %% a timer is running so the tick will flush them. Without
                    %% this, bytes can sit in the persist stage indefinitely
                    %% when publishing stops and the threshold is not met.
                    TimerEffects =
                        case CommitEffects of
                            [] when State5#state.since_persist > 0 ->
                                [{start_persist_timer, (State5#state.cfg)#cfg.persist_interval_ms}];
                            _ ->
                                []
                        end,
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
    State#state{
        manifest = Manifest,
        since_persist = N + 1,
        edits_since_persist = Edits ++ [Edit1]
    }.

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
        edits_since_persist = Edits
    } =
        State
) ->
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

-spec remove_in_flight(reference(), state()) -> state().
remove_in_flight(Ref, #state{in_flight = Q} = State) ->
    Q1 = queue:from_list([E || {R, _} = E <- queue:to_list(Q), R =/= Ref]),
    State#state{in_flight = Q1}.

-spec is_retriable(term()) -> boolean().
is_retriable({http, 500}) -> true;
is_retriable({http, 503}) -> true;
is_retriable(timeout) -> true;
is_retriable(_) -> false.
