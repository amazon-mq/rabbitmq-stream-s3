%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(prop_SUITE).
-moduledoc """
Property-based tests for core invariants.

Covers:
- Manifest edits (append, truncate, replace) preserve structural invariants
- Fragment assembly metadata correctness
- Fragment iterator traversal completeness and ordering
- Garbage collection never reaps a live object and always reclaims a genuine
  orphan (INV#2), the Erlang-level companion to the P models in p/
- Tier resolution routes remote-only offsets to the remote tier, never
  collapses a transient group fetch to a silent local read, and reports a read
  range spanning both tiers (INV#4), mirroring the tier-routing and
  read-resolution P models
- A manifest-replica sync is dropped iff it is older than what is recorded,
  comparing epoch before sequence (the manifest-replica-lifecycle model's
  epoch-monotonicity invariant)
- The read buffer (block queue) serves byte-exact reads against a flat-binary
  model under arbitrary append/read/drop interleavings, and the remote reader
  core built on it replies with byte-exact data at every read offset
""".

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").

all() ->
    [
        %% Manifest edit properties
        append_edit_advances_next_offset,
        append_edit_grows_total_size,
        append_edit_preserves_entry_order,
        truncate_edit_advances_first_offset,
        truncate_edit_shrinks_total_size,
        sequence_of_edits_maintains_invariants,
        retention_cycles_preserve_accounting,
        %% Fragment assembly properties
        assembly_size_is_sum_of_chunks,
        assembly_offsets_are_bounded,
        assembly_cuts_at_or_above_target,
        assembly_spans_are_non_overlapping,
        assembly_index_positions_are_monotonic,
        %% Fragment iterator properties
        iterator_visits_all_fragments,
        iterator_returns_fragments_in_order,
        iterator_size_matches_entries,
        %% Array operations
        array_partition_point,
        array_binary_search_by,
        array_rfind,
        array_fold,
        %% Range spec (FS backend)
        range_spec_to_location_number_suffix,
        range_spec_to_location_number_prefix,
        range_spec_to_location_number_byte_range,
        %% Fragment/index lookup
        find_fragment_timestamp,
        find_index_position_offset,
        find_index_position_timestamp,
        %% Replica reader core: rebalancing
        broadcast_edits_reproduce_manifest,
        broadcast_edits_complete_across_persist_cycles,
        broadcast_edits_with_retention,
        %% Remote reader core
        remote_reader_core_no_crash,
        remote_reader_core_reply_size_bounded,
        remote_reader_core_reply_bytes_exact,
        remote_reader_core_survives_failure_interleavings,
        remote_reader_core_load_bounded,
        remote_reader_core_look_ahead_recovers,
        %% Read pipeline (reassembly queue over the block buffer)
        read_pipeline_matches_model,
        %% Read buffer (block queue)
        read_buffer_matches_model,
        %% Garbage collection reap decision
        gc_classify_never_reaps_live,
        gc_classify_reclaims_orphans,
        gc_still_dangling_respects_live_manifest,
        gc_still_dangling_pending_row_never_deletes,
        gc_stream_lookup_epoch_gate,
        gc_fresh_enough_fails_closed,
        gc_anchor_decision_fails_closed,
        gc_cache_epoch_gate,
        gc_epoch_permits_sweep,
        %% Tier resolution (INV#4)
        tier_routing_empty_local_never_routes_to_offset,
        tier_routing_below_remote_first_routes_remote,
        total_range_spans_both_tiers,
        abs_offset_out_of_total_range_is_rejected,
        resolve_first_lookup_never_silent_local,
        %% Manifest-replica sync staleness (epoch monotonicity)
        is_stale_sync_is_total_order
    ].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.

%% =========================================================================
%% Manifest edit properties
%% =========================================================================

append_edit_advances_next_offset(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_append_edit_advances_next_offset/0, [], 500).

prop_append_edit_advances_next_offset() ->
    ?FORALL(
        {Manifest, Fragments},
        gen_manifest_and_appends(),
        begin
            Final = apply_appends(Fragments, Manifest),
            %% next_offset equals the last fragment's next_offset.
            {_Offset, _FTs, _LTs, NextOff, _Size} = lists:last(Fragments),
            Final#manifest.next_offset =:= NextOff
        end
    ).

append_edit_grows_total_size(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_append_edit_grows_total_size/0, [], 500).

prop_append_edit_grows_total_size() ->
    ?FORALL(
        {Manifest, Fragments},
        gen_manifest_and_appends(),
        begin
            Final = apply_appends(Fragments, Manifest),
            AddedSize = lists:sum([S || {_, _, _, _, S} <- Fragments]),
            Final#manifest.total_size =:= Manifest#manifest.total_size + AddedSize
        end
    ).

append_edit_preserves_entry_order(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_append_edit_preserves_entry_order/0, [], 500).

prop_append_edit_preserves_entry_order() ->
    ?FORALL(
        {Manifest, Fragments},
        gen_manifest_and_appends(),
        begin
            Final = apply_appends(Fragments, Manifest),
            entries_sorted_by_offset(Final#manifest.entries)
        end
    ).

truncate_edit_advances_first_offset(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_truncate_edit_advances_first_offset/0, [], 500).

prop_truncate_edit_advances_first_offset() ->
    ?FORALL(
        {Manifest, NumToRemove},
        gen_manifest_and_truncation(),
        begin
            NumEntries = byte_size(Manifest#manifest.entries) div ?ENTRY_B,
            case NumToRemove < NumEntries of
                true ->
                    Final = apply_truncation(NumToRemove, Manifest),
                    Final#manifest.first_offset >= Manifest#manifest.first_offset;
                false ->
                    %% Can't truncate more than we have; skip.
                    true
            end
        end
    ).

truncate_edit_shrinks_total_size(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_truncate_edit_shrinks_total_size/0, [], 500).

prop_truncate_edit_shrinks_total_size() ->
    ?FORALL(
        {Manifest, NumToRemove},
        gen_manifest_and_truncation(),
        begin
            NumEntries = byte_size(Manifest#manifest.entries) div ?ENTRY_B,
            case NumToRemove < NumEntries of
                true ->
                    Final = apply_truncation(NumToRemove, Manifest),
                    Final#manifest.total_size =< Manifest#manifest.total_size;
                false ->
                    true
            end
        end
    ).

sequence_of_edits_maintains_invariants(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_sequence_of_edits_maintains_invariants/0, [], 300).

prop_sequence_of_edits_maintains_invariants() ->
    ?FORALL(
        Ops,
        gen_edit_sequence(),
        begin
            Final = lists:foldl(fun apply_op/2, #manifest{}, Ops),
            %% The structural well-formedness predicate must hold for the result
            %% of any valid edit sequence (apply_edit also asserts it after each
            %% step, so a malformed intermediate would already have raised).
            rabbitmq_stream_s3_manifest:is_well_formed(Final) andalso
                entries_sorted_by_offset(Final#manifest.entries) andalso
                Final#manifest.total_size >= 0 andalso
                Final#manifest.next_offset >= Final#manifest.first_offset
        end
    ).

%% =========================================================================
%% Manifest retention properties
%% =========================================================================

retention_cycles_preserve_accounting(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_retention_cycles_preserve_accounting/0, [], 500
    ).

%% Property: across any sequence of remote-retention cycles (max_bytes and
%% max_age, in any order) against a manifest containing a group, the accounting
%% stays consistent with an independent model of which fragments survive:
%%   - no fragment is deleted twice (idempotent across cycles);
%%   - deletions are always an oldest-contiguous prefix (never a middle hole);
%%   - total_size equals the sum of surviving fragment sizes (never drifts or
%%     goes negative);
%%   - first_offset equals the oldest survivor and never regresses;
%%   - next_offset is unchanged by retention.
%% A group object is immutable, so re-reading it must not re-process already
%% removed children -- the regression this guards (see manifest_SUITE).
prop_retention_cycles_preserve_accounting() ->
    ?FORALL(
        {TreeSpec, Frags, Schedule},
        gen_ret_scenario(),
        begin
            {M0, GetGroup} = rabbitmq_stream_s3_test_helpers:build_manifest(TreeSpec),
            check_ret_cycles(Schedule, M0, GetGroup, Frags, M0#manifest.first_offset)
        end
    ).

%% Fold the schedule, applying each retention cycle and checking the invariants
%% against the live-fragment model (a list of {Offset, Size, LastTs}).
check_ret_cycles([], _M, _GetGroup, _Live, _PrevFirst) ->
    true;
check_ret_cycles([{Specs, Now} | Rest], M, GetGroup, Live, PrevFirst) ->
    case rabbitmq_stream_s3_manifest:evaluate_remote_retention(M, Specs, Now, GetGroup) of
        unchanged ->
            check_ret_cycles(Rest, M, GetGroup, Live, PrevFirst);
        {Edit, Refs} ->
            DelOffs = [O || #fragment_ref{offset = O} <- Refs],
            LiveOffs = [O || {O, _, _} <- Live],
            M1 = rabbitmq_stream_s3_manifest:apply_edit(Edit, M),
            Live1 = [F || {O, _, _} = F <- Live, not lists:member(O, DelOffs)],
            SurvOffs = [O || {O, _, _} <- Live1],
            ExpSize = lists:sum([S || {_, S, _} <- Live1]),
            Checks = [
                %% No fragment deleted twice, this cycle or in a prior one.
                lists:all(fun(O) -> lists:member(O, LiveOffs) end, DelOffs),
                length(DelOffs) =:= length(lists:usort(DelOffs)),
                %% Deletions are an oldest-contiguous prefix.
                is_oldest_prefix(DelOffs, SurvOffs),
                %% total_size tracks the surviving fragments exactly.
                M1#manifest.total_size =:= ExpSize,
                M1#manifest.total_size >= 0,
                %% first_offset is the oldest survivor and never regresses.
                first_offset_ok(M1, SurvOffs),
                M1#manifest.first_offset >= PrevFirst,
                %% Retention never moves next_offset.
                M1#manifest.next_offset =:= M#manifest.next_offset
            ],
            case lists:all(fun(B) -> B end, Checks) of
                true ->
                    check_ret_cycles(Rest, M1, GetGroup, Live1, M1#manifest.first_offset);
                false ->
                    false
            end
    end.

is_oldest_prefix([], _SurvOffs) -> true;
is_oldest_prefix(_DelOffs, []) -> true;
is_oldest_prefix(DelOffs, SurvOffs) -> lists:max(DelOffs) < lists:min(SurvOffs).

first_offset_ok(M, []) -> M#manifest.first_offset =:= M#manifest.next_offset;
first_offset_ok(M, SurvOffs) -> M#manifest.first_offset =:= lists:min(SurvOffs).

%% Generate a manifest with a leading group of G fragments followed by R
%% trailing root fragments (offsets 0,100,..., last_ts strictly increasing),
%% plus a schedule of retention cycles to apply. The leading group is what
%% exercises partial-then-further group consumption across cycles.
gen_ret_scenario() ->
    ?LET(
        {G, R},
        {integer(3, 6), integer(1, 3)},
        ?LET(
            Sizes,
            vector(G + R, integer(50, 500)),
            begin
                N = G + R,
                Frags = [
                    {I * 100, lists:nth(I + 1, Sizes), (I + 1) * 1000}
                 || I <- lists:seq(0, N - 1)
                ],
                GroupFrags = lists:sublist(Frags, G),
                RootFrags = lists:nthtail(G, Frags),
                Total = lists:sum(Sizes),
                MaxTs = N * 1000,
                ?LET(
                    {Structure, Schedule},
                    {oneof([flat, kilo]), gen_ret_schedule(Total, MaxTs)},
                    begin
                        TreeSpec =
                            [leading_tree(Structure, GroupFrags)] ++
                                [frag_spec(F) || F <- RootFrags],
                        {TreeSpec, Frags, Schedule}
                    end
                )
            end
        )
    ).

%% Nest the leading fragments either in one flat group or, to exercise the
%% recursive multi-tier retention path, in a kilo-group of two-fragment groups.
leading_tree(flat, Frags) ->
    {group, [frag_spec(F) || F <- Frags]};
leading_tree(kilo, Frags) ->
    {kilo_group, [{group, [frag_spec(F) || F <- Chunk]} || Chunk <- chunks_of(2, Frags)]}.

chunks_of(_N, []) ->
    [];
chunks_of(N, Frags) ->
    {Head, Tail} = lists:split(min(N, length(Frags)), Frags),
    [Head | chunks_of(N, Tail)].

frag_spec({Off, Size, LastTs}) ->
    {fragment, #{
        offset => Off, size => Size, first_ts => LastTs, last_ts => LastTs, uid => Off + 1
    }}.

gen_ret_schedule(Total, MaxTs) ->
    ?LET(K, integer(2, 6), vector(K, gen_ret_step(Total, MaxTs))).

%% A step is either a max_bytes target (any size, applied at Now = 0) or a
%% max_age cutoff expressed as Now with a zero max-age (cutoff = Now).
gen_ret_step(Total, MaxTs) ->
    frequency([
        {1, ?LET(T, integer(0, Total), {[{max_bytes, T}], 0})},
        {1, ?LET(Now, integer(0, MaxTs + 2000), {[{max_age, 0}], Now})}
    ]).

%% =========================================================================
%% Replica reader core: rebalancing
%% =========================================================================

broadcast_edits_reproduce_manifest(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_broadcast_edits_reproduce_manifest/0, [], 500
    ).

%% Property: for any sequence of fragment completions (possibly triggering
%% rebalancing), the broadcast edits produced at persist time transform the
%% last-persisted manifest into the persisting manifest.
prop_broadcast_edits_reproduce_manifest() ->
    ?FORALL(
        NumFragments,
        integer(1, 30),
        begin
            %% Use a low threshold so rebalancing triggers within the test.
            Threshold = 4,
            Opts = #{
                stream => <<"stream">>,
                dir => <<"/dir">>,
                epoch => 1,
                reference => test_ref,
                persist_threshold => 100,
                persist_interval_ms => 999999,
                rebalance_threshold => Threshold
            },
            {S0, _} = rabbitmq_stream_s3_replica_reader_core:init(#manifest{}, Opts),
            %% Apply NumFragments fragments, completing group uploads as they arise.
            S1 = apply_n_fragments(NumFragments, Threshold, S0),
            %% Force a persist via tick.
            Now = erlang:system_time(millisecond) + 999999,
            case rabbitmq_stream_s3_replica_reader_core:tick(Now, S1) of
                {S2, [{start_persist, _, _, _, _, _}]} ->
                    From = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S2),
                    {S3, Effects} =
                        rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S2),
                    To = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S3),
                    [Edits] = [Es || {broadcast, _, Es} <- Effects],
                    Replicated = lists:foldl(
                        fun(Edit, M) ->
                            rabbitmq_stream_s3_manifest:apply_edit(Edit, M)
                        end,
                        From,
                        Edits
                    ),
                    Replicated#manifest.entries =:= To#manifest.entries andalso
                        Replicated#manifest.first_offset =:= To#manifest.first_offset andalso
                        Replicated#manifest.next_offset =:= To#manifest.next_offset andalso
                        Replicated#manifest.total_size =:= To#manifest.total_size;
                {_S2, []} ->
                    %% Nothing to persist (no fragments applied). Trivially true.
                    true
            end
        end
    ).

%% Apply N fragments to the core, completing group uploads as they trigger.
apply_n_fragments(N, Threshold, State) ->
    lists:foldl(
        fun(I, S0) ->
            FirstOff = I * 100,
            NextOff = (I + 1) * 100,
            Meta = #{
                first_offset => FirstOff,
                first_timestamp => FirstOff * 1000,
                last_timestamp => (NextOff - 1) * 1000,
                next_offset => NextOff,
                size => 64_000_000,
                num_chunks => 100,
                spans => [{0, 8, 64_000_008}]
            },
            {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(Meta, S0),
            {S2, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(
                Ref, 1000 + I, S1
            ),
            %% If rebalance was triggered, complete it immediately.
            complete_pending_rebalances(Effects, S2, Threshold)
        end,
        State,
        lists:seq(0, N - 1)
    ).

%% Recursively complete group uploads until no more are pending.
complete_pending_rebalances(Effects, State, Threshold) ->
    case [E || {upload_group, _, _, _, _, _} = E <- Effects] of
        [] ->
            State;
        [_ | _] ->
            Uid = erlang:unique_integer([positive]),
            {S1, Effects1} =
                rabbitmq_stream_s3_replica_reader_core:group_upload_complete(Uid, State),
            complete_pending_rebalances(Effects1, S1, Threshold)
    end.

%% =========================================================================
%% Replica reader core: multi-cycle broadcast completeness
%% =========================================================================

broadcast_edits_complete_across_persist_cycles(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_broadcast_edits_complete_across_persist_cycles/0, [], 500
    ).

%% Property: for any interleaving of fragment completions across multiple
%% persist cycles (including fragments completing during in-flight persists),
%% the concatenation of all broadcast edits reproduces the final persisted
%% manifest from the initial state.
prop_broadcast_edits_complete_across_persist_cycles() ->
    ?FORALL(
        {Threshold, Batches},
        {integer(1, 5), gen_persist_batches()},
        begin
            Opts = #{
                stream => <<"stream">>,
                dir => <<"/dir">>,
                epoch => 1,
                reference => test_ref,
                persist_threshold => Threshold,
                persist_interval_ms => 999999,
                rebalance_threshold => 1024
            },
            {S0, _} = rabbitmq_stream_s3_replica_reader_core:init(#manifest{}, Opts),
            {_FinalState, AllEdits} = run_persist_cycles(Batches, S0, 0, 1, []),
            %% Verify: applying all broadcast edits to an empty manifest
            %% produces the final persisted manifest.
            FinalPersisted = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(
                _FinalState
            ),
            Replicated = lists:foldl(
                fun(Edit, M) ->
                    rabbitmq_stream_s3_manifest:apply_edit(Edit, M)
                end,
                #manifest{},
                AllEdits
            ),
            Replicated#manifest.entries =:= FinalPersisted#manifest.entries andalso
                Replicated#manifest.next_offset =:= FinalPersisted#manifest.next_offset andalso
                Replicated#manifest.total_size =:= FinalPersisted#manifest.total_size
        end
    ).

%% Generate a list of batches. Each batch is a count of fragments to complete
%% before the next persist fires. Multiple batches = multiple persist cycles
%% with fragments arriving between and during persists.
gen_persist_batches() ->
    ?LET(N, integer(2, 6), [integer(1, 8) || _ <- lists:seq(1, N)]).

%% Run persist cycles driven by the batch list.
%% For each batch: complete that many fragments, then force a persist and
%% complete it. Fragments in later batches may arrive while a persist from
%% an earlier batch is conceptually in flight (simulated by completing
%% fragments between start_persist and persist_complete).
run_persist_cycles([], State, _Offset, Rev, Edits) ->
    %% Final persist to flush any remaining applied fragments.
    Now = erlang:system_time(millisecond) + 999999,
    case rabbitmq_stream_s3_replica_reader_core:tick(Now, State) of
        {S1, [{start_persist, _, _, _, _, _}]} ->
            {S2, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(Rev, S1),
            BroadcastEdits = lists:append([Es || {broadcast, _, Es} <- Effects]),
            {S2, Edits ++ BroadcastEdits};
        {S1, []} ->
            {S1, Edits}
    end;
run_persist_cycles([Count | Rest], State0, Offset0, Rev0, Edits0) ->
    %% Complete Count fragments. Some may trigger a persist via threshold.
    {State1, Offset1, PersistTriggered1} = complete_fragments(Count, State0, Offset0),
    %% If no persist was triggered by threshold, force one via tick.
    {State2, PersistTriggered2} =
        case PersistTriggered1 of
            true ->
                {State1, true};
            false ->
                Now = erlang:system_time(millisecond) + 999999,
                {S, Effs} = rabbitmq_stream_s3_replica_reader_core:tick(Now, State1),
                {S, Effs =/= []}
        end,
    %% Drain persists if one is in flight.
    {State3, Rev1, NewEdits} =
        case PersistTriggered2 of
            true -> drain_persists(State2, Rev0);
            false -> {State2, Rev0, []}
        end,
    run_persist_cycles(Rest, State3, Offset1, Rev1, Edits0 ++ NewEdits).

complete_fragments(0, State, Offset) ->
    {State, Offset, false};
complete_fragments(N, State0, Offset) ->
    Meta = #{
        first_offset => Offset,
        first_timestamp => Offset * 1000,
        last_timestamp => (Offset + 99) * 1000,
        next_offset => Offset + 100,
        size => 64_000_000,
        num_chunks => 100,
        spans => [{0, 8, 64_000_008}]
    },
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(Meta, State0),
    {S2, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(
        Ref, erlang:unique_integer([positive]), S1
    ),
    Triggered = [E || {start_persist, _, _, _, _, _} = E <- Effects] =/= [],
    case N - 1 of
        0 ->
            {S2, Offset + 100, Triggered};
        Rem ->
            {S3, Off, T2} = complete_fragments(Rem, S2, Offset + 100),
            {S3, Off, Triggered orelse T2}
    end.

%% Drain all persists. A persist_complete may trigger another persist
%% immediately (if fragments accumulated during the in-flight one).
drain_persists(State, Rev) ->
    drain_persists(State, Rev, []).

drain_persists(State0, Rev, Acc) ->
    {State1, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(Rev, State0),
    BroadcastEdits = lists:append([Es || {broadcast, _, Es} <- Effects]),
    case [E || {start_persist, _, _, _, _, _} = E <- Effects] of
        [_ | _] ->
            drain_persists(State1, Rev + 1, Acc ++ BroadcastEdits);
        [] ->
            {State1, Rev + 1, Acc ++ BroadcastEdits}
    end.

%% =========================================================================
%% Replica reader core: retention interleaved with appends and rebalancing
%% =========================================================================

broadcast_edits_with_retention(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_broadcast_edits_with_retention/0, [], 500
    ).

%% Property: for any interleaving of fragment appends and retention edits
%% (possibly triggering rebalancing), the concatenation of all broadcast
%% edits across all persist cycles reproduces the final persisted manifest.
prop_broadcast_edits_with_retention() ->
    ?FORALL(
        {RebalanceThreshold, Ops},
        {integer(3, 6), gen_ops_with_retention()},
        begin
            Opts = #{
                stream => <<"stream">>,
                dir => <<"/dir">>,
                epoch => 1,
                reference => test_ref,
                persist_threshold => 3,
                persist_interval_ms => 999999,
                rebalance_threshold => RebalanceThreshold
            },
            {S0, _} = rabbitmq_stream_s3_replica_reader_core:init(#manifest{}, Opts),
            {FinalState, AllEdits} = run_ops_with_retention(
                Ops, S0, 0, 1, RebalanceThreshold, []
            ),
            FinalPersisted = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(
                FinalState
            ),
            Replicated = lists:foldl(
                fun(Edit, M) ->
                    rabbitmq_stream_s3_manifest:apply_edit(Edit, M)
                end,
                #manifest{},
                AllEdits
            ),
            Replicated#manifest.entries =:= FinalPersisted#manifest.entries andalso
                Replicated#manifest.first_offset =:= FinalPersisted#manifest.first_offset andalso
                Replicated#manifest.next_offset =:= FinalPersisted#manifest.next_offset andalso
                Replicated#manifest.total_size =:= FinalPersisted#manifest.total_size
        end
    ).

%% Generate a sequence of operations weighted toward fragments.
gen_ops_with_retention() ->
    ?LET(
        N,
        integer(4, 20),
        [frequency([{4, fragment}, {1, retention}]) || _ <- lists:seq(1, N)]
    ).

%% Execute operations, draining persists as they trigger.
run_ops_with_retention([], State0, _Offset, Rev, _Threshold, Edits) ->
    %% Final persist to flush remaining edits.
    Now = erlang:system_time(millisecond) + 999999,
    case rabbitmq_stream_s3_replica_reader_core:tick(Now, State0) of
        {S1, [{start_persist, _, _, _, _, _}]} ->
            {S2, FinalEdits} = drain_all_persists(S1, Rev),
            {S2, Edits ++ FinalEdits};
        {S1, []} ->
            {S1, Edits}
    end;
run_ops_with_retention([fragment | Rest], State0, Offset, Rev, Threshold, Edits) ->
    Meta = #{
        first_offset => Offset,
        first_timestamp => Offset * 1000,
        last_timestamp => (Offset + 99) * 1000,
        next_offset => Offset + 100,
        size => 64_000_000,
        num_chunks => 100,
        spans => [{0, 8, 64_000_008}]
    },
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(Meta, State0),
    {S2, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(
        Ref, erlang:unique_integer([positive]), S1
    ),
    %% Complete any rebalance that triggered.
    S3 = complete_pending_rebalances(Effects, S2, Threshold),
    %% Drain any persist that triggered.
    {S4, NewRev, NewEdits} = maybe_drain_persist(S3, Effects, Rev),
    run_ops_with_retention(Rest, S4, Offset + 100, NewRev, Threshold, Edits ++ NewEdits);
run_ops_with_retention([retention | Rest], State0, Offset, Rev, Threshold, Edits) ->
    %% Only apply retention if the manifest has more than one entry AND the first
    %% entry is a fragment. The synthetic edit subtracts the first entry's size
    %% field, which is correct for a fragment but 0 for a group entry (a group's
    %% underlying fragment bytes are still counted in total_size; the real
    %% evaluate_remote_retention descends the group to compute the true delta,
    %% which this synthetic edit cannot). Removing a group here would leave
    %% total_size overcounting - a malformed manifest. Group-removal accounting
    %% is covered in replica_reader_core_SUITE.
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(State0),
    Entries = Manifest#manifest.entries,
    FirstIsFragment =
        case Entries of
            <<_:64, _:64/signed, _:64/signed, ?MANIFEST_KIND_FRAGMENT:8, _/binary>> -> true;
            _ -> false
        end,
    case byte_size(Entries) > ?ENTRY_B andalso FirstIsFragment of
        true ->
            %% Remove the first entry.
            <<_:64, _:64/signed, _:64/signed, _:8, Size:40, _:32, _/binary>> =
                Manifest#manifest.entries,
            <<_:?ENTRY_B/binary, NewFirst/binary>> = Manifest#manifest.entries,
            <<NewFirstOff:64, NewFirstTs:64/signed, NewFirstLTs:64/signed, _/binary>> = NewFirst,
            RetEdit = #edit{
                first_offset = NewFirstOff,
                first_timestamp = NewFirstTs,
                first_last_timestamp = NewFirstLTs,
                next_offset = undefined,
                size = -Size,
                entries = <<>>,
                pos = 0,
                len = ?ENTRY_B
            },
            {S1, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_complete(
                RetEdit, State0
            ),
            %% Drain any persist that triggered.
            {S2, NewRev, NewEdits} = maybe_drain_persist(S1, Effects, Rev),
            run_ops_with_retention(Rest, S2, Offset, NewRev, Threshold, Edits ++ NewEdits);
        false ->
            %% Skip retention if only one entry (must keep at least one).
            run_ops_with_retention(Rest, State0, Offset, Rev, Threshold, Edits)
    end.

%% If a persist was triggered in the effects, drain it (and any cascading persists).
maybe_drain_persist(State, Effects, Rev) ->
    case [E || {start_persist, _, _, _, _, _} = E <- Effects] of
        [_ | _] ->
            {S1, NewEdits} = drain_all_persists(State, Rev),
            {S1, Rev + 1, NewEdits};
        [] ->
            {State, Rev, []}
    end.

drain_all_persists(State0, Rev) ->
    drain_all_persists(State0, Rev, []).

drain_all_persists(State0, Rev, Acc) ->
    {State1, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(Rev, State0),
    BroadcastEdits = lists:append([Es || {broadcast, _, Es} <- Effects]),
    case [E || {start_persist, _, _, _, _, _} = E <- Effects] of
        [_ | _] ->
            drain_all_persists(State1, Rev + 1, Acc ++ BroadcastEdits);
        [] ->
            {State1, Acc ++ BroadcastEdits}
    end.

%% =========================================================================
%% Fragment assembly properties
%% =========================================================================

assembly_size_is_sum_of_chunks(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_assembly_size_is_sum_of_chunks/0, [], 500).

prop_assembly_size_is_sum_of_chunks() ->
    ?FORALL(
        Chunks,
        gen_chunk_sequence(),
        begin
            Assembly = build_assembly(Chunks),
            Meta = rabbitmq_stream_s3_fragment_assembly:metadata(Assembly),
            ExpectedSize = lists:sum([maps:get(data_size, C) || C <- Chunks]),
            maps:get(size, Meta) =:= ExpectedSize
        end
    ).

assembly_offsets_are_bounded(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_assembly_offsets_are_bounded/0, [], 500).

prop_assembly_offsets_are_bounded() ->
    ?FORALL(
        Chunks,
        gen_chunk_sequence(),
        begin
            Assembly = build_assembly(Chunks),
            Meta = rabbitmq_stream_s3_fragment_assembly:metadata(Assembly),
            FirstChunk = hd(Chunks),
            LastChunk = lists:last(Chunks),
            maps:get(first_offset, Meta) =:= maps:get(chunk_id, FirstChunk) andalso
                maps:get(first_timestamp, Meta) =:= maps:get(timestamp, FirstChunk) andalso
                maps:get(last_timestamp, Meta) =:= maps:get(timestamp, LastChunk) andalso
                maps:get(next_offset, Meta) > maps:get(first_offset, Meta)
        end
    ).

assembly_cuts_at_or_above_target(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_assembly_cuts_at_or_above_target/0, [], 500).

prop_assembly_cuts_at_or_above_target() ->
    ?FORALL(
        {Target, Chunks},
        gen_target_and_chunks(),
        begin
            {Assembly, Fed} = build_assembly_until_cut(Target, Chunks),
            FedSize = lists:sum([maps:get(data_size, C) || C <- Fed]),
            TotalSize = lists:sum([maps:get(data_size, C) || C <- Chunks]),
            case rabbitmq_stream_s3_fragment_assembly:is_cut(Assembly) of
                true ->
                    %% Cut happened: fed size must be >= target.
                    FedSize >= Target;
                false ->
                    %% Not cut: we fed all chunks and total < target.
                    length(Fed) =:= length(Chunks) andalso TotalSize < Target
            end
        end
    ).

assembly_spans_are_non_overlapping(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_assembly_spans_are_non_overlapping/0, [], 500).

prop_assembly_spans_are_non_overlapping() ->
    ?FORALL(
        Chunks,
        gen_multi_segment_chunks(),
        begin
            Assembly = build_assembly(Chunks),
            Meta = rabbitmq_stream_s3_fragment_assembly:metadata(Assembly),
            Spans = maps:get(spans, Meta),
            %% Each span has a unique segment offset.
            SegOffsets = [O || {O, _, _} <- Spans],
            length(SegOffsets) =:= length(lists:usort(SegOffsets)) andalso
                %% Each span has start < end.
                lists:all(fun({_, S, E}) -> S < E end, Spans)
        end
    ).

assembly_index_positions_are_monotonic(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_assembly_index_positions_are_monotonic/0, [], 500).

prop_assembly_index_positions_are_monotonic() ->
    ?FORALL(
        Chunks,
        gen_chunk_sequence(),
        begin
            Assembly = build_assembly(Chunks),
            IdxBin = rabbitmq_stream_s3_fragment_assembly:index_records(Assembly),
            Positions = extract_index_positions(IdxBin),
            %% Positions are strictly increasing.
            is_strictly_increasing(Positions)
        end
    ).

%% =========================================================================
%% Fragment iterator properties
%% =========================================================================

iterator_visits_all_fragments(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_iterator_visits_all_fragments/0, [], 200).

prop_iterator_visits_all_fragments() ->
    ?FORALL(
        TreeSpec,
        gen_manifest_tree(),
        begin
            {Manifest, GetGroup} = rabbitmq_stream_s3_test_helpers:build_manifest(TreeSpec),
            ExpectedCount = count_fragments(TreeSpec),
            It = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
            ActualCount = drain_iterator_count(It),
            ActualCount =:= ExpectedCount
        end
    ).

iterator_returns_fragments_in_order(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_iterator_returns_fragments_in_order/0, [], 200).

prop_iterator_returns_fragments_in_order() ->
    ?FORALL(
        TreeSpec,
        gen_manifest_tree(),
        begin
            {Manifest, GetGroup} = rabbitmq_stream_s3_test_helpers:build_manifest(TreeSpec),
            It = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
            Offsets = drain_iterator_offsets(It),
            is_strictly_increasing(Offsets)
        end
    ).

iterator_size_matches_entries(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_iterator_size_matches_entries/0, [], 200).

prop_iterator_size_matches_entries() ->
    ?FORALL(
        TreeSpec,
        gen_manifest_tree(),
        begin
            {Manifest, GetGroup} = rabbitmq_stream_s3_test_helpers:build_manifest(TreeSpec),
            It = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroup),
            Sizes = drain_iterator_sizes(It),
            %% All sizes are positive.
            lists:all(fun(S) -> S > 0 end, Sizes)
        end
    ).

%% =========================================================================
%% Generators
%% =========================================================================

%% Generate a manifest with some existing entries, plus a list of new
%% fragments to append (sorted, non-overlapping, continuing from next_offset).
gen_manifest_and_appends() ->
    ?LET(
        {NumExisting, NumNew},
        {integer(0, 10), integer(1, 10)},
        begin
            ExistingFrags = gen_fragment_list(0, NumExisting),
            ?LET(
                {Existing, New},
                {ExistingFrags, ?LAZY(gen_appends_after(NumExisting, NumNew))},
                {build_manifest(Existing), New}
            )
        end
    ).

gen_appends_after(StartIdx, Count) ->
    gen_fragment_list(StartIdx, Count).

gen_fragment_list(StartIdx, Count) ->
    ?LET(
        Sizes,
        vector(Count, integer(1000, 100000)),
        begin
            {Frags, _} = lists:foldl(
                fun(Size, {Acc, Offset}) ->
                    NumRecs = max(1, Size div 100),
                    NextOff = Offset + NumRecs,
                    FTs = Offset * 10,
                    LTs = (Offset + NumRecs) * 10,
                    {[{Offset, FTs, LTs, NextOff, Size} | Acc], NextOff}
                end,
                {[], StartIdx * 100},
                Sizes
            ),
            lists:reverse(Frags)
        end
    ).

gen_manifest_and_truncation() ->
    ?LET(
        NumEntries,
        integer(2, 15),
        ?LET(
            {Frags, NumToRemove},
            {gen_fragment_list(0, NumEntries), integer(1, NumEntries - 1)},
            {build_manifest(Frags), NumToRemove}
        )
    ).

gen_edit_sequence() ->
    ?LET(
        N,
        integer(1, 20),
        gen_edit_sequence(N, 0, [])
    ).

gen_edit_sequence(0, _NextIdx, Acc) ->
    lists:reverse(Acc);
gen_edit_sequence(N, NextIdx, Acc) ->
    ?LET(
        NumFrags,
        integer(1, 5),
        ?LET(
            Frags,
            gen_fragment_list(NextIdx, NumFrags),
            gen_edit_sequence(N - 1, NextIdx + NumFrags, [{append, Frags} | Acc])
        )
    ).

%% Generate a non-empty sequence of chunks in a single segment.
gen_chunk_sequence() ->
    ?LET(
        N,
        integer(1, 20),
        gen_chunks_in_segment(N, 0, 0, 8)
    ).

gen_chunks_in_segment(0, _Offset, _Ts, _Pos) ->
    [];
gen_chunks_in_segment(N, Offset, Ts, Pos) ->
    ?LET(
        {DataSize, NumRecs},
        {integer(50, 5000), integer(1, 100)},
        begin
            Chunk = #{
                chunk_id => Offset,
                timestamp => Ts,
                num_records => NumRecs,
                data_size => DataSize,
                position => Pos,
                next_position => Pos + DataSize,
                segment_offset => 0,
                crc => 0
            },
            ?LET(
                Rest,
                gen_chunks_in_segment(N - 1, Offset + NumRecs, Ts + 100, Pos + DataSize),
                [Chunk | Rest]
            )
        end
    ).

gen_target_and_chunks() ->
    ?LET(
        Chunks,
        gen_chunk_sequence(),
        begin
            TotalSize = lists:sum([maps:get(data_size, C) || C <- Chunks]),
            %% Target somewhere around the total so we get both cut and not-cut cases.
            ?LET(Target, integer(1, TotalSize * 2), {Target, Chunks})
        end
    ).

%% Generate chunks that span multiple segments.
gen_multi_segment_chunks() ->
    ?LET(
        NumSegments,
        integer(1, 4),
        gen_multi_segment_chunks(NumSegments, 0, 0)
    ).

gen_multi_segment_chunks(0, _Offset, _Ts) ->
    [];
gen_multi_segment_chunks(NumSegs, Offset, Ts) ->
    ?LET(
        N,
        integer(1, 5),
        ?LET(
            {SegChunks, NextOffset, NextTs},
            gen_segment_chunks(N, Offset, Ts, NumSegs - 1),
            ?LET(
                Rest,
                gen_multi_segment_chunks(NumSegs - 1, NextOffset, NextTs),
                SegChunks ++ Rest
            )
        )
    ).

gen_segment_chunks(N, Offset, Ts, SegIdx) ->
    ?LET(
        Chunks,
        gen_chunks_in_segment_at(N, Offset, Ts, 8, SegIdx * 512000),
        begin
            Last = lists:last(Chunks),
            {Chunks, maps:get(chunk_id, Last) + maps:get(num_records, Last),
                maps:get(timestamp, Last) + 100}
        end
    ).

gen_chunks_in_segment_at(0, _Offset, _Ts, _Pos, _SegOffset) ->
    [];
gen_chunks_in_segment_at(N, Offset, Ts, Pos, SegOffset) ->
    ?LET(
        {DataSize, NumRecs},
        {integer(50, 2000), integer(1, 50)},
        begin
            Chunk = #{
                chunk_id => Offset,
                timestamp => Ts,
                num_records => NumRecs,
                data_size => DataSize,
                position => Pos,
                next_position => Pos + DataSize,
                segment_offset => SegOffset,
                crc => 0
            },
            ?LET(
                Rest,
                gen_chunks_in_segment_at(
                    N - 1, Offset + NumRecs, Ts + 100, Pos + DataSize, SegOffset
                ),
                [Chunk | Rest]
            )
        end
    ).

%% Generate a manifest tree spec for the fragment iterator.
gen_manifest_tree() ->
    ?LET(
        N,
        integer(1, 8),
        gen_tree_entries(N, 0)
    ).

gen_tree_entries(0, _Offset) ->
    [];
gen_tree_entries(N, _Offset) when N =< 0 ->
    [];
gen_tree_entries(N, Offset) ->
    ?LET(
        Kind,
        frequency([{6, fragment}, {3, group}, {1, kilo_group}]),
        case Kind of
            fragment ->
                ?LET(
                    Rest,
                    gen_tree_entries(N - 1, Offset + 100),
                    [{fragment, #{offset => Offset, size => 64000}} | Rest]
                );
            group ->
                NumChildren = min(N, 3),
                ?LET(
                    Children,
                    gen_tree_fragments(NumChildren, Offset),
                    begin
                        LastChild = lists:last(Children),
                        {fragment, #{offset := LastOff}} = LastChild,
                        ?LET(
                            Rest,
                            gen_tree_entries(N - NumChildren, LastOff + 100),
                            [{group, Children} | Rest]
                        )
                    end
                );
            kilo_group ->
                %% A kilo_group with 2 groups of 2 fragments each.
                ?LET(
                    Groups,
                    gen_tree_groups(2, Offset),
                    begin
                        LastOff = last_offset_in_groups(Groups),
                        ?LET(
                            Rest,
                            gen_tree_entries(N - 4, LastOff + 100),
                            [{kilo_group, Groups} | Rest]
                        )
                    end
                )
        end
    ).

gen_tree_fragments(0, _Offset) ->
    [];
gen_tree_fragments(N, Offset) ->
    ?LET(
        Rest,
        gen_tree_fragments(N - 1, Offset + 100),
        [{fragment, #{offset => Offset, size => 64000}} | Rest]
    ).

gen_tree_groups(0, _Offset) ->
    [];
gen_tree_groups(N, Offset) ->
    ?LET(
        Children,
        gen_tree_fragments(2, Offset),
        begin
            {fragment, #{offset := LastOff}} = lists:last(Children),
            ?LET(
                Rest,
                gen_tree_groups(N - 1, LastOff + 100),
                [{group, Children} | Rest]
            )
        end
    ).

%% =========================================================================
%% Helpers
%% =========================================================================

build_manifest([]) ->
    #manifest{};
build_manifest(Frags) ->
    {FirstOff, FirstTs, FirstLastTs, _, _} = hd(Frags),
    {_, _, _, NextOff, _} = lists:last(Frags),
    TotalSize = lists:sum([S || {_, _, _, _, S} <- Frags]),
    Entries = iolist_to_binary([
        ?ENTRY(O, FTs, LTs, ?MANIFEST_KIND_FRAGMENT, Size, 0)
     || {O, FTs, LTs, _, Size} <- Frags
    ]),
    #manifest{
        first_offset = FirstOff,
        next_offset = NextOff,
        first_timestamp = FirstTs,
        first_last_timestamp = FirstLastTs,
        total_size = TotalSize,
        entries = Entries
    }.

apply_appends(Fragments, Manifest) ->
    lists:foldl(
        fun({Offset, FTs, LTs, NextOff, Size}, M) ->
            Entry = ?ENTRY(Offset, FTs, LTs, ?MANIFEST_KIND_FRAGMENT, Size, 0),
            Edit = #edit{
                first_offset =
                    case M#manifest.next_offset of
                        0 -> Offset;
                        _ -> M#manifest.first_offset
                    end,
                first_timestamp =
                    case M#manifest.next_offset of
                        0 -> FTs;
                        _ -> M#manifest.first_timestamp
                    end,
                first_last_timestamp =
                    case M#manifest.next_offset of
                        0 -> LTs;
                        _ -> M#manifest.first_last_timestamp
                    end,
                next_offset = NextOff,
                size = Size,
                entries = Entry,
                pos = byte_size(M#manifest.entries),
                len = 0
            },
            rabbitmq_stream_s3_manifest:apply_edit(Edit, M)
        end,
        Manifest,
        Fragments
    ).

apply_truncation(NumToRemove, Manifest) ->
    Entries = Manifest#manifest.entries,
    Len = NumToRemove * ?ENTRY_B,
    %% Compute the size being removed.
    Removed = binary:part(Entries, 0, Len),
    RemovedSize = sum_entry_sizes(Removed),
    %% New first entry.
    Remaining = binary:part(Entries, Len, byte_size(Entries) - Len),
    ?ENTRY(NewFirstOff, NewFirstTs, NewFirstLastTs, _, _, _, _) = Remaining,
    Edit = #edit{
        first_offset = NewFirstOff,
        first_timestamp = NewFirstTs,
        first_last_timestamp = NewFirstLastTs,
        next_offset = undefined,
        size = -RemovedSize,
        entries = <<>>,
        pos = 0,
        len = Len
    },
    rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest).

apply_op({append, Frags}, Manifest) ->
    apply_appends(Frags, Manifest).

sum_entry_sizes(<<>>) ->
    0;
sum_entry_sizes(?ENTRY(_O, _FTs, _LTs, _Kind, Size, _Uid, Rest)) ->
    Size + sum_entry_sizes(Rest).

entries_sorted_by_offset(<<>>) ->
    true;
entries_sorted_by_offset(Entries) when byte_size(Entries) =:= ?ENTRY_B ->
    true;
entries_sorted_by_offset(?ENTRY(O1, _, _, _, _, _, Rest)) ->
    ?ENTRY(O2, _, _, _, _, _, _) = Rest,
    O1 < O2 andalso entries_sorted_by_offset(Rest).

build_assembly(Chunks) ->
    build_assembly_with_target(999_999_999, Chunks).

build_assembly_with_target(Target, Chunks) ->
    lists:foldl(
        fun(C, A) -> rabbitmq_stream_s3_fragment_assembly:add_chunk(C, A) end,
        rabbitmq_stream_s3_fragment_assembly:new(Target),
        Chunks
    ).

build_assembly_until_cut(Target, Chunks) ->
    build_assembly_until_cut(rabbitmq_stream_s3_fragment_assembly:new(Target), Chunks, []).

build_assembly_until_cut(Assembly, [], Fed) ->
    {Assembly, lists:reverse(Fed)};
build_assembly_until_cut(Assembly0, [C | Rest], Fed) ->
    Assembly = rabbitmq_stream_s3_fragment_assembly:add_chunk(C, Assembly0),
    case rabbitmq_stream_s3_fragment_assembly:is_cut(Assembly) of
        true -> {Assembly, lists:reverse([C | Fed])};
        false -> build_assembly_until_cut(Assembly, Rest, [C | Fed])
    end.

extract_index_positions(<<>>) ->
    [];
extract_index_positions(<<_ChunkId:64, _Ts:64, Pos:32/unsigned, Rest/binary>>) ->
    [Pos | extract_index_positions(Rest)].

is_strictly_increasing([]) -> true;
is_strictly_increasing([_]) -> true;
is_strictly_increasing([A, B | Rest]) -> A < B andalso is_strictly_increasing([B | Rest]).

drain_iterator_count(It) ->
    case rabbitmq_stream_s3_fragment_iterator:next(It) of
        {ok, _, It1} -> 1 + drain_iterator_count(It1);
        end_of_manifest -> 0
    end.

drain_iterator_offsets(It) ->
    case rabbitmq_stream_s3_fragment_iterator:next(It) of
        {ok, #fragment_ref{offset = Offset}, It1} -> [Offset | drain_iterator_offsets(It1)];
        end_of_manifest -> []
    end.

drain_iterator_sizes(It) ->
    case rabbitmq_stream_s3_fragment_iterator:next(It) of
        {ok, #fragment_ref{size = Size}, It1} -> [Size | drain_iterator_sizes(It1)];
        end_of_manifest -> []
    end.

count_fragments([]) ->
    0;
count_fragments([{fragment, _} | Rest]) ->
    1 + count_fragments(Rest);
count_fragments([{group, Children} | Rest]) ->
    count_fragments(Children) + count_fragments(Rest);
count_fragments([{kilo_group, Children} | Rest]) ->
    count_fragments(Children) + count_fragments(Rest);
count_fragments([{mega_group, Children} | Rest]) ->
    count_fragments(Children) + count_fragments(Rest).

last_offset_in_groups([]) ->
    0;
last_offset_in_groups(Groups) ->
    {group, Children} = lists:last(Groups),
    {fragment, #{offset := O}} = lists:last(Children),
    O.

%% =========================================================================
%% Array operation properties
%% =========================================================================

-define(ARRAY_ENTRY_SIZE, 8).

array_partition_point(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_array_partition_point/0, [], 500).

prop_array_partition_point() ->
    ?FORALL(
        {SortedValues, Threshold},
        ?LET(Vs, list(non_neg_integer()), {lists:sort(Vs), non_neg_integer()}),
        begin
            Array = ints_to_array(SortedValues),
            N = length(SortedValues),
            Predicate = fun(<<V:64/unsigned>>) -> V < Threshold end,
            Idx = rabbitmq_stream_s3_array:partition_point(Predicate, ?ARRAY_ENTRY_SIZE, Array),
            Before = lists:all(
                fun(I) -> Predicate(rabbitmq_stream_s3_array:at(I, ?ARRAY_ENTRY_SIZE, Array)) end,
                lists:seq(0, Idx - 1)
            ),
            After = lists:all(
                fun(I) ->
                    not Predicate(rabbitmq_stream_s3_array:at(I, ?ARRAY_ENTRY_SIZE, Array))
                end,
                lists:seq(Idx, N - 1)
            ),
            Before andalso After
        end
    ).

array_binary_search_by(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_array_binary_search_by/0, [], 500).

prop_array_binary_search_by() ->
    ?FORALL(
        {SortedValues, Target},
        ?LET(Vs, list(non_neg_integer()), {lists:usort(Vs), non_neg_integer()}),
        begin
            Array = ints_to_array(SortedValues),
            N = length(SortedValues),
            CmpFn = fun(<<V:64/unsigned>>) ->
                if
                    V =:= Target -> eq;
                    V > Target -> gt;
                    true -> lt
                end
            end,
            Result = rabbitmq_stream_s3_array:binary_search_by(CmpFn, ?ARRAY_ENTRY_SIZE, Array),
            case Result of
                {ok, Idx} ->
                    CmpFn(rabbitmq_stream_s3_array:at(Idx, ?ARRAY_ENTRY_SIZE, Array)) =:= eq;
                {error, Idx} ->
                    Before = lists:all(
                        fun(I) ->
                            CmpFn(rabbitmq_stream_s3_array:at(I, ?ARRAY_ENTRY_SIZE, Array)) =:= lt
                        end,
                        lists:seq(0, Idx - 1)
                    ),
                    After = lists:all(
                        fun(I) ->
                            CmpFn(rabbitmq_stream_s3_array:at(I, ?ARRAY_ENTRY_SIZE, Array)) =:= gt
                        end,
                        lists:seq(Idx, N - 1)
                    ),
                    Before andalso After
            end
        end
    ).

array_rfind(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_array_rfind/0, [], 500).

prop_array_rfind() ->
    ?FORALL(
        {Values, Threshold},
        {list(non_neg_integer()), non_neg_integer()},
        begin
            Array = ints_to_array(Values),
            N = length(Values),
            Predicate = fun(<<V:64/unsigned>>) -> V < Threshold end,
            Result = rabbitmq_stream_s3_array:rfind(Predicate, ?ARRAY_ENTRY_SIZE, Array),
            case Result of
                undefined ->
                    lists:all(
                        fun(I) ->
                            not Predicate(
                                rabbitmq_stream_s3_array:at(I, ?ARRAY_ENTRY_SIZE, Array)
                            )
                        end,
                        lists:seq(0, N - 1)
                    );
                Idx ->
                    AtIdx = Predicate(
                        rabbitmq_stream_s3_array:at(Idx, ?ARRAY_ENTRY_SIZE, Array)
                    ),
                    NoneAfter = lists:all(
                        fun(I) ->
                            not Predicate(
                                rabbitmq_stream_s3_array:at(I, ?ARRAY_ENTRY_SIZE, Array)
                            )
                        end,
                        lists:seq(Idx + 1, N - 1)
                    ),
                    AtIdx andalso NoneAfter
            end
        end
    ).

array_fold(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_array_fold/0, [], 500).

prop_array_fold() ->
    ?FORALL(
        Values,
        list(non_neg_integer()),
        begin
            Array = ints_to_array(Values),
            N = length(Values),
            Folded = rabbitmq_stream_s3_array:fold(
                fun(Entry, Acc) -> Acc ++ [Entry] end, [], ?ARRAY_ENTRY_SIZE, Array
            ),
            Sequential = [
                rabbitmq_stream_s3_array:at(I, ?ARRAY_ENTRY_SIZE, Array)
             || I <- lists:seq(0, N - 1)
            ],
            Folded =:= Sequential
        end
    ).

ints_to_array(Ints) ->
    iolist_to_binary([<<V:64/unsigned>> || V <- Ints]).

%% =========================================================================
%% Range spec properties (FS backend)
%% =========================================================================

range_spec_to_location_number_suffix(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_range_spec_suffix/0, [], 500).

prop_range_spec_suffix() ->
    ?FORALL(
        {FileSize, N},
        ?LET(FS, pos_integer(), {FS, integer(1, FS)}),
        begin
            {Loc, Num} = rabbitmq_stream_s3_api_fs:range_spec_to_location_number(FileSize, -N),
            (Loc =:= FileSize - N) andalso (Num =:= N)
        end
    ).

range_spec_to_location_number_prefix(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_range_spec_prefix/0, [], 500).

prop_range_spec_prefix() ->
    ?FORALL(
        {FileSize, N},
        {pos_integer(), pos_integer()},
        begin
            {Loc, Num} = rabbitmq_stream_s3_api_fs:range_spec_to_location_number(FileSize, N),
            (Loc =:= 0) andalso (Num =:= min(N, FileSize))
        end
    ).

range_spec_to_location_number_byte_range(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_range_spec_byte_range/0, [], 500).

prop_range_spec_byte_range() ->
    ?FORALL(
        {FileSize, Start, MaybeEnd},
        ?LET(
            FS,
            pos_integer(),
            ?LET(S, integer(0, FS - 1), {FS, S, oneof([integer(S, FS + 100), undefined])})
        ),
        begin
            {Loc, Num} = rabbitmq_stream_s3_api_fs:range_spec_to_location_number(
                FileSize, {Start, MaybeEnd}
            ),
            Expected =
                case MaybeEnd of
                    undefined -> FileSize - Start;
                    End -> End - Start + 1
                end,
            (Loc =:= Start) andalso (Num =:= Expected)
        end
    ).

%% =========================================================================
%% Fragment and index lookup properties
%% =========================================================================

find_fragment_timestamp(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_find_fragment_timestamp/0, [], 500).

prop_find_fragment_timestamp() ->
    ?FORALL(
        Fragments,
        gen_fragments(),
        begin
            Entries = fragments_to_entries(Fragments),
            GetGroup = fun(_) -> erlang:error(no_groups) end,
            FindFragment = fun(Ts) ->
                {ok, #fragment_ref{offset = Offset}} = rabbitmq_stream_s3_log_reader:find_fragment(
                    Entries, {timestamp, Ts}, GetGroup
                ),
                Offset
            end,
            WithinRange = lists:all(
                fun({Offset, FTs, LTs}) ->
                    %% A timestamp anywhere within a fragment, including both
                    %% boundaries, resolves to that fragment. The last_ts case
                    %% is the boundary one: the chunk at last_ts lives in this
                    %% fragment, so the seek must not advance to the next.
                    Mid = FTs + (LTs - FTs) div 2,
                    FindFragment(FTs) =:= Offset andalso
                        FindFragment(Mid) =:= Offset andalso
                        FindFragment(LTs) =:= Offset
                end,
                Fragments
            ),
            InGap = lists:all(
                fun({{_O1, _FTs1, LTs1}, {O2, FTs2, _LTs2}}) ->
                    %% Only meaningful when a timestamp lies strictly between the
                    %% two fragments. Such a timestamp snaps to the later
                    %% fragment, the right-boundary preference for timestamps.
                    case LTs1 + 1 < FTs2 of
                        false ->
                            true;
                        true ->
                            Ts = LTs1 + (FTs2 - LTs1) div 2,
                            FindFragment(Ts) =:= O2
                    end
                end,
                lists:zip(lists:droplast(Fragments), tl(Fragments))
            ),
            {LastOffset, _FTs, LastLTs} = lists:last(Fragments),
            AfterAll = FindFragment(LastLTs + 1000) =:= LastOffset,
            WithinRange andalso InGap andalso AfterAll
        end
    ).

gen_fragments() ->
    ?LET(
        {N, Gaps},
        {integer(1, 20), non_empty(list(boolean()))},
        begin
            GapsN = lists:sublist(Gaps ++ lists:duplicate(N, false), N),
            {Frags, _, _} = lists:foldl(
                fun(HasGap, {Acc, NextOffset, NextTs}) ->
                    FTs = NextTs,
                    LTs = FTs + 100,
                    Gap =
                        case HasGap of
                            true -> 50;
                            false -> 0
                        end,
                    {[{NextOffset, FTs, LTs} | Acc], NextOffset + 20, LTs + Gap + 1}
                end,
                {[], 0, 1_000_000},
                GapsN
            ),
            lists:reverse(Frags)
        end
    ).

fragments_to_entries(Fragments) ->
    iolist_to_binary([
        ?ENTRY(O, FTs, LTs, ?MANIFEST_KIND_FRAGMENT, 200, 0)
     || {O, FTs, LTs} <- Fragments
    ]).

find_index_position_offset(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_find_index_position_offset/0, [], 500).

prop_find_index_position_offset() ->
    ?FORALL(
        {Chunks, QueryOffset},
        gen_index_data(),
        begin
            IndexData = chunks_to_index(Chunks),
            {ChunkId, _Ts, _Pos} = rabbitmq_stream_s3_log_reader:find_index_position(
                IndexData, {offset, QueryOffset}
            ),
            Offsets = [O || {O, _T} <- Chunks],
            Expected =
                case lists:takewhile(fun(O) -> O =< QueryOffset end, Offsets) of
                    [] -> hd(Offsets);
                    Matching -> lists:last(Matching)
                end,
            ChunkId =:= Expected
        end
    ).

find_index_position_timestamp(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_find_index_position_timestamp/0, [], 500).

prop_find_index_position_timestamp() ->
    ?FORALL(
        {Chunks, QueryTs},
        gen_index_data(),
        begin
            IndexData = chunks_to_index(Chunks),
            {ChunkId, _Ts, _Pos} = rabbitmq_stream_s3_log_reader:find_index_position(
                IndexData, {timestamp, QueryTs}
            ),
            Expected =
                case lists:dropwhile(fun({_O, Ts}) -> Ts < QueryTs end, Chunks) of
                    [] -> element(1, lists:last(Chunks));
                    [{O, _T} | _] -> O
                end,
            ChunkId =:= Expected
        end
    ).

gen_index_data() ->
    ?LET(
        {N, Steps},
        {integer(1, 20), non_empty(list(integer(1, 100)))},
        begin
            StepsN = lists:sublist(Steps ++ lists:duplicate(N, 1), N),
            {Chunks, LastOffset, LastTs} = lists:foldl(
                fun(Step, {Acc, NextOffset, NextTs}) ->
                    {[{NextOffset, NextTs} | Acc], NextOffset + Step, NextTs + Step}
                end,
                {[], 1, 1_000_000},
                StepsN
            ),
            SortedChunks = lists:reverse(Chunks),
            ?LET(Query, integer(0, max(LastOffset, LastTs) + 200), {SortedChunks, Query})
        end
    ).

chunks_to_index(Chunks) ->
    iolist_to_binary([
        ?INDEX_RECORD(O, Ts, Pos)
     || {Pos, {O, Ts}} <- lists:zip(lists:seq(0, length(Chunks) - 1), Chunks)
    ]).

%% =========================================================================
%% Remote reader core properties
%% =========================================================================

remote_reader_core_no_crash(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_remote_reader_core_no_crash/0, [], 500).

prop_remote_reader_core_no_crash() ->
    ?FORALL(
        Events,
        gen_rrc_event_sequence(),
        begin
            FragRef = #fragment_ref{offset = 0, uid = 1, size = 1_000_000},
            Manifest = #manifest{
                first_offset = 0,
                next_offset = 200,
                entries = ?ENTRY(0, 0, 0, ?MANIFEST_KIND_FRAGMENT, 1_000_000, 1)
            },
            GetGroupFun = fun(_) -> {error, not_found} end,
            Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroupFun),
            Iterator =
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
                    {ok, _, It} -> It;
                    _ -> Iterator0
                end,
            {State0, _} = rabbitmq_stream_s3_remote_reader_core:init(
                <<"prop-stream">>, FragRef, 8, Iterator, #{}
            ),
            %% Run all events. Must not crash.
            try
                _ = run_rrc_events(Events, State0),
                true
            catch
                C:R:St ->
                    file:write_file(
                        "/tmp/prop_crash.txt",
                        io_lib:format("~p:~p~nEvents: ~w~nStack: ~p~n", [C, R, Events, St])
                    ),
                    false
            end
        end
    ).

remote_reader_core_reply_size_bounded(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_remote_reader_core_reply_size_bounded/0, [], 500).

prop_remote_reader_core_reply_size_bounded() ->
    ?FORALL(
        {ReadOffset, ReadBytes, DataSize},
        {range(8, 500), range(1, 1000), range(100, 5000)},
        begin
            FragSize = 1_000_000,
            FragRef = #fragment_ref{offset = 0, uid = 1, size = FragSize},
            Manifest = #manifest{
                first_offset = 0,
                next_offset = 200,
                entries = ?ENTRY(0, 0, 0, ?MANIFEST_KIND_FRAGMENT, FragSize, 1)
            },
            GetGroupFun = fun(_) -> {error, not_found} end,
            Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroupFun),
            Iterator =
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
                    {ok, _, It} -> It;
                    _ -> Iterator0
                end,
            {S0, _} = rabbitmq_stream_s3_remote_reader_core:init(
                <<"prop-stream">>, FragRef, 8, Iterator, #{}
            ),
            %% Provide data.
            Data = binary:copy(<<0>>, DataSize),
            {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(
                S0, {data, rrc_id(S0, 0, 8), Data, done}
            ),
            %% Issue a read.
            {_S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                S1, {read, ReadOffset, ReadBytes, chunk_boundary}
            ),
            %% If a reply was produced, its data must be <= ReadBytes.
            case [iolist_to_binary(D) || {reply, {ok, D}} <- Effects] of
                [] -> true;
                [ReplyData] -> byte_size(ReplyData) =< ReadBytes
            end
        end
    ).

%% ------------------------------------------------------------------
%% Remote reader core generators
%% ------------------------------------------------------------------

gen_rrc_event_sequence() ->
    ?LET(N, range(1, 20), gen_rrc_events(N, 8)).

gen_rrc_events(0, _NextReadPos) ->
    [];
gen_rrc_events(N, NextReadPos) ->
    ?LET(
        {Event, NextPos},
        gen_rrc_event(NextReadPos),
        ?LET(Rest, gen_rrc_events(N - 1, NextPos), [Event | Rest])
    ).

gen_rrc_event(NextReadPos) ->
    frequency([
        {3, gen_rrc_read_event(NextReadPos)},
        {3, ?LET(E, gen_rrc_data_event(), {E, NextReadPos})},
        {2, ?LET(E, gen_rrc_error_event(), {E, NextReadPos})},
        {1, ?LET(Kind, oneof([fault, pool_busy]), {{retry, Kind}, NextReadPos})},
        {1, ?LET(E, gen_rrc_iterator_refreshed_event(), {E, NextReadPos})},
        {1, {deadline_expired, NextReadPos}}
    ]).

gen_rrc_read_event(NextReadPos) ->
    ?LET(
        Bytes,
        range(1, 5000),
        {{read, NextReadPos, Bytes, chunk_boundary}, NextReadPos + Bytes}
    ).

%% Requests are addressed by the range they were issued for, which the
%% generator cannot know, so deliveries and failures are emitted as markers and
%% bound to an actual outstanding range when the sequence runs. `Which` picks
%% among the outstanding ranges, so responses arrive out of issue order.
gen_rrc_data_event() ->
    ?LET(
        {Size, Which},
        {range(1, 10000), range(0, 7)},
        {deliver, Which, binary:copy(<<0>>, Size), oneof([done, continue])}
    ).

gen_rrc_error_event() ->
    ?LET(
        {Reason, Which},
        {
            oneof([timeout, slow_down, connection_error, stream_error, internal_error, pool_busy]),
            range(0, 7)
        },
        {fail, Which, Reason}
    ).

gen_rrc_iterator_refreshed_event() ->
    oneof([
        {iterator_refreshed, end_of_manifest},
        ?LET(
            {Offset, Size, Uid},
            {range(0, 500), range(100_000, 5_000_000), range(1, 16#FFFFFFFF)},
            begin
                Manifest = #manifest{
                    first_offset = Offset,
                    next_offset = Offset + 1,
                    entries = ?ENTRY(Offset, 0, 0, ?MANIFEST_KIND_FRAGMENT, Size, Uid)
                },
                GetGroupFun = fun(_) -> {error, not_found} end,
                Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(
                    Manifest, Offset, GetGroupFun
                ),
                Iterator =
                    case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
                        {ok, _, It} -> It;
                        _ -> Iterator0
                    end,
                {iterator_refreshed, Iterator}
            end
        )
    ]).

run_rrc_events([], State) ->
    State;
run_rrc_events([{deliver, Which, Data, DoneOrContinue} | Rest], State0) ->
    State =
        case pick_rrc_range(Which, State0) of
            none ->
                State0;
            {Fragment, Start} ->
                {State1, _} = rabbitmq_stream_s3_remote_reader_core:step(
                    State0, {data, rrc_id(State0, Fragment, Start), Data, DoneOrContinue}
                ),
                State1
        end,
    run_rrc_events(Rest, State);
run_rrc_events([{fail, Which, Reason} | Rest], State0) ->
    State =
        case pick_rrc_range(Which, State0) of
            none ->
                State0;
            {Fragment, Start} ->
                {State1, _} = rabbitmq_stream_s3_remote_reader_core:step(
                    State0, {request_error, rrc_id(State0, Fragment, Start), Fragment, Reason}
                ),
                State1
        end,
    run_rrc_events(Rest, State);
run_rrc_events([Event | Rest], State0) ->
    {State1, _} = rabbitmq_stream_s3_remote_reader_core:step(State0, Event),
    run_rrc_events(Rest, State1).

%% Fire both retry timers. Each backoff kind releases only the ranges queued
%% against it, so a run that mixes S3 faults with pool_busy has to fire both to
%% put every failed range back in flight.
rrc_retry_all(State0) ->
    lists:foldl(
        fun(Kind, Acc) ->
            {Acc1, _} = rabbitmq_stream_s3_remote_reader_core:step(Acc, {retry, Kind}),
            Acc1
        end,
        State0,
        [fault, pool_busy]
    ).

pick_rrc_range(Which, State) ->
    case rabbitmq_stream_s3_remote_reader_core:outstanding_ranges(State) of
        [] ->
            none;
        Ranges ->
            {Fragment, Start, _End} = lists:nth(Which rem length(Ranges) + 1, Ranges),
            {Fragment, Start}
    end.

%% Every reply carries exactly the bytes of the stream at the read offset,
%% whatever order the concurrent range requests are answered in. Data content is
%% a function of absolute position, so any bookkeeping error - a block dropped
%% too eagerly, an off-by-one in block slicing, a staged block flushed at the
%% wrong offset, a hole papered over after a short response - surfaces as a
%% content mismatch rather than only a size mismatch. This is the property that
%% covers reassembly: with several ranges of a fragment in flight, responses
%% interleave, and the buffer only ever accepts contiguous appends.
remote_reader_core_reply_bytes_exact(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_remote_reader_core_reply_bytes_exact/0, [], 500).

prop_remote_reader_core_reply_bytes_exact() ->
    ?FORALL(
        {Steps, RequestSize, MaxDepth},
        {list({range(0, 7), range(1, 4000), range(0, 99)}), range(64, 4000), range(1, 8)},
        begin
            FragSize = 100_000_000,
            FragRef = #fragment_ref{offset = 0, uid = 1, size = FragSize},
            Manifest = #manifest{
                first_offset = 0,
                next_offset = 200,
                entries = ?ENTRY(0, 0, 0, ?MANIFEST_KIND_FRAGMENT, FragSize, 1)
            },
            GetGroupFun = fun(_) -> {error, not_found} end,
            Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroupFun),
            Iterator =
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
                    {ok, _, It} -> It;
                    _ -> Iterator0
                end,
            Opts = #{
                request_size => RequestSize,
                window_max => RequestSize * 8,
                max_depth => MaxDepth
            },
            {S0, _} = rabbitmq_stream_s3_remote_reader_core:init(
                <<"prop-stream">>, FragRef, 8, Iterator, Opts
            ),
            run_rrc_exact_steps(Steps, S0, 8, 8)
        end
    ).

run_rrc_exact_steps([], _State, _DataEnd, _ReadFloor) ->
    true;
run_rrc_exact_steps([{Rot, LenWant, PosFrac} | Steps], S0, DataEnd0, ReadFloor) ->
    %% Answer every outstanding range, rotated so the responses land in a
    %% different order from the one they were issued in.
    Outstanding = [{Start, End} || {0, Start, End} <- outstanding_rrc_ranges(S0)],
    {S1, DataEnd} = answer_rrc_ranges(rotate(Rot, Outstanding), S0, DataEnd0),
    case DataEnd > ReadFloor of
        false ->
            run_rrc_exact_steps(Steps, S1, DataEnd, ReadFloor);
        true ->
            %% Read at or past the previous read (reads are non-decreasing) and
            %% within what has been delivered, so a reply is guaranteed.
            ReadPos = ReadFloor + ((DataEnd - 1 - ReadFloor) * PosFrac) div 100,
            ReadLen = max(1, min(LenWant, DataEnd - ReadPos)),
            {S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                S1, {read, ReadPos, ReadLen, chunk_boundary}
            ),
            case [iolist_to_binary(D) || {reply, {ok, D}} <- Effects] of
                [Reply] ->
                    Reply =:= rb_pattern(ReadPos, ReadLen) andalso
                        run_rrc_exact_steps(Steps, S2, DataEnd, ReadPos);
                _ ->
                    false
            end
    end.

%% Answer each range in full, alternating the closing frame: every other range
%% is delivered with `continue`, so it has handed over every byte it owes while
%% its response stays open. Those bytes must still reach the buffer, and the
%% requests behind such a range must still be able to flush - on the next step
%% and every step after it, not just the one it was delivered on. Answering
%% only with `done` never builds that state, and it is the state in which the
%% reassembly queue can wedge: the caller's read then goes unserved, which this
%% property's `_ -> false` turns into a counterexample.
answer_rrc_ranges(Ranges, State, DataEnd) ->
    {Acc, MaxEnd, _} = lists:foldl(
        fun({Start, End}, {StateAcc, MaxEnd0, N}) ->
            Data = rb_pattern(Start, End - Start + 1),
            DoneOrContinue =
                case N rem 2 of
                    0 -> done;
                    1 -> continue
                end,
            {StateAcc1, _} = rabbitmq_stream_s3_remote_reader_core:step(
                StateAcc, {data, rrc_id(StateAcc, 0, Start), Data, DoneOrContinue}
            ),
            {StateAcc1, max(MaxEnd0, End + 1), N + 1}
        end,
        {State, DataEnd, 0},
        Ranges
    ),
    {Acc, MaxEnd}.

%% The core addresses a request by the id the pipeline minted for it; a property
%% knows which range S3 is answering. An id it cannot match is dropped as stale,
%% which is the right outcome for a range that has left the queue.
rrc_id(State, Fragment, Start) ->
    case rabbitmq_stream_s3_remote_reader_core:request_id(State, Fragment, Start) of
        {ok, Id} -> Id;
        error -> 0
    end.

outstanding_rrc_ranges(State) ->
    rabbitmq_stream_s3_remote_reader_core:outstanding_ranges(State).

rotate(_N, []) ->
    [];
rotate(N, List) ->
    {Head, Tail} = lists:split(N rem length(List), List),
    Tail ++ Head.

%% The same exactness guarantee as `remote_reader_core_reply_bytes_exact`, but
%% where every outstanding range may also fail, close early, or close without
%% closing its range - and where those outcomes interleave with successes across
%% a full pipeline. Failure and success are not independent: a range that fails
%% is put back at the byte its bytes reached, which rewrites its key, and its
%% neighbours keep streaming into the queue it re-enters. That is what makes the
%% combination worth generating rather than each outcome on its own.
%%
%% Two things are asserted after every event:
%%
%%  1. The request queue is well formed - no range runs backwards, no two
%%     requests share a key, and each fragment's ranges stay sorted and
%%     disjoint. A key collision is not a local error: requests are addressed by
%%     `{fragment, range_start}`, so two requests sharing one means deliveries
%%     are routed to the wrong request and silently dropped.
%%  2. Any bytes replied to a read are the stream's bytes at that position.
%%
%% and one thing at the end of the run: the core is still alive, in the sense
%% that answering everything it is waiting on drains its queue and gets the
%% pending read served. See `rrc_drains/2` for why a liveness oracle is needed
%% on top of the two above.
remote_reader_core_survives_failure_interleavings(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_remote_reader_core_survives_failure_interleavings/0, [], 500
    ).

prop_remote_reader_core_survives_failure_interleavings() ->
    ?FORALL(
        {Steps, RequestSize, MaxDepth, ProbeLen},
        {list(gen_rrc_failure_step()), range(64, 4000), range(1, 8), range(1, 5000)},
        begin
            FragSize = 100_000_000,
            FragRef = #fragment_ref{offset = 0, uid = 1, size = FragSize},
            Manifest = #manifest{
                first_offset = 0,
                next_offset = 200,
                entries = ?ENTRY(0, 0, 0, ?MANIFEST_KIND_FRAGMENT, FragSize, 1)
            },
            GetGroupFun = fun(_) -> {error, not_found} end,
            Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroupFun),
            Iterator =
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
                    {ok, _, It} -> It;
                    _ -> Iterator0
                end,
            Opts = #{
                request_size => RequestSize,
                window_max => RequestSize * 8,
                max_depth => MaxDepth
            },
            {S0, _} = rabbitmq_stream_s3_remote_reader_core:init(
                <<"prop-stream">>, FragRef, 8, Iterator, Opts
            ),
            rrc_queue_well_formed(S0) andalso
                run_rrc_failure_steps(Steps, S0, 8, {ProbeLen, RequestSize})
        end
    ).

%% One step answers every range outstanding at its start, rotated so responses
%% land out of issue order, then reads. `Outcomes` is cycled over the ranges, so
%% a step mixes successes and failures rather than doing one thing to all of
%% them.
gen_rrc_failure_step() ->
    {range(0, 7), non_empty(list(gen_rrc_outcome())), range(1, 4000)}.

gen_rrc_outcome() ->
    frequency([
        %% The whole range, properly closed.
        {5, done},
        %% The whole range, but the closing frame never arrives. The request
        %% stays in flight owing nothing, which is the state a failure has to
        %% handle without rewriting the range backwards.
        {2, continue},
        %% A response that ends before its range does.
        {2, ?LET(Frac, range(0, 100), {short, Frac})},
        {3, ?LET(R, oneof([timeout, slow_down, connection_error, pool_busy]), {fail, R})},
        {2, skip}
    ]).

run_rrc_failure_steps([], State, ReadFloor, Probe) ->
    rrc_drains(State, ReadFloor, Probe);
run_rrc_failure_steps([{Rot, Outcomes, LenWant} | Steps], S0, ReadFloor, Probe) ->
    %% Release anything queued for retry so failed ranges are re-issued rather
    %% than accumulating in `backoff` for the rest of the run.
    S1 = rrc_retry_all(S0),
    Ranges = rotate(Rot, [{Start, End} || {0, Start, End} <- outstanding_rrc_ranges(S1)]),
    case apply_rrc_outcomes(Ranges, Outcomes, S1) of
        false ->
            false;
        {ok, S2} ->
            {S3, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                S2, {read, ReadFloor, LenWant, chunk_boundary}
            ),
            case rrc_queue_well_formed(S3) of
                false ->
                    false;
                true ->
                    case [iolist_to_binary(D) || {reply, {ok, D}} <- Effects] of
                        [] ->
                            run_rrc_failure_steps(Steps, S3, ReadFloor, Probe);
                        [Reply] ->
                            Reply =:= rb_pattern(ReadFloor, byte_size(Reply)) andalso
                                run_rrc_failure_steps(
                                    Steps, S3, ReadFloor + byte_size(Reply), Probe
                                )
                    end
            end
    end.

%% Liveness. Everything above this point is a safety property: it says what the
%% core must not do, and a core that has stopped doing anything at all passes
%% every one of them. A wedged reassembly queue is exactly that shape - the
%% ranges stay well formed, the load stays inside the window, no reply is ever
%% wrong because no reply is ever produced - so it needs an oracle that asks for
%% progress rather than for the absence of a mistake.
%%
%% The oracle: stop injecting faults and answer, in full and properly closed,
%% every range the core is waiting on, round after round. That is the friendliest
%% environment a reader can be in, and S3 has now told it everything it asked to
%% know, so two things must follow. The queue must drain to empty - a request
%% that no longer waits on any byte must leave it - and the consumer's next read
%% must be served. Neither holds if some request at the head of the queue can no
%% longer flush: the rounds then answer the same ranges forever and the read is
%% never served.
%%
%% The rounds are capped so a wedge fails the property instead of hanging the
%% suite. The cap is generous: with no reads to advance the consumer, the buffer
%% fills to the prefetch window within a few rounds and the core stops issuing,
%% so a healthy queue empties in about `window_max / request_size` of them.
-define(RRC_DRAIN_ROUNDS, 64).

rrc_drains(State0, ReadFloor, {ProbeLen, RequestSize}) ->
    case rrc_drain(State0, ?RRC_DRAIN_ROUNDS) of
        false ->
            false;
        {ok, State0b} ->
            {State, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                State0b, {read, ReadFloor, ProbeLen, chunk_boundary}
            ),
            rrc_await_reply(
                State, ReadFloor, ProbeLen, Effects, await_rounds(ProbeLen, RequestSize)
            )
    end.

%% How many rounds the post-drain read is allowed. A round answers every range
%% outstanding at its start, so with `max_depth` at 1 it moves one request size,
%% and a read is served only once every byte below it has been fetched: the
%% rounds needed are the read's own length in requests, plus the window the
%% reader fills ahead of it. A fixed cap cannot cover that - at the smallest
%% request size the generator produces, a long read needs more rounds than a
%% short one is anywhere near, and the property failed a core that was serving
%% the read correctly, one round at a time.
await_rounds(ProbeLen, RequestSize) ->
    %% `window_max` is 8 requests in this fixture; the slack is for the miss
    %% that widens the window and the round the reply itself lands in.
    ProbeLen div RequestSize + 8 + 8.

%% The post-drain read has to be served eventually, not within the one step that
%% issues it. Draining leaves no read pending, so the core has no reason to have
%% fetched past its prefetch window; a read the buffer cannot satisfy then costs
%% a fetch round-trip, and the miss it takes is exactly what widens the window
%% and issues the range that will serve it. Demanding the reply in the first
%% step called that legitimate warm-up a wedge.
%%
%% The teeth are kept. The read stays pending across the rounds, so the reply
%% arrives with whichever delivery completes it; a core that is genuinely stuck
%% either asks for nothing while still owing a reply - which is the wedge, and
%% is failed on the spot - or keeps asking forever and runs out of rounds.
rrc_await_reply(_State, _ReadFloor, _ProbeLen, _Effects, 0) ->
    false;
rrc_await_reply(State0, ReadFloor, ProbeLen, Effects, Rounds) ->
    case [iolist_to_binary(D) || {reply, {ok, D}} <- Effects] of
        [Reply] ->
            Reply =:= rb_pattern(ReadFloor, ProbeLen);
        [] ->
            State1 = rrc_retry_all(State0),
            case outstanding_rrc_ranges(State1) of
                [] ->
                    %% A read it cannot serve and nothing left to answer: the
                    %% core has stopped.
                    false;
                Ranges ->
                    {State, Emitted} = lists:foldl(
                        fun({Fragment, Start, End}, {Acc, EffAcc}) ->
                            Data = rb_pattern(Start, End - Start + 1),
                            {Acc1, Eff} = rabbitmq_stream_s3_remote_reader_core:step(
                                Acc, {data, rrc_id(Acc, Fragment, Start), Data, done}
                            ),
                            {Acc1, EffAcc ++ Eff}
                        end,
                        {State1, []},
                        Ranges
                    ),
                    rrc_queue_well_formed(State) andalso
                        rrc_await_reply(State, ReadFloor, ProbeLen, Emitted, Rounds - 1)
            end
    end.

rrc_drain(_State, 0) ->
    %% Still waiting on ranges after every one of them has been answered
    %% repeatedly: the queue cannot drain.
    false;
rrc_drain(State0, Rounds) ->
    State1 = rrc_retry_all(State0),
    case outstanding_rrc_ranges(State1) of
        [] ->
            {ok, State1};
        Ranges ->
            State = lists:foldl(
                fun({Fragment, Start, End}, Acc) ->
                    Data = rb_pattern(Start, End - Start + 1),
                    {Acc1, _} = rabbitmq_stream_s3_remote_reader_core:step(
                        Acc, {data, rrc_id(Acc, Fragment, Start), Data, done}
                    ),
                    Acc1
                end,
                State1,
                Ranges
            ),
            case rrc_queue_well_formed(State) of
                true -> rrc_drain(State, Rounds - 1);
                false -> false
            end
    end.

apply_rrc_outcomes([], _Outcomes, State) ->
    {ok, State};
apply_rrc_outcomes([{Start, End} | Ranges], [Outcome | Rest], State0) ->
    State = apply_rrc_outcome(Outcome, Start, End, State0),
    case rrc_queue_well_formed(State) of
        true -> apply_rrc_outcomes(Ranges, Rest ++ [Outcome], State);
        false -> false
    end.

apply_rrc_outcome(skip, _Start, _End, State) ->
    State;
apply_rrc_outcome({fail, Reason}, Start, _End, State0) ->
    {State, _} = rabbitmq_stream_s3_remote_reader_core:step(
        State0, {request_error, rrc_id(State0, 0, Start), 0, Reason}
    ),
    State;
apply_rrc_outcome({short, Frac}, Start, End, State0) ->
    Len = ((End - Start + 1) * Frac) div 100,
    {State, _} = rabbitmq_stream_s3_remote_reader_core:step(
        State0, {data, rrc_id(State0, 0, Start), rb_pattern(Start, Len), done}
    ),
    State;
apply_rrc_outcome(DoneOrContinue, Start, End, State0) ->
    Data = rb_pattern(Start, End - Start + 1),
    {State, _} = rabbitmq_stream_s3_remote_reader_core:step(
        State0, {data, rrc_id(State0, 0, Start), Data, DoneOrContinue}
    ),
    State.

%% Ranges run forwards, keys are unique, and each fragment's ranges are sorted
%% and disjoint - the ordering the request queue documents, and what addressing
%% requests by `{fragment, range_start}` depends on.
%%
%% Sorted and disjoint, not contiguous: a request that has delivered every byte
%% it owes but whose closing frame has not arrived stays in the queue while the
%% requests behind it are dropped, so a gap the buffer already holds can open up
%% between two queued ranges.
rrc_queue_well_formed(State) ->
    Ranges = outstanding_rrc_ranges(State),
    Keys = [{F, S} || {F, S, _} <- Ranges],
    lists:all(fun({_, S, E}) -> S =< E end, Ranges) andalso
        length(lists:usort(Keys)) =:= length(Keys) andalso
        rrc_ranges_disjoint(Ranges).

rrc_ranges_disjoint([{F, _, End}, {F, Start, _} = Next | Rest]) ->
    Start > End andalso rrc_ranges_disjoint([Next | Rest]);
rrc_ranges_disjoint([_ | Rest]) ->
    rrc_ranges_disjoint(Rest);
rrc_ranges_disjoint([]) ->
    true.

%% The prefetch window and the depth cap are the reader's memory bound. Neither
%% may be exceeded by any sequence of events: a range that is put back after a
%% failure is already accounted for, so re-issuing it must not double-count, and
%% a short response must not leave bytes outstanding that nothing tracks.
%%
%% The one thing that may lift the window is the read in hand. A read of N bytes
%% cannot be served while fewer than N are outstanding, so the ceiling rises to
%% whatever the pending read needs (see `fetch_ceiling/1` in the core) - the
%% alternative is refusing to fetch and never serving it. That is bounded by the
%% largest read in the sequence, so the property still has teeth: nothing else
%% may push the reader past its window.
%% Every property above this one drives a single-fragment manifest whose group
%% fun returns `{error, not_found}`, so the look-ahead can only ever answer
%% `end_of_manifest`: `next_peek = failed` - the state a transient group fetch
%% leaves behind, and the state the reader has to climb back out of - was
%% unreachable in all 500 iterations of each of them. That is why a stranded
%% memo survived several reviews. This fixture puts every fragment after the
%% first behind a group node and lets the generator decide how many of those
%% fetches fail, so the region is reachable and the invariants checked inside
%% `step/2` apply to it like anywhere else.
%%
%% The oracle is the recovery, because the failure is not a crash: a reader
%% carrying a stranded memo keeps serving the fragment it is on and stops only
%% prefetching, which every safety property here is happy with. So the second
%% phase turns S3 healthy, expires the read - the event that used to disown the
%% clock and strand the memo with it - and then only answers and reads. No retry
%% event is injected, deliberately: with no clock armed and none coming, a
%% reader that cannot re-attempt the fetch on its own never asks for the next
%% fragment again, and the rounds run out.
remote_reader_core_look_ahead_recovers(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_remote_reader_core_look_ahead_recovers/0, [], 500).

prop_remote_reader_core_look_ahead_recovers() ->
    ?FORALL(
        {Events, GroupFailures},
        {gen_rrc_event_sequence(), range(0, 8)},
        begin
            Failures = counters:new(1, []),
            counters:add(Failures, 1, GroupFailures),
            FragSize = 2000,
            Fragments = [{Offset, FragSize, Offset + 1} || Offset <- lists:seq(0, 700, 100)],
            Iterator = rrc_grouped_iterator(Fragments, Failures),
            [{Offset, _, Uid} | _] = Fragments,
            FragRef = #fragment_ref{offset = Offset, uid = Uid, size = FragSize},
            Opts = #{request_size => 1000, window_max => 8000, max_depth => 4},
            {S0, _} = rabbitmq_stream_s3_remote_reader_core:init(
                <<"prop-stream">>, FragRef, ?SEGMENT_HEADER_B, Iterator, Opts
            ),
            %% Phase 1: chaos, with group fetches failing while the budget
            %% lasts. Iterator refreshes are dropped: the generator builds them
            %% around a single-fragment manifest of its own, which would replace
            %% this fixture with one that has nothing to look ahead to.
            S1 = run_rrc_events([E || E <- Events, not is_rrc_refresh(E)], S0),
            %% Phase 2: S3 is healthy and the read the shell was waiting on has
            %% expired.
            counters:put(Failures, 1, 0),
            {S2, _} = rabbitmq_stream_s3_remote_reader_core:step(S1, deadline_expired),
            ReadFloor = rabbitmq_stream_s3_remote_reader_core:read_position(S2),
            rrc_looks_ahead_again(S2, ReadFloor, ?RRC_DRAIN_ROUNDS)
        end
    ).

is_rrc_refresh({iterator_refreshed, _}) -> true;
is_rrc_refresh(_Event) -> false.

%% A round answers everything outstanding and reads, which is what walks the
%% consumer to the end of the fragment and puts the frontier where it has to
%% look ahead. The reader has recovered as soon as it asks for a fragment other
%% than the one it is on, or reaches the end of the manifest.
rrc_looks_ahead_again(_State, _ReadFloor, 0) ->
    false;
rrc_looks_ahead_again(State0, ReadFloor, Rounds) ->
    Current = rabbitmq_stream_s3_remote_reader_core:current_fragment_offset(State0),
    case [F || {F, _, _} <- outstanding_rrc_ranges(State0), F =/= Current] of
        [_ | _] ->
            true;
        [] ->
            State1 = lists:foldl(
                fun({Fragment, Start, End}, Acc) ->
                    {Acc1, _} = rabbitmq_stream_s3_remote_reader_core:step(
                        Acc,
                        {data, rrc_id(Acc, Fragment, Start), rb_pattern(Start, End - Start + 1),
                            done}
                    ),
                    Acc1
                end,
                State0,
                outstanding_rrc_ranges(State0)
            ),
            {State, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                State1, {read, ReadFloor, 500, chunk_boundary}
            ),
            case [R || {reply, R} <- Effects] of
                [{become_local, _}] ->
                    %% The manifest ended: there is no next fragment to ask for.
                    true;
                [{next_fragment, _}] ->
                    rrc_looks_ahead_again(
                        State,
                        rabbitmq_stream_s3_remote_reader_core:read_position(State),
                        Rounds - 1
                    );
                [{ok, Data}] ->
                    rrc_looks_ahead_again(State, ReadFloor + iolist_size(Data), Rounds - 1);
                _ ->
                    rrc_looks_ahead_again(State, ReadFloor, Rounds - 1)
            end
    end.

%% A manifest whose first fragment is a direct entry and whose every later one
%% sits behind a group node, so looking one fragment ahead always costs a group
%% fetch - the synchronous S3 GET the look-ahead memo exists to avoid repeating.
%% The first `Failures` of those fetches fail transiently; the counter is the
%% side channel because the fun is called from inside the core, not by the test.
rrc_grouped_iterator([{FirstOffset, FirstSize, FirstUid} | Rest], Failures) ->
    Groups = <<
        <<(?ENTRY(Offset, 0, 0, ?MANIFEST_KIND_GROUP, 0, Uid))/binary>>
     || {Offset, _, Uid} <- Rest
    >>,
    Manifest = #manifest{
        first_offset = FirstOffset,
        next_offset = element(1, lists:last(Rest)) + 100,
        entries =
            <<
                (?ENTRY(FirstOffset, 0, 0, ?MANIFEST_KIND_FRAGMENT, FirstSize, FirstUid))/binary,
                Groups/binary
            >>
    },
    GetGroupFun = fun(#group_ref{offset = Offset}) ->
        case counters:get(Failures, 1) of
            0 ->
                [{_, Size, Uid}] = [E || {O, _, _} = E <- Rest, O =:= Offset],
                {ok, ?ENTRY(Offset, 0, 0, ?MANIFEST_KIND_FRAGMENT, Size, Uid)};
            _ ->
                counters:sub(Failures, 1, 1),
                {error, slow_down}
        end
    end,
    Iterator = rabbitmq_stream_s3_fragment_iterator:init(Manifest, FirstOffset, GetGroupFun),
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator) of
        {ok, _, Advanced} -> Advanced;
        _ -> Iterator
    end.

remote_reader_core_load_bounded(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_remote_reader_core_load_bounded/0, [], 500).

prop_remote_reader_core_load_bounded() ->
    ?FORALL(
        {Events, RequestSize, MaxDepth},
        {gen_rrc_event_sequence(), range(64, 4000), range(1, 8)},
        begin
            FragSize = 1_000_000,
            FragRef = #fragment_ref{offset = 0, uid = 1, size = FragSize},
            Manifest = #manifest{
                first_offset = 0,
                next_offset = 200,
                entries = ?ENTRY(0, 0, 0, ?MANIFEST_KIND_FRAGMENT, FragSize, 1)
            },
            GetGroupFun = fun(_) -> {error, not_found} end,
            Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, 0, GetGroupFun),
            Iterator =
                case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
                    {ok, _, It} -> It;
                    _ -> Iterator0
                end,
            WindowMax = RequestSize * 8,
            Opts = #{
                request_size => RequestSize, window_max => WindowMax, max_depth => MaxDepth
            },
            {S0, _} = rabbitmq_stream_s3_remote_reader_core:init(
                <<"prop-stream">>, FragRef, 8, Iterator, Opts
            ),
            %% `read_pos` is never negative, so the most any pending read can
            %% ask for is the furthest byte any read in the sequence reaches.
            MaxReadEnd = lists:max([0 | [O + B || {read, O, B, _} <- Events]]),
            MaxOutstanding = max(WindowMax, MaxReadEnd) + RequestSize,
            check_rrc_load(Events, S0, MaxOutstanding, MaxDepth)
        end
    ).

check_rrc_load([], _State, _MaxOutstanding, _MaxDepth) ->
    true;
check_rrc_load([Event | Rest], State0, MaxOutstanding, MaxDepth) ->
    State = run_rrc_events([Event], State0),
    {Outstanding, InFlight} = rabbitmq_stream_s3_remote_reader_core:load(State),
    Outstanding =< MaxOutstanding andalso InFlight =< MaxDepth andalso
        check_rrc_load(Rest, State, MaxOutstanding, MaxDepth).

%% =========================================================================
%% Read buffer (block queue) properties
%%
%% The model is the flat binary of everything ever appended, based at the
%% buffer's initial position. Whatever interleaving of appends, reads, and
%% block-granular drops occurs, the retained window and every in-range read
%% must be byte-identical to the corresponding slice of that history.
%% =========================================================================

%% =========================================================================
%% Read pipeline properties
%% =========================================================================

%% The reassembly queue's whole job is to turn range responses that interleave,
%% arrive short, arrive empty or fail into a contiguous buffer without ever
%% papering over a hole. The oracle for that does not need to model the queue:
%% a byte is in the buffer exactly when every byte below it in the fragment has
%% been received, whatever order the ranges were answered in. So the model is
%% just the bytes S3 has said, by position, and everything observable is
%% recomputed from them.
%%
%% Two things are asserted after every command, and they are the two halves of
%% one statement. A read is served if and only if the model says every byte
%% below its end has arrived - serving one it cannot is a hole papered over,
%% refusing one it can is a stall - and a served read carries exactly the bytes
%% of the positions it claims.
%%
%% Single fragment on purpose: the prefetched next fragment's own queue is
%% driven by the core's properties above. What is exercised here is the
%% reassembly, which is per fragment.
read_pipeline_matches_model(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_read_pipeline_matches_model/0, [], 500).

prop_read_pipeline_matches_model() ->
    ?FORALL(
        Ops,
        non_empty(list(gen_rp_op())),
        begin
            FragSize = 20_000,
            FragRef = #fragment_ref{offset = 0, uid = 1, size = FragSize},
            P = rabbitmq_stream_s3_read_pipeline:new(<<"prop-stream">>, FragRef, ?SEGMENT_HEADER_B),
            check_rp_ops(Ops, P, #{}, FragSize)
        end
    ).

gen_rp_op() ->
    frequency([
        {4, {push, range(1, 3000)}},
        %% `Which` picks among the outstanding ranges, so responses land out of
        %% issue order.
        {5, {deliver, range(0, 7), gen_rp_delivery()}},
        {2, {fail, range(0, 7)}},
        {1, release},
        {1, ready},
        {3, {read, range(1, 4000)}}
    ]).

gen_rp_delivery() ->
    frequency([
        %% Exactly the range, properly closed.
        {5, full},
        %% Ends before the range does, which queues the rest as a gap request.
        {3, ?LET(Frac, range(0, 100), {short, Frac})},
        %% Closes without a byte.
        {1, empty},
        %% More bytes than the range asked for: must be clipped, never spill
        %% into the successor's range.
        {1, over},
        %% Not closed, so the range stays in flight owing the rest.
        {2, partial}
    ]).

check_rp_ops([], _P, _Got, _FragSize) ->
    true;
check_rp_ops([Op | Ops], P0, Got0, FragSize) ->
    {P, Got} = apply_rp_op(Op, P0, Got0, FragSize),
    case check_rp_read(P, Got, FragSize) of
        true -> check_rp_ops(Ops, P, Got, FragSize);
        false -> false
    end.

apply_rp_op({push, Len}, P0, Got, FragSize) ->
    Frontier = rabbitmq_stream_s3_read_pipeline:frontier(0, P0),
    IdxStart = ?SEGMENT_HEADER_B + FragSize,
    case Frontier < IdxStart of
        true ->
            End = min(Frontier + Len - 1, IdxStart - 1),
            FragRef = rabbitmq_stream_s3_read_pipeline:current_fragment(P0),
            {_Spec, P} = rabbitmq_stream_s3_read_pipeline:push(FragRef, {Frontier, End}, P0),
            {P, Got};
        false ->
            {P0, Got}
    end;
apply_rp_op({deliver, Which, How}, P0, Got, _FragSize) ->
    case pick_rp_inflight(Which, P0) of
        none ->
            {P0, Got};
        {Id, Start, End} ->
            %% S3 continues a range from where it left off, not from its start:
            %% a response that was left open owes only the bytes after the ones
            %% it has already sent.
            Pos = rp_range_pos(Start, End, Got),
            Remaining = End - Pos + 1,
            {Bytes, Close} = rp_delivery(How, Remaining),
            Data = rb_pattern(Pos, Bytes),
            {_Signals, P} = rabbitmq_stream_s3_read_pipeline:data(Id, Data, Close, P0),
            %% The pipeline clips an over-delivery to the range, so the model
            %% records only what the range owns.
            {P, rp_received(Pos, min(Bytes, Remaining), Got)}
    end;
apply_rp_op({fail, Which}, P0, Got, _FragSize) ->
    case pick_rp_inflight(Which, P0) of
        none ->
            {P0, Got};
        {Id, Start, End} ->
            case rabbitmq_stream_s3_read_pipeline:fail(Id, fault, P0) of
                {ok, P} -> {P, rp_drop_staged(Start, End, Got)};
                {dropped, P} -> {P, Got};
                stale -> {P0, Got}
            end
    end;
apply_rp_op(release, P0, Got, _FragSize) ->
    {rabbitmq_stream_s3_read_pipeline:release(fault, P0), Got};
apply_rp_op(ready, P0, Got, _FragSize) ->
    {_Specs, P} = rabbitmq_stream_s3_read_pipeline:ready(8, P0),
    {P, Got};
apply_rp_op({read, Len}, P0, Got, _FragSize) ->
    ReadPos = rabbitmq_stream_s3_read_pipeline:read_position(P0),
    case rabbitmq_stream_s3_read_pipeline:read(ReadPos, Len, P0) of
        {ok, _Data, P} -> {P, Got};
        _ -> {P0, Got}
    end.

rp_delivery(full, Len) -> {Len, done};
rp_delivery(empty, _Len) -> {0, done};
rp_delivery(over, Len) -> {Len + 64, done};
rp_delivery(partial, Len) -> {max(1, Len div 2), continue};
rp_delivery({short, Frac}, Len) -> {max(1, Len * Frac div 100), done}.

%% A failure drops what *that range* had received but not yet appended, which is
%% everything of its own above the contiguous run: the buffer keeps what it has
%% already taken and the range restarts there. Other ranges' staged bytes are
%% untouched - they are held on their own request.
rp_drop_staged(Start, End, Got) ->
    Contiguous = rp_contiguous(Got),
    maps:filter(
        fun(Pos, _Byte) -> Pos < Contiguous orelse Pos < Start orelse Pos > End end,
        Got
    ).

rp_received(_Start, Len, Got) when Len =< 0 ->
    Got;
rp_received(Start, Len, Got) ->
    Data = rb_pattern(Start, Len),
    lists:foldl(
        fun(I, Acc) -> Acc#{Start + I => binary:at(Data, I)} end,
        Got,
        lists:seq(0, Len - 1)
    ).

%% Where a range's next delivery starts: past its own contiguous prefix. A
%% range's received bytes are contiguous from its start, since a failure puts it
%% back at the last byte that reached a buffer and drops the rest.
rp_range_pos(Start, End, _Got) when Start > End ->
    Start;
rp_range_pos(Start, End, Got) ->
    case is_map_key(Start, Got) of
        true -> rp_range_pos(Start + 1, End, Got);
        false -> Start
    end.

%% One past the last byte of the contiguous run from the start of the data
%% region: exactly what the buffer can hold, whatever order the ranges arrived.
rp_contiguous(Got) ->
    rp_contiguous(Got, ?SEGMENT_HEADER_B).

rp_contiguous(Got, Pos) ->
    case is_map_key(Pos, Got) of
        true -> rp_contiguous(Got, Pos + 1);
        false -> Pos
    end.

%% Both halves of the statement: a read is served exactly when the model says
%% the bytes are there, and a served read carries exactly those bytes.
check_rp_read(P, Got, FragSize) ->
    ReadPos = rabbitmq_stream_s3_read_pipeline:read_position(P),
    Len = 64,
    Contiguous = rp_contiguous(Got),
    Servable = ReadPos + Len =< Contiguous,
    case rabbitmq_stream_s3_read_pipeline:read(ReadPos, Len, P) of
        {ok, Data, _P} ->
            Servable andalso iolist_to_binary(Data) =:= rb_pattern(ReadPos, Len);
        await ->
            not Servable;
        past_end ->
            ReadPos >= ?SEGMENT_HEADER_B + FragSize
    end.

pick_rp_inflight(Which, P) ->
    case rabbitmq_stream_s3_read_pipeline:inflight_ranges(P) of
        [] ->
            none;
        Ranges ->
            {F, Start, End} = lists:nth(Which rem length(Ranges) + 1, Ranges),
            {ok, Id} = rabbitmq_stream_s3_read_pipeline:find_request(F, Start, P),
            {Id, Start, End}
    end.

read_buffer_matches_model(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_read_buffer_matches_model/0, [], 500).

prop_read_buffer_matches_model() ->
    ?FORALL(
        {StartPos, Ops},
        {range(0, 100), list(gen_rb_op())},
        begin
            Buf = rabbitmq_stream_s3_read_buffer:new(StartPos),
            check_rb_ops(Ops, Buf, StartPos, <<>>)
        end
    ).

gen_rb_op() ->
    frequency([
        %% Zero-length appends exercise the empty-block no-op.
        {4, {append, range(0, 2000)}},
        {3, {read, range(0, 99), range(0, 2000)}},
        %% Fractions above 100 drop past end_pos.
        {2, {drop, range(0, 120)}}
    ]).

check_rb_ops([], _Buf, _Base, _History) ->
    true;
check_rb_ops([{append, Len} | Ops], Buf0, Base, History0) ->
    Data = rb_pattern(rabbitmq_stream_s3_read_buffer:end_pos(Buf0), Len),
    Buf = rabbitmq_stream_s3_read_buffer:append(Data, Buf0),
    History = <<History0/binary, Data/binary>>,
    check_rb_window(Buf, Base, History) andalso check_rb_ops(Ops, Buf, Base, History);
check_rb_ops([{read, PosFrac, LenWant} | Ops], Buf, Base, History) ->
    Start = rabbitmq_stream_s3_read_buffer:start_pos(Buf),
    End = rabbitmq_stream_s3_read_buffer:end_pos(Buf),
    case End - Start of
        0 ->
            check_rb_ops(Ops, Buf, Base, History);
        Size ->
            Pos = Start + (Size * PosFrac) div 100,
            Len = min(LenWant, End - Pos),
            Expected = binary:part(History, Pos - Base, Len),
            Flat = rabbitmq_stream_s3_read_buffer:read(Pos, Len, Buf),
            IoData = rabbitmq_stream_s3_read_buffer:read_iodata(Pos, Len, Buf),
            Flat =:= Expected andalso
                iolist_to_binary(IoData) =:= Expected andalso
                check_rb_ops(Ops, Buf, Base, History)
    end;
check_rb_ops([{drop, PosFrac} | Ops], Buf0, Base, History) ->
    Start0 = rabbitmq_stream_s3_read_buffer:start_pos(Buf0),
    End = rabbitmq_stream_s3_read_buffer:end_pos(Buf0),
    Pos = Start0 + ((End - Start0) * PosFrac) div 100,
    Buf = rabbitmq_stream_s3_read_buffer:drop_before(Pos, Buf0),
    Start = rabbitmq_stream_s3_read_buffer:start_pos(Buf),
    %% Blocks are dropped whole: the new start never passes Pos (nor end_pos),
    %% never regresses, and the end is untouched.
    Start >= Start0 andalso
        Start =< max(Start0, min(Pos, End)) andalso
        rabbitmq_stream_s3_read_buffer:end_pos(Buf) =:= End andalso
        check_rb_window(Buf, Base, History) andalso
        check_rb_ops(Ops, Buf, Base, History).

%% The retained window [start_pos, end_pos) is byte-identical to the history
%% slice at those offsets, and the size accounting agrees.
check_rb_window(Buf, Base, History) ->
    Start = rabbitmq_stream_s3_read_buffer:start_pos(Buf),
    End = rabbitmq_stream_s3_read_buffer:end_pos(Buf),
    Size = rabbitmq_stream_s3_read_buffer:size(Buf),
    Size =:= End - Start andalso
        rabbitmq_stream_s3_read_buffer:read(Start, Size, Buf) =:=
            binary:part(History, Start - Base, Size).

%% Bytes whose value is a function of their absolute offset, so a read's
%% content proves which offsets it came from.
rb_pattern(_Pos, 0) ->
    <<>>;
rb_pattern(Pos, Len) ->
    <<<<(P rem 251)>> || P <- lists:seq(Pos, Pos + Len - 1)>>.

%% =========================================================================
%% Garbage collection reap-decision properties
%%
%% The Erlang-level companion to the P models in p/. The P models prove the GC
%% invariants (chiefly INV#2, "GC never deletes a live object") across
%% concurrent interleavings; these properties fuzz the input space of the pure
%% reap-decision functions -- floors, epochs, UIDs, and leading-group shapes --
%% which the hand-written eunit examples in rabbitmq_stream_s3_gc do not reach.
%% The two axes are complementary: P covers the ordering, these cover the
%% classification breadth.
%% =========================================================================

%% INV#2 at classification time: for any manifest and floor, classify/2 never
%% marks a live-referenced object deletable. Live means a fragment or group at or
%% above the floor, the current-epoch manifest, or the referenced leading group
%% straddling the floor (partial expiry). A conservative skip-groups manifest
%% (leading kilo-/mega-group) additionally protects every group below the floor.
gc_classify_never_reaps_live(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_gc_classify_never_reaps_live/0, [], 500).

prop_gc_classify_never_reaps_live() ->
    ?FORALL(
        Scenario,
        gen_gc_classify_scenario(),
        begin
            #{lookup := Lookup, live_keys := LiveKeys} = Scenario,
            lists:all(
                fun(Key) -> rabbitmq_stream_s3_gc:classify(Key, Lookup) =:= skip end,
                LiveKeys
            )
        end
    ).

%% Anti-vacuity: a genuine orphan (fragment or non-leading group below the floor,
%% or a stale-epoch manifest) is always classified deletable. Without this a
%% classify/2 that skipped everything would pass the safety property vacuously.
gc_classify_reclaims_orphans(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_gc_classify_reclaims_orphans/0, [], 500).

prop_gc_classify_reclaims_orphans() ->
    ?FORALL(
        Scenario,
        gen_gc_classify_scenario(),
        begin
            #{lookup := Lookup, dead_keys := DeadKeys} = Scenario,
            lists:all(
                fun(Key) ->
                    case rabbitmq_stream_s3_gc:classify(Key, Lookup) of
                        {ok, _} -> true;
                        skip -> false
                    end
                end,
                DeadKeys
            )
        end
    ).

%% Build a single-stream GC scenario: a manifest whose leading entry is a
%% fragment, group, or kilo-/mega-group, a floor above that leading entry
%% (partial-expiry, so a leading group straddles the floor), an epoch, and the
%% lookup entry the sweep would compute. LiveKeys must all classify to skip;
%% DeadKeys must all classify to a finding.
gen_gc_classify_scenario() ->
    ?LET(
        {LeadingKind, LeadingUid, Floor, Epoch, AboveOffs, BelowOffs, DeadGroupOffs, StaleEpochs},
        {
            oneof([fragment, group, kilo_group, mega_group]),
            gc_uid(),
            integer(2, 100000),
            integer(1, 1000),
            non_empty(list(non_neg_integer())),
            non_empty(list(non_neg_integer())),
            list(pos_integer()),
            list(non_neg_integer())
        },
        begin
            StreamId = <<"gc-prop-stream">>,
            LeadingOffset = 0,
            Kind = gc_leading_kind(LeadingKind),
            %% leading_group_info reads only the first entry, so a single leading
            %% entry is a faithful manifest for building the lookup.
            Manifest = #manifest{
                first_offset = Floor,
                next_offset = Floor + 1,
                entries = ?ENTRY(LeadingOffset, 0, 0, Kind, 0, LeadingUid)
            },
            Lookup = #{
                StreamId => rabbitmq_stream_s3_gc:lookup_entry(StreamId, Epoch, Floor, Manifest)
            },
            SkipGroups = LeadingKind =:= kilo_group orelse LeadingKind =:= mega_group,
            %% Fragments and groups at or above the floor are live.
            LiveAbove =
                [
                    rabbitmq_stream_s3:fragment_key(StreamId, Floor + (O rem 1000), gc_uid_of(O))
                 || O <- AboveOffs
                ] ++
                    [
                        gc_group_key(StreamId, Floor + (O rem 1000), gc_uid_of(O + 1))
                     || O <- AboveOffs
                    ],
            %% The current-epoch manifest is live.
            LiveManifest = [
                rabbitmq_stream_s3:manifest_key(StreamId, #manifest_ref{epoch = Epoch, uid = 7})
            ],
            %% The referenced leading group (a group straddling the floor) is
            %% live; in skip-groups mode every group below the floor is protected.
            LiveGroups =
                case LeadingKind of
                    group ->
                        [gc_group_key(StreamId, LeadingOffset, LeadingUid)];
                    _ when SkipGroups ->
                        [
                            gc_group_key(StreamId, gc_below(O, Floor), gc_uid_of(O))
                         || O <- DeadGroupOffs
                        ];
                    _ ->
                        []
                end,
            LiveKeys = LiveAbove ++ LiveManifest ++ LiveGroups,
            %% Fragments below the floor are always dead, regardless of leading
            %% kind.
            DeadFragments = [
                rabbitmq_stream_s3:fragment_key(StreamId, gc_below(O, Floor), gc_uid_of(O))
             || O <- BelowOffs
            ],
            %% A manifest strictly below the current epoch is stale.
            DeadManifests = [
                rabbitmq_stream_s3:manifest_key(
                    StreamId, #manifest_ref{epoch = E rem Epoch, uid = 9}
                )
             || E <- StaleEpochs, Epoch > 0
            ],
            %% Non-leading groups below the floor are dead only when not in
            %% conservative skip-groups mode. Offset >= 1 keeps them clear of the
            %% leading group at offset 0.
            DeadGroups =
                case SkipGroups of
                    true ->
                        [];
                    false ->
                        [
                            gc_group_key(StreamId, 1 + (O rem (Floor - 1)), gc_uid_of(O + 3))
                         || O <- DeadGroupOffs
                        ]
                end,
            DeadKeys = DeadFragments ++ DeadManifests ++ DeadGroups,
            #{lookup => Lookup, live_keys => LiveKeys, dead_keys => DeadKeys}
        end
    ).

gc_leading_kind(fragment) -> ?MANIFEST_KIND_FRAGMENT;
gc_leading_kind(group) -> ?MANIFEST_KIND_GROUP;
gc_leading_kind(kilo_group) -> ?MANIFEST_KIND_KILO_GROUP;
gc_leading_kind(mega_group) -> ?MANIFEST_KIND_MEGA_GROUP.

gc_group_key(StreamId, Offset, Uid) ->
    rabbitmq_stream_s3:group_key(
        StreamId, #group_ref{offset = Offset, kind = ?MANIFEST_KIND_GROUP, uid = Uid}
    ).

%% Map an arbitrary non-negative integer to an offset strictly below the floor.
gc_below(O, Floor) -> O rem Floor.

gc_uid() -> integer(1, 16#FFFFFFFF).

%% A deterministic, always-in-range UID derived from a generated integer, so keys
%% vary without adding another generator dimension.
gc_uid_of(O) -> 1 + (O rem 16#FFFFFFFF).

%% Execute-time re-validation (still_dangling/1), guards B and C: an offset-based
%% finding is deletable iff it is still below the *live* floor and, for a group,
%% is not the live referenced leading group. This is the reset re-read: a
%% re-tiered fragment at or above the live floor is protected, a genuine orphan
%% below it is reclaimed, and the live leading group straddling the floor is
%% protected while a stale object at the same offset (different UID) is reaped.
gc_still_dangling_respects_live_manifest(_Config) ->
    {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
    unlink(Pid),
    try
        rabbit_ct_proper_helpers:run_proper(
            fun prop_gc_still_dangling_respects_live_manifest/0, [], 300
        )
    after
        gen_server:stop(Pid)
    end.

prop_gc_still_dangling_respects_live_manifest() ->
    ?FORALL(
        {LiveFloor, LeadingUid, DataOffs, GroupOffs},
        {
            integer(2, 100000),
            gc_uid(),
            non_empty(list(non_neg_integer())),
            list(non_neg_integer())
        },
        begin
            StreamId = <<"gc-still-dangling-prop-stream">>,
            LeadingOffset = 0,
            %% A live manifest whose leading group straddles the floor.
            ok = rabbitmq_stream_s3_manifest_replica:put_manifest(
                StreamId,
                #manifest{
                    first_offset = LiveFloor,
                    next_offset = LiveFloor + 1,
                    entries = ?ENTRY(
                        LeadingOffset, 0, 0, ?MANIFEST_KIND_GROUP, 0, LeadingUid
                    )
                }
            ),
            LeadingKey = gc_group_key(StreamId, LeadingOffset, LeadingUid),
            %% The live leading group is below the floor but referenced: keep it.
            LeadingChecks = [
                not gc_still_dangling(StreamId, LeadingKey)
            ],
            %% A data fragment is deletable iff it is below the live floor.
            DataChecks = [
                begin
                    Offset = O rem (LiveFloor * 2),
                    Key = rabbitmq_stream_s3:fragment_key(StreamId, Offset, gc_uid_of(O)),
                    gc_still_dangling(StreamId, Key) =:= (Offset < LiveFloor)
                end
             || O <- DataOffs
            ],
            %% A group at the leading offset with a *different* UID is a stale
            %% orphan (below floor, not the live leading group): deletable. A
            %% group at or above the floor is a re-tier: protected.
            GroupChecks = [
                begin
                    StaleAtLeading = gc_group_key(
                        StreamId, LeadingOffset, gc_other_uid(LeadingUid)
                    ),
                    Retier = gc_group_key(StreamId, LiveFloor + (O rem 1000), gc_uid_of(O)),
                    gc_still_dangling(StreamId, StaleAtLeading) andalso
                        not gc_still_dangling(StreamId, Retier)
                end
             || O <- GroupOffs
            ],
            %% Epoch-based findings are never re-checked (epoch is monotonic).
            EpochChecks = [
                gc_still_dangling_finding(#{
                    stream_id => StreamId, key => <<"any">>, reason => stale_epoch
                }),
                gc_still_dangling_finding(#{
                    stream_id => StreamId, key => <<"any">>, reason => no_anchor
                })
            ],
            lists:all(
                fun(B) -> B end,
                LeadingChecks ++ DataChecks ++ GroupChecks ++ EpochChecks
            )
        end
    ).

%% A pending row (attached but not yet resolved) is not a live manifest to
%% compare against: still_dangling must never treat any offset or group as
%% deletable while a stream's row is in that state.
gc_still_dangling_pending_row_never_deletes(_Config) ->
    {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
    unlink(Pid),
    try
        rabbit_ct_proper_helpers:run_proper(
            fun prop_gc_still_dangling_pending_row_never_deletes/0, [], 300
        )
    after
        gen_server:stop(Pid)
    end.

prop_gc_still_dangling_pending_row_never_deletes() ->
    ?FORALL(
        {Offset, Uid},
        {non_neg_integer(), gc_uid()},
        begin
            StreamId = <<"gc-still-dangling-pending-prop-stream">>,
            ok = rabbitmq_stream_s3_manifest_replica:mark_pending(StreamId),
            DataKey = rabbitmq_stream_s3:fragment_key(StreamId, Offset, Uid),
            GroupKey = gc_group_key(StreamId, Offset, Uid),
            not gc_still_dangling(StreamId, DataKey) andalso
                not gc_still_dangling(StreamId, GroupKey)
        end
    ).

gc_still_dangling(StreamId, Key) ->
    gc_still_dangling_finding(#{
        stream_id => StreamId, key => Key, reason => below_first_offset
    }).

gc_still_dangling_finding(Finding) ->
    rabbitmq_stream_s3_gc:still_dangling(Finding).

%% A UID distinct from the given one, staying in range.
gc_other_uid(Uid) when Uid =:= 16#FFFFFFFF -> 1;
gc_other_uid(Uid) -> Uid + 1.

%% Guards A and D (cache-epoch gate): an offset-based delete is permitted only
%% when the local manifest cache holds a manifest at exactly the committed epoch.
%% A cache behind the committed epoch (the reset-after-snapshot window), a legacy
%% entry with no epoch, or no manifest at all must all fail closed.
gc_cache_epoch_gate(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_gc_cache_epoch_gate/0, [], 500).

prop_gc_cache_epoch_gate() ->
    ?FORALL(
        {CacheResult, Committed},
        {
            oneof([
                {#manifest{}, integer(0, 1000)},
                {#manifest{}, undefined},
                undefined
            ]),
            integer(0, 1000)
        },
        begin
            Result = rabbitmq_stream_s3_gc:cache_at_committed_epoch(CacheResult, Committed),
            Expected =
                case CacheResult of
                    {#manifest{}, Committed} -> true;
                    _ -> false
                end,
            Result =:= Expected
        end
    ).

%% The reset-path epoch fence: with a pinned writer epoch the sweep is permitted
%% only when the committed epoch equals it (a deposed or not-yet-committed writer
%% skips); with no writer epoch pinned (the operator CLI path) the consistent
%% read is the only guard and the sweep is always permitted.
gc_epoch_permits_sweep(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_gc_epoch_permits_sweep/0, [], 500).

prop_gc_epoch_permits_sweep() ->
    ?FORALL(
        {Committed, MaybeWriter},
        {integer(0, 1000), oneof([undefined, integer(0, 1000)])},
        begin
            Config =
                case MaybeWriter of
                    undefined -> #{mode => delete};
                    W -> #{mode => delete, writer_epoch => W}
                end,
            Result = rabbitmq_stream_s3_gc:epoch_permits_sweep(Committed, Config),
            Expected =
                case MaybeWriter of
                    undefined -> true;
                    WriterEpoch -> Committed =:= WriterEpoch
                end,
            Result =:= Expected
        end
    ).

%% Guard A (the cache-epoch gate) as a pure function of the two read results. The
%% shared per-stream lookup decision yields an entry only when the quorum read
%% succeeds, the writer-epoch fence permits the sweep, and the cache holds a
%% manifest at exactly the committed epoch (with the cross-stream path also
%% skipping an empty manifest). Every other combination fails closed. This is the
%% functional core the read-performing shells (build_lookup/2, build_stream_lookup/2)
%% delegate to, so it is exercised here without a live db or replica cache.
gc_stream_lookup_epoch_gate(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_gc_stream_lookup_epoch_gate/0, [], 500).

prop_gc_stream_lookup_epoch_gate() ->
    ?FORALL(
        {Consistent, Cache, MaybeWriter, AllowEmpty},
        {gc_consistent(), gc_cache(), oneof([undefined, integer(0, 10)]), boolean()},
        begin
            StreamId = <<"gc-lookup-prop-stream">>,
            Config =
                case MaybeWriter of
                    undefined -> #{mode => delete};
                    W -> #{mode => delete, writer_epoch => W}
                end,
            Result = rabbitmq_stream_s3_gc:stream_lookup_decision(
                StreamId, Consistent, Cache, Config, AllowEmpty
            ),
            Expected = gc_expected_lookup(Consistent, Cache, MaybeWriter, AllowEmpty),
            case {Result, Expected} of
                {skip, skip} ->
                    true;
                {{ok, Entry}, {ok, CommittedEpoch, FirstOffset}} ->
                    maps:get(epoch, Entry) =:= CommittedEpoch andalso
                        maps:get(first_offset, Entry) =:= FirstOffset;
                _ ->
                    false
            end
        end
    ).

%% Independent oracle for stream_lookup_decision/5: skip | {ok, Epoch, FirstOffset}.
gc_expected_lookup({error, _}, _Cache, _Writer, _AllowEmpty) ->
    skip;
gc_expected_lookup({ok, #{epoch := CommittedEpoch}}, Cache, Writer, AllowEmpty) ->
    case Writer =:= undefined orelse Writer =:= CommittedEpoch of
        false -> skip;
        true -> gc_expected_from_cache(CommittedEpoch, Cache, AllowEmpty)
    end.

gc_expected_from_cache(_CommittedEpoch, undefined, _AllowEmpty) ->
    skip;
gc_expected_from_cache(_CommittedEpoch, {#manifest{entries = <<>>}, _}, false) ->
    %% Cross-stream sweep skips an empty manifest regardless of its epoch.
    skip;
gc_expected_from_cache(CommittedEpoch, {#manifest{first_offset = FO}, CommittedEpoch}, _AllowEmpty) ->
    {ok, CommittedEpoch, FO};
gc_expected_from_cache(_CommittedEpoch, {#manifest{}, _CacheEpoch}, _AllowEmpty) ->
    skip.

%% Guard D (the execute-time freshness re-check) as a pure function of the read
%% results: an offset-based delete is permitted only when the quorum read succeeds
%% and the cache is at the committed epoch. A quorum failure or a lagging cache
%% both fail closed.
gc_fresh_enough_fails_closed(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_gc_fresh_enough_fails_closed/0, [], 500).

prop_gc_fresh_enough_fails_closed() ->
    ?FORALL(
        {Consistent, Cache},
        {gc_consistent(), gc_cache()},
        begin
            StreamId = <<"gc-fresh-prop-stream">>,
            Result = rabbitmq_stream_s3_gc:fresh_enough_decision(StreamId, Consistent, Cache),
            Expected =
                case Consistent of
                    {error, _} ->
                        false;
                    {ok, #{epoch := CommittedEpoch}} ->
                        case Cache of
                            {#manifest{}, CommittedEpoch} -> true;
                            _ -> false
                        end
                end,
            Result =:= Expected
        end
    ).

%% The no_anchor backstop as a pure function of the anchor read: reap only on a
%% consistent read that positively reports the anchor gone; a present anchor or any
%% read error fails closed.
gc_anchor_decision_fails_closed(_Config) ->
    rabbit_ct_proper_helpers:run_proper(fun prop_gc_anchor_decision_fails_closed/0, [], 500).

prop_gc_anchor_decision_fails_closed() ->
    ?FORALL(
        Read,
        oneof([{ok, boolean()}, {error, oneof([timeout, no_quorum, shutdown])}]),
        begin
            StreamId = <<"gc-anchor-prop-stream">>,
            Result = rabbitmq_stream_s3_gc:anchor_decision(StreamId, Read),
            Expected =
                case Read of
                    {ok, Exists} -> not Exists;
                    {error, _} -> false
                end,
            Result =:= Expected
        end
    ).

%% A committed-metadata quorum read result: an ok payload carrying the committed
%% epoch, or a read error (partition, no quorum).
gc_consistent() ->
    oneof([
        {ok, #{epoch => integer(0, 10)}},
        {error, oneof([timeout, no_quorum, shutdown])}
    ]).

%% A local replica cache read result: a manifest tagged with its synced epoch
%% (possibly a legacy undefined), or no cached manifest at all. The manifest is
%% empty or carries a single leading fragment, at an arbitrary floor.
gc_cache() ->
    oneof([
        undefined,
        ?LET(
            {Empty, Epoch, FirstOffset},
            {boolean(), oneof([integer(0, 10), undefined]), integer(0, 100000)},
            {gc_cache_manifest(Empty, FirstOffset), Epoch}
        )
    ]).

gc_cache_manifest(true, FirstOffset) ->
    #manifest{first_offset = FirstOffset, next_offset = FirstOffset};
gc_cache_manifest(false, FirstOffset) ->
    #manifest{
        first_offset = FirstOffset,
        next_offset = FirstOffset + 1,
        entries = ?ENTRY(FirstOffset, 0, 0, ?MANIFEST_KIND_FRAGMENT, 0, 1)
    }.

%% =========================================================================
%% Tier resolution properties (INV#4)
%%
%% The Erlang-level companion to the tier-routing and read-resolution P models.
%% The models prove INV#4 (tier resolution total and gap-free) across concurrent
%% interleavings; these properties fuzz the input space of the pure routing
%% decision in rabbitmq_stream_s3_log_reader, which the hand-written eunit
%% examples reach only at a handful of points.
%% =========================================================================

%% The `=/= -1` guard in resolve_remote_location/2 (the tier-routing model's
%% load-bearing guard): when the local log is empty (first_chunk_id = -1) there
%% is no local floor, so an offset at or beyond the remote tail must wait at the
%% live tail ({local, next}) and never resolve to a local reader at that offset.
%% Dropping the guard would treat the empty log's floor of -1 as a real floor and
%% route every offset to {local, Offset}, silently skipping the remote tier.
tier_routing_empty_local_never_routes_to_offset(_Config) ->
    {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
    unlink(Pid),
    try
        rabbit_ct_proper_helpers:run_proper(
            fun prop_tier_routing_empty_local_never_routes_to_offset/0, [], 300
        )
    after
        gen_server:stop(Pid)
    end.

prop_tier_routing_empty_local_never_routes_to_offset() ->
    ?FORALL(
        {RemoteFirst, Span, Extra},
        {non_neg_integer(), pos_integer(), non_neg_integer()},
        begin
            StreamId = <<"tier-routing-empty-prop-stream">>,
            RemoteNext = RemoteFirst + Span,
            ok = tier_routing_put_manifest(StreamId, RemoteFirst, RemoteNext),
            %% Fresh shared atomics: first_chunk_id = -1 (empty local log).
            Shared = osiris_log_shared:new(),
            Config = #{name => StreamId, shared => Shared},
            %% An offset at or beyond the remote tail: with the local log empty
            %% this must wait at the live tail, not read locally at the offset.
            Offset = RemoteNext + Extra,
            rabbitmq_stream_s3_log_reader:resolve_remote_location(Offset, Config) =:=
                {local, next}
        end
    ).

%% INV#4, both directions: an offset the local tier does not cover must route to
%% the remote tier, while an offset the local tier covers may route locally.
%% Modelled with a contiguous layout -- the remote tier holds
%% [RemoteFirst, RemoteNext) and, when the local log is non-empty, the local
%% floor sits at RemoteNext -- so an offset below RemoteFirst is below the local
%% floor (or the local log is empty) and must attach at the remote start, while
%% an offset at or above the local floor is served locally. This is the
%% tier-routing model's remoteCovers/localCovers obligation, exercised through
%% the I/O-free resolution branches.
tier_routing_below_remote_first_routes_remote(_Config) ->
    {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
    unlink(Pid),
    try
        rabbit_ct_proper_helpers:run_proper(
            fun prop_tier_routing_below_remote_first_routes_remote/0, [], 300
        )
    after
        gen_server:stop(Pid)
    end.

prop_tier_routing_below_remote_first_routes_remote() ->
    ?FORALL(
        {RemoteFirst, Span, Below, LocalEmpty, Above},
        {
            %% RemoteFirst >= 1 leaves room for an offset strictly below it.
            integer(1, 100000),
            pos_integer(),
            non_neg_integer(),
            boolean(),
            non_neg_integer()
        },
        begin
            StreamId = <<"tier-routing-below-prop-stream">>,
            RemoteNext = RemoteFirst + Span,
            ok = tier_routing_put_manifest(StreamId, RemoteFirst, RemoteNext),
            Shared = osiris_log_shared:new(),
            case LocalEmpty of
                true ->
                    ok;
                false ->
                    %% Contiguous local tier: its floor is the remote tail.
                    ok = osiris_log_shared:set_first_chunk_id(Shared, RemoteNext)
            end,
            Config = #{name => StreamId, shared => Shared},
            %% An offset strictly below the remote first offset is not covered by
            %% the local tier (which, if any, starts at RemoteNext), so it must
            %% attach at the remote start rather than a local reader.
            RemoteOnly = Below rem RemoteFirst,
            RemoteRouted =
                case rabbitmq_stream_s3_log_reader:resolve_remote_location(RemoteOnly, Config) of
                    {ok, #remote_location{}} -> true;
                    _ -> false
                end,
            %% With a non-empty local log, an offset at or above the local floor
            %% is covered locally and routes to a local reader at that offset.
            LocalRouted =
                case LocalEmpty of
                    true ->
                        true;
                    false ->
                        LocalOffset = RemoteNext + Above,
                        rabbitmq_stream_s3_log_reader:resolve_remote_location(
                            LocalOffset, Config
                        ) =:= {local, LocalOffset}
                end,
            RemoteRouted andalso LocalRouted
        end
    ).

%% Cache a manifest with a single leading fragment covering [First, Next). A
%% fragment (not a group) leading entry keeps the resolution I/O-free: descending
%% to the first fragment does not fetch a group object.
tier_routing_put_manifest(StreamId, First, Next) ->
    rabbitmq_stream_s3_manifest_replica:put_manifest(
        StreamId,
        #manifest{
            first_offset = First,
            next_offset = Next,
            entries = ?ENTRY(First, 0, 0, ?MANIFEST_KIND_FRAGMENT, 0, 1)
        }
    ).

%% INV#4 (tier resolution total): total_range/1 reports the union of the local
%% and remote extents, and is empty only when both tiers are empty. The
%% load-bearing case is an empty local log (first_chunk_id = -1) over a non-empty
%% remote tier: returning `empty` there (the pre-fix regression) made {abs}
%% reads of valid remote offsets fail as out_of_range. Mirrors the tier-routing
%% model's totality obligation on the read domain.
total_range_spans_both_tiers(_Config) ->
    {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
    unlink(Pid),
    try
        rabbit_ct_proper_helpers:run_proper(fun prop_total_range_spans_both_tiers/0, [], 500)
    after
        gen_server:stop(Pid)
    end.

prop_total_range_spans_both_tiers() ->
    ?FORALL(
        {Local, Remote},
        {gen_local_extent(), gen_remote_extent()},
        begin
            StreamId = <<"total-range-prop-stream">>,
            Config = tier_config(StreamId, Local, Remote),
            Result = rabbitmq_stream_s3_log_reader:total_range(Config),
            Expected =
                case {Local, Remote} of
                    {empty, empty} -> empty;
                    {empty, {Rf, Rn}} -> {Rf, Rn - 1};
                    {{Lf, Ll}, empty} -> {Lf, Ll};
                    {{Lf, Ll}, {Rf, Rn}} -> {min(Lf, Rf), max(Ll, Rn - 1)}
                end,
            %% The reported range spans both tiers, and is empty only when both
            %% tiers are empty (never empty while either tier holds data).
            Result =:= Expected andalso
                (Result =:= empty) =:= (Local =:= empty andalso Remote =:= empty)
        end
    ).

%% INV#4 (resolution total over the read domain): the {abs, Offset} spec is
%% defined exactly on total_range/1. An offset outside the total range must be
%% rejected as offset_out_of_range carrying that range, never routed to a tier.
%% The in-range direction (which reads a fragment index) is covered by the e2e
%% suites; this pins the pure out-of-range boundary.
abs_offset_out_of_total_range_is_rejected(_Config) ->
    {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
    unlink(Pid),
    try
        rabbit_ct_proper_helpers:run_proper(
            fun prop_abs_offset_out_of_total_range_is_rejected/0, [], 500
        )
    after
        gen_server:stop(Pid)
    end.

prop_abs_offset_out_of_total_range_is_rejected() ->
    ?FORALL(
        {Local, Remote, Below, Above},
        {gen_local_extent(), gen_remote_extent(), pos_integer(), non_neg_integer()},
        begin
            StreamId = <<"abs-range-prop-stream">>,
            Config = tier_config(StreamId, Local, Remote),
            Range = rabbitmq_stream_s3_log_reader:total_range(Config),
            OutOfRange =
                case Range of
                    empty ->
                        %% With no data at all, every offset is out of range.
                        [0, Above];
                    {First, Last} ->
                        %% Strictly below the first and strictly above the last.
                        [First - Below, Last + 1 + Above]
                end,
            lists:all(
                fun(Offset) ->
                    rabbitmq_stream_s3_log_reader:resolve_remote_location(
                        {abs, Offset}, Config
                    ) =:=
                        {error, {offset_out_of_range, Range}}
                end,
                OutOfRange
            )
        end
    ).

%% Configure a Config for total_range/1: an optional non-empty local log
%% [LocalFirst, LocalLast] (or an empty local log, first_chunk_id = -1) and an
%% optional non-empty remote extent [RemoteFirst, RemoteNext) cached in the
%% replica. A manifest is always written (empty or single-fragment) so no stale
%% row from a prior iteration leaks in.
tier_config(StreamId, Local, Remote) ->
    Shared = osiris_log_shared:new(),
    case Local of
        empty ->
            ok;
        {LocalFirst, LocalLast} ->
            ok = osiris_log_shared:set_first_chunk_id(Shared, LocalFirst),
            ok = osiris_log_shared:set_committed_offset(Shared, LocalLast)
    end,
    Manifest =
        case Remote of
            empty ->
                #manifest{first_offset = 0, next_offset = 0};
            {RemoteFirst, RemoteNext} ->
                #manifest{
                    first_offset = RemoteFirst,
                    next_offset = RemoteNext,
                    entries = ?ENTRY(RemoteFirst, 0, 0, ?MANIFEST_KIND_FRAGMENT, 0, 1)
                }
        end,
    ok = rabbitmq_stream_s3_manifest_replica:put_manifest(StreamId, Manifest),
    #{name => StreamId, shared => Shared}.

%% An empty local log, or a non-empty one [First, Last] with First =< Last.
gen_local_extent() ->
    oneof([
        empty,
        ?LET(
            {First, Span},
            {non_neg_integer(), non_neg_integer()},
            {First, First + Span}
        )
    ]).

%% An empty remote tier, or a non-empty extent [First, Next) with Next > First.
gen_remote_extent() ->
    oneof([
        empty,
        ?LET(
            {First, Span},
            {non_neg_integer(), pos_integer()},
            {First, First + Span}
        )
    ]).

%% INV#4 (no silent remote skip): resolve_first_lookup/1 is total over the three
%% outcomes of the fragment iterator and must never collapse a transient group
%% fetch failure into a local read. An `ok` serves the read remotely, an
%% `end_of_manifest` (the remote tier is genuinely empty) serves it locally, and
%% a `group_fetch_failed` must surface as a retry. Mirrors the read-resolution
%% model's NoSilentRemoteSkip monitor: only `end_of_manifest` may yield
%% {local, first}.
resolve_first_lookup_never_silent_local(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_resolve_first_lookup_never_silent_local/0, [], 500
    ).

prop_resolve_first_lookup_never_silent_local() ->
    ?FORALL(
        Lookup,
        gen_first_lookup(),
        begin
            Result = rabbitmq_stream_s3_log_reader:resolve_first_lookup(Lookup),
            case Lookup of
                {ok, #fragment_ref{offset = Offset}, _Iterator} ->
                    case Result of
                        {remote, #remote_location{chunk_id = Offset}} -> true;
                        _ -> false
                    end;
                end_of_manifest ->
                    Result =:= {local, first};
                {error, {group_fetch_failed, _} = Reason} ->
                    Result =:= {retry, Reason}
            end
        end
    ).

%% One of the three fragment-iterator outcomes resolve_first_lookup/1 interprets.
%% The iterator term is opaque to the function (it is only stored), so a
%% placeholder atom stands in.
gen_first_lookup() ->
    oneof([
        ?LET(
            Offset,
            non_neg_integer(),
            {ok, #fragment_ref{offset = Offset, uid = 1, size = 0}, iterator_placeholder}
        ),
        end_of_manifest,
        {error, {group_fetch_failed, oneof([timeout, no_quorum, closed])}}
    ]).

%% =========================================================================
%% Manifest-replica sync staleness (epoch monotonicity)
%%
%% The Erlang-level companion to the manifest-replica-lifecycle P model's
%% NoStaleFloorServed invariant. is_stale_sync/3 decides whether a delayed,
%% possibly reordered sync from a deposed writer is dropped.
%% =========================================================================

%% is_stale_sync/3 is exactly the lexicographic (epoch, sequence) order -- epoch
%% dominates so a higher epoch always wins regardless of where its sequence
%% restarted -- and a first sync (nothing recorded) is never stale. The
%% consequence checked here is the epoch-monotonicity invariant: a sync that is
%% applied (not stale) never lets the recorded (epoch, sequence) regress.
is_stale_sync_is_total_order(_Config) ->
    rabbit_ct_proper_helpers:run_proper(
        fun prop_is_stale_sync_is_total_order/0, [], 500
    ).

prop_is_stale_sync_is_total_order() ->
    ?FORALL(
        {Epoch, Seq, Recorded},
        {
            non_neg_integer(),
            non_neg_integer(),
            oneof([undefined, {non_neg_integer(), non_neg_integer(), node()}])
        },
        begin
            Stale = rabbitmq_stream_s3_manifest_replica:is_stale_sync(Epoch, Seq, Recorded),
            Expected =
                case Recorded of
                    undefined -> false;
                    {Seq0, Epoch0, _} -> {Epoch, Seq} < {Epoch0, Seq0}
                end,
            %% Applying a non-stale sync never regresses the recorded frontier.
            Monotonic =
                case Recorded of
                    undefined -> true;
                    {Seq0b, Epoch0b, _} -> Stale orelse {Epoch, Seq} >= {Epoch0b, Seq0b}
                end,
            Stale =:= Expected andalso Monotonic
        end
    ).
