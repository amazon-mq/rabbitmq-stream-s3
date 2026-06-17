%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(prop_SUITE).
-moduledoc """
Property-based tests for core invariants.

Covers:
- Manifest edits (append, truncate, replace) preserve structural invariants
- Fragment assembly metadata correctness
- Fragment iterator traversal completeness and ordering
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
        remote_reader_core_reply_size_bounded
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
    %% Only apply retention if the manifest has more than one entry.
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(State0),
    case byte_size(Manifest#manifest.entries) > ?ENTRY_B of
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
                #fragment_ref{offset = Offset} = rabbitmq_stream_s3_log_reader:find_fragment(
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
                S0, {data, make_ref(), 0, Data, done}
            ),
            %% Issue a read.
            {_S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
                S1, {read, ReadOffset, ReadBytes, chunk_boundary}
            ),
            %% If a reply was produced, its data must be <= ReadBytes.
            case [D || {reply, {ok, D}} <- Effects] of
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
        {1, {{retry}, NextReadPos}},
        {1, ?LET(E, gen_rrc_iterator_refreshed_event(), {E, NextReadPos})},
        {1, {deadline_expired, NextReadPos}}
    ]).

gen_rrc_read_event(NextReadPos) ->
    ?LET(
        Bytes,
        range(1, 5000),
        {{read, NextReadPos, Bytes, chunk_boundary}, NextReadPos + Bytes}
    ).

gen_rrc_data_event() ->
    ?LET(
        Size,
        range(1, 10000),
        {data, make_ref(), 0, binary:copy(<<0>>, Size), oneof([done, continue])}
    ).

gen_rrc_error_event() ->
    ?LET(
        Reason,
        oneof([timeout, slow_down, connection_error, stream_error, internal_error, pool_busy]),
        {request_error, make_ref(), 0, Reason}
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
run_rrc_events([{retry} | Rest], State0) ->
    {State1, _} = rabbitmq_stream_s3_remote_reader_core:step(State0, retry),
    run_rrc_events(Rest, State1);
run_rrc_events([Event | Rest], State0) ->
    {State1, _} = rabbitmq_stream_s3_remote_reader_core:step(State0, Event),
    run_rrc_events(Rest, State1).
