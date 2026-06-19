%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(replica_reader_core_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("include/rabbitmq_stream_s3.hrl").

all() ->
    [
        init_returns_empty_effects,
        fragment_cut_emits_submit_transfer,
        first_fragment_starts_persist_timer,
        second_fragment_no_timer,
        in_order_completion_applies_immediately,
        out_of_order_completion_buffered,
        out_of_order_three_fragments,
        persist_triggered_by_threshold,
        persist_not_triggered_below_threshold,
        persist_not_triggered_while_in_flight,
        persist_triggered_by_tick,
        transfer_complete_below_threshold_starts_timer,
        tick_no_op_when_nothing_applied,
        tick_no_op_when_persist_in_flight,
        tick_no_op_before_interval,
        persist_complete_emits_range_broadcast_retention,
        persist_complete_triggers_another_if_pending,
        persist_complete_rearms_timer_for_subthreshold_remainder,
        persist_failed_conflict_returns_reinitialize,
        persist_failed_not_found_nonzero_revision_stops,
        persist_failed_not_found_zero_revision_reinitializes,
        persist_failed_transient_retries,
        transfer_failed_retriable_resubmits,
        transfer_failed_status_map_retriable_resubmits,
        transfer_failed_fatal_retries_no_gap,
        fatal_failure_does_not_drain_subsequent,
        fatal_failure_retry_then_complete_no_gap,
        fatal_failure_mid_sequence,
        await_offset_satisfied_immediately,
        await_offset_blocks_until_applied,
        multiple_waiters_notified,
        first_fragment_sets_manifest_metadata,
        interleaved_cut_complete_cut,
        timer_restarts_after_persist_and_new_cut,
        persist_broadcast_only_includes_persisted_edits,
        transfer_during_persist_broadcast_in_next_persist,
        multiple_transfers_during_persist_all_broadcast,
        many_in_flight_reverse_completion,
        fatal_only_transfer_retries_keeps_queue,
        %% Rebalancing
        rebalance_detected_at_threshold,
        rebalance_not_detected_below_threshold,
        rebalance_defers_persist,
        group_upload_complete_applies_edit,
        group_upload_complete_triggers_persist,
        rebalance_edits_in_broadcast,
        interleaved_appends_during_rebalance,
        recursive_rebalance_groups_to_kilo_group,
        rebalance_tick_defers_while_in_flight,
        group_upload_failed_retriable_retries,
        group_upload_failed_fatal_abandons,
        %% Retention
        retention_complete_applies_edit,
        retention_complete_triggers_persist,
        retention_defers_persist,
        retention_edit_broadcast_on_persist_complete,
        retention_failed_clears_flag,
        pending_prefix_rewrite_reports_blocker,
        persist_failed_during_rebalance_retries_after,
        recursive_rebalance_three_levels_deep,
        multiple_persist_conflicts_reinitialize,
        %% Edge cases
        fatal_front_does_not_drain_buffered_completions,
        await_offset_satisfied_during_cascading_persist,
        tick_fires_at_exact_interval_boundary,
        retention_and_rebalance_same_persist_cycle,
        persist_complete_defers_retention_during_rebalance,
        rebalance_deferred_while_retention_in_flight,
        retention_complete_rechecks_deferred_rebalance,
        retention_failed_rechecks_deferred_rebalance,
        retention_deletes_all_entries,
        new_fragment_after_empty_manifest
    ].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

init_returns_empty_effects(_Config) ->
    {_S, Effects} = init_core(),
    ?assertEqual([], Effects).

fragment_cut_emits_submit_transfer(_Config) ->
    {S0, _} = init_core(),
    Meta = meta(0, 100),
    {_S1, _Ref, Effects} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(Meta, S0),
    ?assertMatch([{submit_transfer, _, <<"stream">>, <<"/dir">>, _} | _], Effects).

first_fragment_starts_persist_timer(_Config) ->
    {S0, _} = init_core(),
    {_S1, _Ref, Effects} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    Timers = [E || {start_persist_timer, _} = E <- Effects],
    ?assertMatch([{start_persist_timer, 2000}], Timers).

second_fragment_no_timer(_Config) ->
    {S0, _} = init_core(),
    {S1, _Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {_S2, _Ref2, Effects} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    Timers = [E || {start_persist_timer, _} = E <- Effects],
    ?assertEqual([], Timers).

in_order_completion_applies_immediately(_Config) ->
    {S0, _} = init_core(),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, _Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, 1001, S1),
    %% Manifest should have advanced.
    ?assertEqual(100, manifest_next_offset(S2)).

out_of_order_completion_buffered(_Config) ->
    {S0, _} = init_core(),
    {S1, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, Ref2, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    %% Complete second before first.
    {S3, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref2, 2002, S2),
    %% Manifest should NOT have advanced (first still pending).
    ?assertEqual(0, manifest_next_offset(S3)),
    %% Now complete first. Both should apply.
    {S4, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S3),
    ?assertEqual(200, manifest_next_offset(S4)).

out_of_order_three_fragments(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 10}),
    {S1, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, Ref2, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    {S3, Ref3, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(200, 300), S2),
    %% Complete 3, then 1, then 2.
    {S4, E4} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref3, 3003, S3),
    ?assertEqual([], [E || {start_persist, _, _, _, _, _} = E <- E4]),
    {S5, E5} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S4),
    ?assertEqual([], [E || {start_persist, _, _, _, _, _} = E <- E5]),
    %% Completing 2 unblocks all three.
    {S6, _E6} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref2, 2002, S5),
    ?assertEqual(300, manifest_next_offset(S6)).

persist_triggered_by_threshold(_Config) ->
    %% Threshold = 3.
    {S0, _} = init_core(#{persist_threshold => 3}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    {_S3, Effects} = cut_and_complete_effects(S2, 200, 300, 1003),
    Commits = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertMatch([{start_persist, _, _, _, _, _}], Commits).

persist_not_triggered_below_threshold(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 5}),
    {_S1, Effects} = cut_and_complete_effects(S0, 0, 100, 1001),
    Commits = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertEqual([], Commits).

persist_not_triggered_while_in_flight(_Config) ->
    %% Threshold = 2, so 2 completions would normally trigger.
    {S0, _} = init_core(#{persist_threshold => 2}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% This triggers the commit (threshold reached).
    {S2, _} = cut_and_complete_effects(S1, 100, 200, 1002),
    %% Now commit is in flight. More completions should NOT trigger another.
    {_S3, Effects} = cut_and_complete_effects(S2, 200, 300, 1003),
    Commits = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertEqual([], Commits).

persist_triggered_by_tick(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 100, persist_interval_ms => 1000}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Tick with enough time elapsed.
    Now = erlang:system_time(millisecond) + 2000,
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S1),
    Commits = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertMatch([{start_persist, _, _, _, _, _}], Commits).

%% When a transfer completes but since_persist is below the threshold,
%% a persist timer must be started so the tick will eventually flush.
%% Without this, bytes sit in the persist stage indefinitely when
%% publishing stops.
transfer_complete_below_threshold_starts_timer(_Config) ->
    %% Threshold=5, so a single completion won't trigger a persist.
    {S0, _} = init_core(#{persist_threshold => 5, persist_interval_ms => 2000}),
    {_S1, Effects} = cut_and_complete_effects(S0, 0, 100, 1001),
    Timers = [E || {start_persist_timer, _} = E <- Effects],
    ?assertMatch([{start_persist_timer, 2000}], Timers),
    %% No persist should have started.
    Commits = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertEqual([], Commits).

tick_no_op_when_nothing_applied(_Config) ->
    {S0, _} = init_core(),
    Now = erlang:system_time(millisecond) + 100000,
    {_S1, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S0),
    ?assertEqual([], Effects).

tick_no_op_when_persist_in_flight(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 1}),
    %% This triggers a commit (threshold=1).
    _S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Tick should not trigger another commit.
    Now = erlang:system_time(millisecond) + 100000,
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, _S1),
    ?assertEqual([], Effects).

persist_complete_emits_range_broadcast_retention(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Commit is now in flight. Complete it.
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S1),
    ?assertMatch([_ | _], [E || {update_range, _, _} = E <- Effects]),
    ?assertMatch([_ | _], [E || {broadcast, _, _} = E <- Effects]),
    ?assertMatch([_ | _], [E || {evaluate_retention, _, _} = E <- Effects]),
    ?assertMatch([_ | _], [E || cancel_persist_timer = E <- Effects]).

persist_complete_triggers_another_if_pending(_Config) ->
    %% Threshold = 2. Apply 2 to trigger commit, then apply 2 more while in flight.
    {S0, _} = init_core(#{persist_threshold => 2}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    %% Commit is in flight. Apply more.
    S3 = cut_and_complete(S2, 200, 300, 1003),
    S4 = cut_and_complete(S3, 300, 400, 1004),
    %% Complete the first commit. Should trigger another.
    {_S5, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S4),
    Commits = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertMatch([{start_persist, _, _, _, _, _}], Commits).

persist_complete_rearms_timer_for_subthreshold_remainder(_Config) ->
    %% Regression: when a persist completes leaving a remainder that is
    %% above zero but below the threshold, persist_complete must re-arm the
    %% persist timer. Otherwise the cancel_persist_timer it emits leaves the
    %% remainder unpersisted until the next publish (a quiesce stall).
    %% Threshold = 3. Apply 3 to start a persist, then 2 more while in flight,
    %% leaving a remainder of 2 (0 < 2 < 3) when the persist completes.
    {S0, _} = init_core(#{persist_threshold => 3}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    S3 = cut_and_complete(S2, 200, 300, 1003),
    S4 = cut_and_complete(S3, 300, 400, 1004),
    S5 = cut_and_complete(S4, 400, 500, 1005),
    {_S6, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S5),
    %% No new persist should start (remainder is below threshold)...
    ?assertEqual([], [E || {start_persist, _, _, _, _, _} = E <- Effects]),
    %% ...but a timer must be re-armed so tick/1 flushes the remainder.
    ?assertMatch(
        [{start_persist_timer, _} | _],
        [E || {start_persist_timer, _} = E <- Effects]
    ).

persist_failed_conflict_returns_reinitialize(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(conflict, S1),
    ?assertMatch([reinitialize], Effects).

persist_failed_not_found_nonzero_revision_stops(_Config) ->
    %% not_found with a non-zero expected revision means the stream's metadata
    %% node was deleted out from under an established stream. The reader must
    %% stop (not retry the orphan-PUT, not reinitialize and risk resurrecting
    %% the deleted stream). A non-zero last_persisted revision is produced by a
    %% completed persist, so drive one first.
    {S0, _} = init_core(#{persist_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:persist_complete(7, S1),
    %% Start a second persist; its expected revision is the persisted 7.
    S3 = cut_and_complete(S2, 100, 200, 1002),
    {_S4, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(not_found, S3),
    ?assertEqual([stop], Effects).

persist_failed_not_found_zero_revision_reinitializes(_Config) ->
    %% A first-ever persist (expected revision 0) uses create-if-absent and
    %% cannot legitimately return not_found. If it somehow does, do not stop a
    %% possibly-live new stream and do not retry forever: reinitialize once.
    %% Here last_persisted_manifest is the initial empty manifest (revision 0).
    {S0, _} = init_core(#{persist_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(not_found, S1),
    ?assertEqual([reinitialize], Effects).

persist_failed_transient_retries(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(s3_error, S1),
    %% Should retry: either start_persist or start_persist_timer.
    HasRetry = lists:any(
        fun
            ({start_persist, _, _, _, _, _}) -> true;
            ({start_persist_timer, _}) -> true;
            (_) -> false
        end,
        Effects
    ),
    ?assert(HasRetry).

transfer_failed_retriable_resubmits(_Config) ->
    {S0, _} = init_core(),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    %% slow_down is the S3 API layer's term for a 503 (the real shape; the old
    %% {http, 503} term was never constructed by any code).
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref, slow_down, S1),
    ?assertMatch([{resubmit_transfer, Ref, _, _, _}], Effects).

transfer_failed_status_map_retriable_resubmits(_Config) ->
    %% A non-special-cased transient status arrives as #{status => _}; a 5xx
    %% must be treated as retriable.
    {S0, _} = init_core(),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(
        Ref, #{status => 504, headers => []}, S1
    ),
    ?assertMatch([{resubmit_transfer, Ref, _, _, _}], Effects).

transfer_failed_fatal_retries_no_gap(_Config) ->
    %% A non-transient upload failure must NOT advance the manifest. The
    %% fragment is retried (with a backoff delay) rather than dropped, so
    %% next_offset cannot move past data that is not durable in S3.
    {S0, _} = init_core(),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(
        Ref, checksum_mismatch, S1
    ),
    ?assertMatch(
        [{resubmit_transfer_delayed, Ref, _, _, _, checksum_mismatch}], Effects
    ),
    %% The manifest must not have advanced.
    ?assertEqual(0, manifest_next_offset(S2)),
    %% A subsequent successful retry of the same fragment applies it.
    {S3, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, 1001, S2),
    ?assertEqual(100, manifest_next_offset(S3)).

fatal_failure_does_not_drain_subsequent(_Config) ->
    %% The previously buggy behavior: dropping a fatally-failed fragment and
    %% draining the fragments behind it advanced next_offset over a
    %% non-durable range, leaving a silent hole (issue #206). The failed
    %% fragment must instead block the queue until it is durable.
    {S0, _} = init_core(#{persist_threshold => 10}),
    {S1, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, Ref2, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    %% Complete second.
    {S3, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref2, 2002, S2),
    ?assertEqual(0, manifest_next_offset(S3)),
    %% First fails fatally. The second must NOT drain: the manifest stays put.
    {S4, _} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref1, enoent, S3),
    ?assertEqual(0, manifest_next_offset(S4)),
    %% Only once the first fragment's retry succeeds do both drain, contiguously.
    {S5, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S4),
    ?assertEqual(200, manifest_next_offset(S5)).

fatal_failure_retry_then_complete_no_gap(_Config) ->
    %% End-to-end regression for issue #206: fragment B between A and C fails
    %% with a non-transient error, then its retry succeeds. The manifest must
    %% end up contiguous with no missing entry.
    {S0, _} = init_core(#{persist_threshold => 10}),
    {S1, RefA, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, RefB, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    {S3, RefC, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(200, 300), S2),
    %% A completes and applies.
    {S4, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(RefA, 1001, S3),
    ?assertEqual(100, manifest_next_offset(S4)),
    %% B fails fatally. Manifest must not advance.
    {S5, BEffects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(
        RefB, {open_failed, "seg", enoent}, S4
    ),
    ?assertMatch([{resubmit_transfer_delayed, RefB, _, _, _, _}], BEffects),
    ?assertEqual(100, manifest_next_offset(S5)),
    %% C completes but is buffered behind B; still no advance.
    {S6, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(RefC, 3003, S5),
    ?assertEqual(100, manifest_next_offset(S6)),
    %% B's retry succeeds: B then C drain, contiguous through 300.
    {S7, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(RefB, 2002, S6),
    ?assertEqual(300, manifest_next_offset(S7)),
    %% Three contiguous fragment entries, no hole.
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(S7),
    ?assertEqual(3 * ?ENTRY_B, byte_size(Manifest#manifest.entries)),
    Offsets = entry_offsets(Manifest#manifest.entries),
    ?assertEqual([0, 100, 200], Offsets).

await_offset_satisfied_immediately(_Config) ->
    {S0, _} = init_core_with_manifest(#manifest{next_offset = 500}),
    {_S1, Effects} = rabbitmq_stream_s3_replica_reader_core:await_offset(100, fake_from, S0),
    ?assertMatch([{reply_waiters, [{fake_from, ok}]}], Effects).

await_offset_blocks_until_applied(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 1}),
    {S1, []} = rabbitmq_stream_s3_replica_reader_core:await_offset(100, fake_from, S0),
    %% Complete a fragment that advances past the waiter's offset.
    {S2, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 200), S1),
    {S3, Effects1} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, 1001, S2),
    %% Threshold=1 triggers a commit. Complete it to notify waiters.
    ?assertMatch([{start_persist, _, _, _, _, _}], [
        E
     || {start_persist, _, _, _, _, _} = E <- Effects1
    ]),
    {_S4, Effects2} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S3),
    Replies = [E || {reply_waiters, _} = E <- Effects2],
    ?assertMatch([{reply_waiters, [{fake_from, ok}]}], Replies).

multiple_waiters_notified(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 1}),
    {S1, []} = rabbitmq_stream_s3_replica_reader_core:await_offset(50, from1, S0),
    {S2, []} = rabbitmq_stream_s3_replica_reader_core:await_offset(150, from2, S1),
    %% First fragment satisfies from1 but not from2.
    {S3, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S2),
    {S4, Effects1} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S3),
    %% Commit fires (threshold=1). Complete it.
    {S5, Effects1b} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S4),
    Replies1 = lists:flatten([Rs || {reply_waiters, Rs} <- Effects1 ++ Effects1b]),
    ?assertMatch([{from1, ok}], Replies1),
    %% Second fragment satisfies from2.
    {S6, Ref2, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S5),
    {S7, Effects2} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref2, 1002, S6),
    {_S8, Effects2b} = rabbitmq_stream_s3_replica_reader_core:persist_complete(2, S7),
    Replies2 = lists:flatten([Rs || {reply_waiters, Rs} <- Effects2 ++ Effects2b]),
    ?assertMatch([{from2, ok}], Replies2).

tick_no_op_before_interval(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 100, persist_interval_ms => 5000}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Tick too soon. Should not trigger.
    Now = erlang:system_time(millisecond) + 100,
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S1),
    ?assertEqual([], Effects).

first_fragment_sets_manifest_metadata(_Config) ->
    {S0, _} = init_core(#{persist_threshold => 10}),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(42, 100), S0),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, 1001, S1),
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(S2),
    ?assertEqual(42, Manifest#manifest.first_offset),
    ?assertEqual(42 * 1000, Manifest#manifest.first_timestamp),
    ?assertEqual(100, Manifest#manifest.next_offset).

interleaved_cut_complete_cut(_Config) ->
    %% Cut 1, complete 1 (queue drains to empty), cut 2.
    %% Second cut should restart the commit timer since queue was empty.
    {S0, _} = init_core(#{persist_threshold => 10}),
    {S1, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S1),
    %% since_persist is now 1, but queue is empty. Next cut should start timer.
    {_S3, _Ref2, Effects} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
        meta(100, 200), S2
    ),
    Timers = [E || {start_persist_timer, _} = E <- Effects],
    %% Timer should NOT restart here because since_persist > 0.
    %% The timer was already started with the first cut.
    ?assertEqual([], Timers).

timer_restarts_after_persist_and_new_cut(_Config) ->
    %% After a commit completes (timer cancelled), the next cut should
    %% restart the timer.
    {S0, _} = init_core(#{persist_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Commit is in flight. Complete it.
    {S2, Effects1} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S1),
    ?assertMatch([_ | _], [E || cancel_persist_timer = E <- Effects1]),
    %% Now cut another fragment. Timer should restart.
    {_S3, _Ref, Effects2} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
        meta(100, 200), S2
    ),
    Timers = [E || {start_persist_timer, _} = E <- Effects2],
    ?assertMatch([{start_persist_timer, _}], Timers).

persist_broadcast_only_includes_persisted_edits(_Config) ->
    %% Fragments applied during a commit should not appear in the broadcast
    %% for that commit.
    {S0, _} = init_core(#{persist_threshold => 1}),
    %% This triggers a commit covering offset 0..100.
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Apply another fragment while commit is in flight.
    S2 = cut_and_complete(S1, 100, 200, 1002),
    ?assertEqual(200, manifest_next_offset(S2)),
    %% Complete the commit. Broadcast should only cover 0..100.
    {_S3, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S2),
    [BroadcastEdits] = [Edits || {broadcast, _, Edits} <- Effects],
    ?assertMatch([#edit{next_offset = 100}], BroadcastEdits).

transfer_during_persist_broadcast_in_next_persist(_Config) ->
    %% A fragment that completes during an in-flight persist must appear
    %% in the *next* persist's broadcast. Regression: previously the entry
    %% was silently dropped because persist_complete reset
    %% appended_since_persist unconditionally.
    {S0, _} = init_core(#{persist_threshold => 1}),
    %% Fragment A completes, triggers persist (threshold=1).
    {S1, RefA, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(RefA, 1001, S1),
    %% Persist is now in flight. Fragment B completes during it.
    {S3, RefB, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S2),
    {S4, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(RefB, 1002, S3),
    ?assertEqual(200, manifest_next_offset(S4)),
    %% First persist completes. Its broadcast covers only A (0..100).
    {S5, E1} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S4),
    [Edits1] = [Edits || {broadcast, _, Edits} <- E1],
    ?assertMatch([#edit{next_offset = 100}], Edits1),
    %% The first persist_complete should trigger another persist (B is pending).
    ?assertMatch([{start_persist, _, _, _, _, _} | _], [
        E
     || {start_persist, _, _, _, _, _} = E <- E1
    ]),
    %% Second persist completes. Its broadcast must cover B (100..200).
    {_S6, E2} = rabbitmq_stream_s3_replica_reader_core:persist_complete(2, S5),
    [Edits2] = [Edits || {broadcast, _, Edits} <- E2],
    ?assertMatch([#edit{next_offset = 200}], Edits2),
    %% Verify the full chain: applying both broadcasts to an empty manifest
    %% reproduces the final manifest.
    FinalManifest = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(_S6),
    assert_edits_reproduce_manifest(#manifest{}, FinalManifest, Edits1 ++ Edits2).

multiple_transfers_during_persist_all_broadcast(_Config) ->
    %% Multiple fragments complete during a single in-flight persist.
    %% All must appear in the subsequent broadcast.
    {S0, _} = init_core(#{persist_threshold => 1}),
    %% Fragment A triggers persist.
    {S1, RefA, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(RefA, 1001, S1),
    %% Fragments B, C, D complete during the persist.
    {S3, RefB, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S2),
    {S4, RefC, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(200, 300), S3),
    {S5, RefD, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(300, 400), S4),
    {S6, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(RefB, 1002, S5),
    {S7, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(RefC, 1003, S6),
    {S8, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(RefD, 1004, S7),
    ?assertEqual(400, manifest_next_offset(S8)),
    %% First persist completes (covers A only).
    {S9, E1} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S8),
    [Edits1] = [Edits || {broadcast, _, Edits} <- E1],
    ?assertMatch([#edit{next_offset = 100}], Edits1),
    %% Second persist completes (must cover B+C+D as individual edits).
    {_S10, E2} = rabbitmq_stream_s3_replica_reader_core:persist_complete(2, S9),
    [Edits2] = [Edits || {broadcast, _, Edits} <- E2],
    ?assertEqual(3, length(Edits2)),
    ?assertMatch(#edit{next_offset = 400}, lists:last(Edits2)),
    %% Full chain reproduces the manifest.
    FinalManifest = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(_S10),
    assert_edits_reproduce_manifest(#manifest{}, FinalManifest, Edits1 ++ Edits2).

fatal_failure_mid_sequence(_Config) ->
    %% Cut 4 fragments. Complete 1, 3 and 4. Fragment 2 fails fatally.
    %% Fragments 3 and 4 must stay buffered (not drain) because 2 still
    %% blocks the queue. They only drain once 2's retry succeeds.
    {S0, _} = init_core(#{persist_threshold => 10}),
    {S1, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, Ref2, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    {S3, Ref3, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(200, 300), S2),
    {S4, Ref4, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(300, 400), S3),
    %% Complete 1, 3, 4 (not 2).
    {S5, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S4),
    ?assertEqual(100, manifest_next_offset(S5)),
    {S6, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref3, 1003, S5),
    ?assertEqual(100, manifest_next_offset(S6)),
    {S7, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref4, 1004, S6),
    ?assertEqual(100, manifest_next_offset(S7)),
    %% Fragment 2 fails fatally. 3 and 4 must NOT drain.
    {S8, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref2, enoent, S7),
    ?assertMatch([{resubmit_transfer_delayed, Ref2, _, _, _, enoent}], Effects),
    ?assertEqual(100, manifest_next_offset(S8)),
    %% Fragment 2's retry succeeds. Now 2, 3 and 4 drain to 400.
    {S9, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref2, 1002, S8),
    ?assertEqual(400, manifest_next_offset(S9)).

many_in_flight_reverse_completion(_Config) ->
    %% 10 fragments cut, completed in reverse order. All should apply
    %% when the first (oldest) completes last.
    {S0, _} = init_core(#{persist_threshold => 20}),
    {Refs, S1} = lists:foldl(
        fun(N, {Acc, St}) ->
            {St1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
                meta(N * 100, (N + 1) * 100), St
            ),
            {Acc ++ [Ref], St1}
        end,
        {[], S0},
        lists:seq(0, 9)
    ),
    %% Complete in reverse order (newest first).
    [OldestRef | NewerRefs] = Refs,
    S2 = lists:foldl(
        fun(Ref, St) ->
            {St1, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(
                Ref, erlang:unique_integer([positive]), St
            ),
            St1
        end,
        S1,
        lists:reverse(NewerRefs)
    ),
    %% Nothing applied yet (first is still pending).
    ?assertEqual(0, manifest_next_offset(S2)),
    %% Complete the first (oldest). All 10 should apply.
    {S3, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(
        OldestRef, erlang:unique_integer([positive]), S2
    ),
    ?assertEqual(1000, manifest_next_offset(S3)).

fatal_only_transfer_retries_keeps_queue(_Config) ->
    %% Single in-flight transfer fails fatally. The fragment must stay in the
    %% queue for retry (not be dropped), so its eventual completion still
    %% applies to the manifest.
    {S0, _} = init_core(#{persist_threshold => 10}),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref, enoent, S1),
    ?assertMatch([{resubmit_transfer_delayed, Ref, _, _, _, enoent}], Effects),
    ?assertEqual(0, manifest_next_offset(S2)),
    %% The retry of the same fragment eventually succeeds and applies.
    {S3, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, 1001, S2),
    ?assertEqual(100, manifest_next_offset(S3)).

%% ------------------------------------------------------------------
%% Rebalancing tests
%% ------------------------------------------------------------------

rebalance_detected_at_threshold(_Config) ->
    %% Use a small threshold for testing.
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    %% Apply Threshold-1 fragments without capturing effects.
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 2)
    ),
    %% The Threshold-th completion should trigger rebalance detection.
    {S2, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
        meta((Threshold - 1) * 100, Threshold * 100), S1
    ),
    {_S3, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(
        Ref, 1000 + Threshold - 1, S2
    ),
    UploadGroups = [E || {upload_group, _, _, _, _, _} = E <- Effects],
    ?assertMatch([{upload_group, <<"stream">>, ?MANIFEST_KIND_GROUP, _, 0, _}], UploadGroups).

rebalance_not_detected_below_threshold(_Config) ->
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    %% Apply Threshold - 1 fragments. Should not trigger rebalance.
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 3)
    ),
    {S2, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
        meta((Threshold - 2) * 100, (Threshold - 1) * 100), S1
    ),
    {_S3, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(
        Ref, 2000, S2
    ),
    UploadGroups = [E || {upload_group, _, _, _, _, _} = E <- Effects],
    ?assertEqual([], UploadGroups).

rebalance_defers_persist(_Config) ->
    %% When rebalance is in flight, persist should not start even if threshold is met.
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => Threshold}),
    %% Apply Threshold fragments. This triggers both rebalance and would trigger persist.
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 2)
    ),
    {S2, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
        meta((Threshold - 1) * 100, Threshold * 100), S1
    ),
    {_S3, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(
        Ref, 2000, S2
    ),
    %% Should have upload_group but NOT start_persist.
    UploadGroups = [E || {upload_group, _, _, _, _, _} = E <- Effects],
    Persists = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertMatch([_], UploadGroups),
    ?assertEqual([], Persists).

group_upload_complete_applies_edit(_Config) ->
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    %% Apply Threshold fragments to trigger rebalance.
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    %% Now complete the group upload.
    GroupUid = 9999,
    {S2, _Effects} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(GroupUid, S1),
    %% The manifest should now have 1 group entry instead of Threshold fragment entries.
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(S2),
    NumEntries = byte_size(Manifest#manifest.entries) div ?ENTRY_B,
    ?assertEqual(1, NumEntries),
    %% The entry should be a group entry.
    <<_:64, _:64/signed, _:64/signed, Kind:8, _:40, Uid:32, _/binary>> =
        Manifest#manifest.entries,
    ?assertEqual(?MANIFEST_KIND_GROUP, Kind),
    ?assertEqual(GroupUid, Uid).

group_upload_complete_triggers_persist(_Config) ->
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => Threshold}),
    %% Apply Threshold fragments (triggers rebalance, defers persist).
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    %% Complete the group upload. Persist should now fire.
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(9999, S1),
    Persists = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertMatch([_], Persists).

rebalance_edits_in_broadcast(_Config) ->
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => Threshold}),
    %% Apply Threshold fragments, complete group upload, then persist.
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(9999, S1),
    %% Persist should be in flight. Complete it.
    From = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S2),
    {S3, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(2, S2),
    To = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S3),
    %% Broadcast should contain Threshold append edits then the rebalance edit.
    [Edits] = [Es || {broadcast, _, Es} <- Effects],
    ?assertEqual(Threshold + 1, length(Edits)),
    AppendEdits = lists:sublist(Edits, Threshold),
    [RebalanceEdit] = lists:nthtail(Threshold, Edits),
    %% Append edits: len=0, each has one entry.
    lists:foreach(
        fun(E) ->
            ?assertEqual(0, E#edit.len),
            ?assertEqual(?ENTRY_B, byte_size(E#edit.entries))
        end,
        AppendEdits
    ),
    %% Rebalance edit: len > 0, entries = one group entry.
    ?assert(RebalanceEdit#edit.len > 0),
    ?assertEqual(?ENTRY_B, byte_size(RebalanceEdit#edit.entries)),
    %% The invariant: applying edits to From must produce To.
    assert_edits_reproduce_manifest(From, To, Edits).

interleaved_appends_during_rebalance(_Config) ->
    %% Fragments arrive, rebalance triggers, MORE fragments arrive and
    %% complete while the group upload is in flight, then the group upload
    %% completes, then persist. The broadcast edits must still transform
    %% From into To correctly.
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    %% Apply Threshold fragments to trigger rebalance.
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    %% Rebalance is now in flight. Apply 2 more fragments.
    S2 = cut_and_complete(S1, Threshold * 100, (Threshold + 1) * 100, 2001),
    S3 = cut_and_complete(S2, (Threshold + 1) * 100, (Threshold + 2) * 100, 2002),
    %% Complete the group upload.
    {S4, _} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(9999, S3),
    %% Manifest should have: 1 group entry + 2 fragment entries.
    M = rabbitmq_stream_s3_replica_reader_core:manifest(S4),
    ?assertEqual(3 * ?ENTRY_B, byte_size(M#manifest.entries)),
    %% Force persist via tick.
    Now = erlang:system_time(millisecond) + 10000,
    {S5, PersistEffects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S4),
    ?assertMatch([{start_persist, _, _, _, _, _}], PersistEffects),
    %% Complete persist and verify the invariant.
    From = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S5),
    {S6, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(2, S5),
    To = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S6),
    [Edits] = [Es || {broadcast, _, Es} <- Effects],
    assert_edits_reproduce_manifest(From, To, Edits).

recursive_rebalance_groups_to_kilo_group(_Config) ->
    %% Start with a manifest that already has Threshold-1 group entries,
    %% then add Threshold fragment entries to trigger a group, which brings
    %% the group count to Threshold, triggering a kilo-group.
    Threshold = 4,
    %% Build a manifest with Threshold-1 group entries already present.
    GroupEntries = lists:foldl(
        fun(I, Acc) ->
            Offset = I * 10000,
            FTs = I * 1000,
            LTs = (I + 1) * 1000,
            Uid = 5000 + I,
            E = ?ENTRY(Offset, FTs, LTs, ?MANIFEST_KIND_GROUP, 0, Uid),
            <<Acc/binary, E/binary>>
        end,
        <<>>,
        lists:seq(0, Threshold - 2)
    ),
    %% The manifest starts after the groups, so next_offset is past them.
    BaseOffset = (Threshold - 1) * 10000,
    Manifest = #manifest{
        first_offset = 0,
        first_timestamp = 0,
        first_last_timestamp = 1000,
        next_offset = BaseOffset,
        total_size = 0,
        entries = GroupEntries
    },
    {S0, _} = init_core_with_manifest(Manifest, #{
        rebalance_threshold => Threshold, persist_threshold => 100
    }),
    %% Apply Threshold fragments. This triggers a group rebalance (fragments→group).
    S1 = lists:foldl(
        fun(I, Acc) ->
            Offset = BaseOffset + I * 100,
            cut_and_complete(Acc, Offset, Offset + 100, 2000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    %% Complete the first group upload (fragments→group).
    {S2, Effects1} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(8888, S1),
    %% After this, we should have Threshold group entries, triggering kilo-group.
    KiloEffects = [E || {upload_group, _, ?MANIFEST_KIND_KILO_GROUP, _, _, _} = E <- Effects1],
    ?assertMatch([_], KiloEffects),
    %% Complete the kilo-group upload.
    {S3, _Effects2} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(7777, S2),
    %% Manifest should now have 1 kilo-group entry.
    M = rabbitmq_stream_s3_replica_reader_core:manifest(S3),
    <<_:64, _:64/signed, _:64/signed, Kind:8, _:40, _:32, _/binary>> = M#manifest.entries,
    ?assertEqual(?MANIFEST_KIND_KILO_GROUP, Kind).

rebalance_tick_defers_while_in_flight(_Config) ->
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    %% Apply Threshold fragments to trigger rebalance.
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    %% Tick should not trigger persist while rebalance is in flight.
    Now = erlang:system_time(millisecond) + 10000,
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S1),
    ?assertEqual([], Effects).

group_upload_failed_retriable_retries(_Config) ->
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    %% Retriable failure re-emits upload_group. internal_error is the API
    %% layer's term for a 500 (the real shape).
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:group_upload_failed(
        internal_error, S1
    ),
    UploadGroups = [E || {upload_group, _, _, _, _, _} = E <- Effects],
    ?assertMatch([_], UploadGroups).

group_upload_failed_fatal_abandons(_Config) ->
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => Threshold}),
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    %% Fatal failure abandons rebalance and allows persist to proceed. A 403
    %% arrives as #{status => 403}; it is not in the retriable set.
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:group_upload_failed(
        #{status => 403, headers => []}, S1
    ),
    %% No upload_group retry, but persist should fire (threshold met).
    UploadGroups = [E || {upload_group, _, _, _, _, _} = E <- Effects],
    Persists = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertEqual([], UploadGroups),
    ?assertMatch([_], Persists).

persist_failed_during_rebalance_retries_after(_Config) ->
    %% Persist fires (threshold=2), then rebalance triggers (threshold=4).
    %% Persist fails with a transient error while rebalance_in_flight = true.
    %% The retry is blocked. After group_upload_complete, persist fires.
    Threshold = 4,
    {S0, _} = init_core(#{persist_threshold => 2, rebalance_threshold => Threshold}),
    %% Apply 2 fragments → persist fires.
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    %% Persist is now in flight. Apply 2 more → rebalance triggers.
    S3 = cut_and_complete(S2, 200, 300, 1003),
    {S4, Ref4, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(300, 400), S3),
    {S5, Effects5} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref4, 1004, S4),
    %% Rebalance should have triggered.
    ?assertMatch([_], [E || {upload_group, _, _, _, _, _} = E <- Effects5]),
    %% Persist fails with transient error. Retry is blocked by rebalance.
    {S6, FailEffects} = rabbitmq_stream_s3_replica_reader_core:persist_failed(s3_error, S5),
    %% Should get a timer (fallback) but no start_persist.
    ?assertEqual([], [E || {start_persist, _, _, _, _, _} = E <- FailEffects]),
    ?assertMatch([{start_persist_timer, _}], [E || {start_persist_timer, _} = E <- FailEffects]),
    %% Tick is also blocked by rebalance_in_flight.
    Now = erlang:system_time(millisecond) + 100000,
    {S7, TickEffects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S6),
    ?assertEqual([], TickEffects),
    %% Group upload completes. Persist should now fire.
    {_S8, CompleteEffects} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(
        9999, S7
    ),
    ?assertMatch([_], [E || {start_persist, _, _, _, _, _} = E <- CompleteEffects]).

recursive_rebalance_three_levels_deep(_Config) ->
    %% Start with a manifest that has Threshold-1 kilo-group entries and
    %% Threshold-1 group entries. Adding Threshold fragments triggers:
    %% 1. fragments → group (brings group count to Threshold)
    %% 2. groups → kilo-group (brings kilo-group count to Threshold)
    %% 3. kilo-groups → mega-group
    Threshold = 4,
    %% Build Threshold-1 kilo-group entries.
    KiloEntries = lists:foldl(
        fun(I, Acc) ->
            Offset = I * 1_000_000,
            FTs = I * 100_000,
            LTs = (I + 1) * 100_000,
            Uid = 7000 + I,
            E = ?ENTRY(Offset, FTs, LTs, ?MANIFEST_KIND_KILO_GROUP, 0, Uid),
            <<Acc/binary, E/binary>>
        end,
        <<>>,
        lists:seq(0, Threshold - 2)
    ),
    %% Then Threshold-1 group entries after the kilo-groups.
    KiloEnd = (Threshold - 1) * 1_000_000,
    GroupEntries = lists:foldl(
        fun(I, Acc) ->
            Offset = KiloEnd + I * 10_000,
            FTs = (Threshold - 1) * 100_000 + I * 1000,
            LTs = (Threshold - 1) * 100_000 + (I + 1) * 1000,
            Uid = 8000 + I,
            E = ?ENTRY(Offset, FTs, LTs, ?MANIFEST_KIND_GROUP, 0, Uid),
            <<Acc/binary, E/binary>>
        end,
        <<>>,
        lists:seq(0, Threshold - 2)
    ),
    AllEntries = <<KiloEntries/binary, GroupEntries/binary>>,
    BaseOffset = KiloEnd + (Threshold - 1) * 10_000,
    Manifest = #manifest{
        first_offset = 0,
        first_timestamp = 0,
        first_last_timestamp = 100_000,
        next_offset = BaseOffset,
        total_size = 0,
        entries = AllEntries
    },
    {S0, _} = init_core_with_manifest(Manifest, #{
        rebalance_threshold => Threshold, persist_threshold => 100
    }),
    %% Apply Threshold fragments → triggers fragments→group.
    S1 = lists:foldl(
        fun(I, Acc) ->
            Offset = BaseOffset + I * 100,
            cut_and_complete(Acc, Offset, Offset + 100, 3000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    %% Complete group upload (fragments→group). Now Threshold groups exist → kilo-group.
    {S2, E1} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(8888, S1),
    ?assertMatch([_], [E || {upload_group, _, ?MANIFEST_KIND_KILO_GROUP, _, _, _} = E <- E1]),
    %% Complete kilo-group upload. Now Threshold kilo-groups exist → mega-group.
    {S3, E2} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(7777, S2),
    ?assertMatch([_], [E || {upload_group, _, ?MANIFEST_KIND_MEGA_GROUP, _, _, _} = E <- E2]),
    %% Complete mega-group upload. No further rebalancing needed.
    {S4, E3} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(6666, S3),
    ?assertEqual([], [E || {upload_group, _, _, _, _, _} = E <- E3]),
    %% Final manifest should have 1 mega-group entry.
    M = rabbitmq_stream_s3_replica_reader_core:manifest(S4),
    ?assertEqual(?ENTRY_B, byte_size(M#manifest.entries)),
    <<_:64, _:64/signed, _:64/signed, Kind:8, _:40, _:32>> = M#manifest.entries,
    ?assertEqual(?MANIFEST_KIND_MEGA_GROUP, Kind).

%% ------------------------------------------------------------------
%% Retention tests
%% ------------------------------------------------------------------

retention_complete_applies_edit(_Config) ->
    %% A retention edit removes the first fragment from the manifest.
    {S0, _} = init_core(#{persist_threshold => 100}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    S3 = cut_and_complete(S2, 200, 300, 1003),
    %% Remove the first entry.
    RetEdit = #edit{
        first_offset = 100,
        first_timestamp = 100 * 1000,
        first_last_timestamp = 199 * 1000,
        next_offset = undefined,
        size = -64_000_000,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    {S4, _Effects} = rabbitmq_stream_s3_replica_reader_core:retention_complete(RetEdit, S3),
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(S4),
    ?assertEqual(100, Manifest#manifest.first_offset),
    ?assertEqual(300, Manifest#manifest.next_offset),
    ?assertEqual(2 * ?ENTRY_B, byte_size(Manifest#manifest.entries)).

retention_complete_triggers_persist(_Config) ->
    %% retention_complete increments since_persist. With threshold=1 it
    %% should trigger a persist.
    {S0, _} = init_core(#{persist_threshold => 3}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    S3 = cut_and_complete(S2, 200, 300, 1003),
    %% Persist fires (threshold=3). Complete it.
    {S4, _} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S3),
    %% Now apply retention. since_persist goes to 1. Use tick to trigger.
    RetEdit = #edit{
        first_offset = 100,
        first_timestamp = 100 * 1000,
        first_last_timestamp = 199 * 1000,
        next_offset = undefined,
        size = -64_000_000,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    {S5, _} = rabbitmq_stream_s3_replica_reader_core:retention_complete(RetEdit, S4),
    %% Tick with enough time elapsed should trigger persist.
    Now = erlang:system_time(millisecond) + 10000,
    {_S6, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S5),
    Persists = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertMatch([_], Persists).

retention_defers_persist(_Config) ->
    %% While retention_in_flight is true, persist must not start.
    {S0, _} = init_core(#{persist_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Persist fires (threshold=1). Complete it.
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S1),
    %% Mark retention as started.
    S3 = rabbitmq_stream_s3_replica_reader_core:retention_started(S2),
    %% Apply another fragment. Persist should be deferred.
    {S4, Effects} = cut_and_complete_effects(S3, 100, 200, 1002),
    Persists = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertEqual([], Persists),
    %% Tick should also not trigger persist.
    Now = erlang:system_time(millisecond) + 10000,
    {_S5, TickEffects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S4),
    ?assertEqual([], TickEffects).

retention_edit_broadcast_on_persist_complete(_Config) ->
    %% The retention edit must appear in the broadcast only after persist.
    {S0, _} = init_core(#{persist_threshold => 3}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    S3 = cut_and_complete(S2, 200, 300, 1003),
    %% Persist fires (threshold=3). Complete it to establish baseline.
    {S4, _} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S3),
    %% Apply retention (removes first entry).
    RetEdit = #edit{
        first_offset = 100,
        first_timestamp = 100 * 1000,
        first_last_timestamp = 199 * 1000,
        next_offset = undefined,
        size = -64_000_000,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    {S5, RetEffects} = rabbitmq_stream_s3_replica_reader_core:retention_complete(RetEdit, S4),
    %% retention_complete must NOT emit broadcast or update_range.
    ?assertEqual([], [E || {broadcast, _, _} = E <- RetEffects]),
    ?assertEqual([], [E || {update_range, _, _} = E <- RetEffects]),
    %% Force persist via tick.
    Now = erlang:system_time(millisecond) + 10000,
    {S6, PersistEffects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S5),
    ?assertMatch([{start_persist, _, _, _, _, _}], PersistEffects),
    %% Complete persist. NOW the retention edit should be broadcast.
    From = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S6),
    {S7, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(2, S6),
    To = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S7),
    [Edits] = [Es || {broadcast, _, Es} <- Effects],
    %% The retention edit should be in the broadcast.
    RetEdits = [E || #edit{len = L} = E <- Edits, L > 0],
    ?assertMatch([_], RetEdits),
    %% The invariant must hold.
    assert_edits_reproduce_manifest(From, To, Edits).

pending_prefix_rewrite_reports_blocker(_Config) ->
    %% pending_prefix_rewrite/1 is the single gate the shell consults before
    %% evaluating remote retention. It must report none when idle, and name the
    %% specific in-flight prefix rewrite (retention or rebalance) so the CLI can
    %% tell the operator which one is blocking.
    {S0, _} = init_core(#{rebalance_threshold => 4, persist_threshold => 100}),
    ?assertEqual(none, rabbitmq_stream_s3_replica_reader_core:pending_prefix_rewrite(S0)),
    %% An async remote-retention evaluation in flight.
    SRet = rabbitmq_stream_s3_replica_reader_core:retention_started(S0),
    ?assertEqual(retention, rabbitmq_stream_s3_replica_reader_core:pending_prefix_rewrite(SRet)),
    %% A rebalance in flight: four fragments reach the rebalance threshold.
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    S3 = cut_and_complete(S2, 200, 300, 1003),
    S4 = cut_and_complete(S3, 300, 400, 1004),
    #{rebalance_in_flight := true} =
        rabbitmq_stream_s3_replica_reader_core:format_state(S4),
    ?assertEqual(rebalance, rabbitmq_stream_s3_replica_reader_core:pending_prefix_rewrite(S4)).

retention_failed_clears_flag(_Config) ->
    %% After retention_failed, persist should be unblocked.
    {S0, _} = init_core(#{persist_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S1),
    %% Mark retention started, then apply a fragment.
    S3 = rabbitmq_stream_s3_replica_reader_core:retention_started(S2),
    S4 = cut_and_complete(S3, 100, 200, 1002),
    %% Retention fails. Persist should now be possible.
    {S5, _} = rabbitmq_stream_s3_replica_reader_core:retention_failed(timeout, S4),
    %% Tick should trigger persist (since_persist > 0, interval elapsed).
    Now = erlang:system_time(millisecond) + 10000,
    {_S6, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S5),
    Persists = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertMatch([_], Persists).

multiple_persist_conflicts_reinitialize(_Config) ->
    %% 3 consecutive persist conflicts. Each triggers reinitialize.
    %% After each reinit, the core is re-initialized with the "resolved"
    %% manifest and continues operating normally. No leaked state.
    Opts = #{persist_threshold => 1},
    %% Cycle 1: cut, complete, persist fires, conflict.
    {S0, _} = init_core(Opts),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    {_S2, E1} = rabbitmq_stream_s3_replica_reader_core:persist_failed(conflict, S1),
    ?assertEqual([reinitialize], E1),
    %% Reinit with manifest at next_offset=100 (simulating resolved manifest).
    Manifest1 = #manifest{
        first_offset = 0,
        first_timestamp = 0,
        first_last_timestamp = 99000,
        next_offset = 100,
        total_size = 64_000_000,
        entries = ?ENTRY(0, 0, 99000, ?MANIFEST_KIND_FRAGMENT, 64_000_000, 1001)
    },
    {S3, _} = init_core_with_manifest(Manifest1, Opts),
    %% Cycle 2: cut, complete, persist fires, conflict.
    S4 = cut_and_complete(S3, 100, 200, 1002),
    {_S5, E2} = rabbitmq_stream_s3_replica_reader_core:persist_failed(conflict, S4),
    ?assertEqual([reinitialize], E2),
    %% Reinit with manifest at next_offset=200.
    Manifest2 = #manifest{
        first_offset = 0,
        first_timestamp = 0,
        first_last_timestamp = 199000,
        next_offset = 200,
        total_size = 128_000_000,
        entries = <<
            (Manifest1#manifest.entries)/binary,
            (?ENTRY(100, 100000, 199000, ?MANIFEST_KIND_FRAGMENT, 64_000_000, 1002))/binary
        >>
    },
    {S6, _} = init_core_with_manifest(Manifest2, Opts),
    %% Cycle 3: same pattern.
    S7 = cut_and_complete(S6, 200, 300, 1003),
    {_S8, E3} = rabbitmq_stream_s3_replica_reader_core:persist_failed(conflict, S7),
    ?assertEqual([reinitialize], E3),
    %% Reinit and verify normal operation resumes.
    Manifest3 = #manifest{
        first_offset = 0,
        first_timestamp = 0,
        first_last_timestamp = 299000,
        next_offset = 300,
        total_size = 192_000_000,
        entries = <<
            (Manifest2#manifest.entries)/binary,
            (?ENTRY(200, 200000, 299000, ?MANIFEST_KIND_FRAGMENT, 64_000_000, 1003))/binary
        >>
    },
    {S9, _} = init_core_with_manifest(Manifest3, Opts),
    %% After 3 conflicts, the core operates normally.
    S10 = cut_and_complete(S9, 300, 400, 1004),
    %% Persist fires (threshold=1). Complete it successfully this time.
    {S11, PEffects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S10),
    ?assertMatch([_ | _], [E || {update_range, _, _} = E <- PEffects]),
    ?assertMatch([_ | _], [E || {broadcast, _, _} = E <- PEffects]),
    ?assertEqual(
        400, (rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S11))#manifest.next_offset
    ).

%% ------------------------------------------------------------------
%% Edge case tests
%% ------------------------------------------------------------------

fatal_front_does_not_drain_buffered_completions(_Config) ->
    %% Cut 3 fragments. Complete 2 and 3 (out of order). Then fragment 1
    %% fails fatally. Fragments 2 and 3 must NOT drain: fragment 1 still
    %% blocks the queue until its retry succeeds.
    {S0, _} = init_core(#{persist_threshold => 10}),
    {S1, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, Ref2, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    {S3, Ref3, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(200, 300), S2),
    %% Complete 2 and 3 (not 1). Nothing should apply yet.
    {S4, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref2, 2002, S3),
    {S5, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref3, 3003, S4),
    ?assertEqual(0, manifest_next_offset(S5)),
    %% Fragment 1 fails fatally. 2 and 3 must stay buffered.
    {S6, _} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref1, enoent, S5),
    ?assertEqual(0, manifest_next_offset(S6)),
    %% Fragment 1's retry succeeds. Now all three drain to 300.
    {S7, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S6),
    ?assertEqual(300, manifest_next_offset(S7)).

await_offset_satisfied_during_cascading_persist(_Config) ->
    %% Waiter at 150 (satisfied by first persist covering 0..200).
    %% Waiter at 350 (needs second persist covering 200..400).
    %% Persist threshold=2. Apply 2 → triggers persist, then 2 more during it.
    {S0, _} = init_core(#{persist_threshold => 2}),
    {S1, []} = rabbitmq_stream_s3_replica_reader_core:await_offset(150, from1, S0),
    {S2, []} = rabbitmq_stream_s3_replica_reader_core:await_offset(350, from2, S1),
    %% Apply 2 fragments → triggers persist.
    S3 = cut_and_complete(S2, 0, 100, 1001),
    S4 = cut_and_complete(S3, 100, 200, 1002),
    %% Apply 2 more during the in-flight persist.
    S5 = cut_and_complete(S4, 200, 300, 1003),
    S6 = cut_and_complete(S5, 300, 400, 1004),
    %% First persist completes. from1 should be notified, cascading persist starts.
    {S7, E1} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S6),
    Replies1 = lists:flatten([Rs || {reply_waiters, Rs} <- E1]),
    ?assertMatch([{from1, ok}], Replies1),
    ?assertMatch([_], [E || {start_persist, _, _, _, _, _} = E <- E1]),
    %% Second persist completes. from2 should be notified.
    {_S8, E2} = rabbitmq_stream_s3_replica_reader_core:persist_complete(2, S7),
    Replies2 = lists:flatten([Rs || {reply_waiters, Rs} <- E2]),
    ?assertMatch([{from2, ok}], Replies2).

tick_fires_at_exact_interval_boundary(_Config) ->
    %% Tick should fire when (Now - LastTs) == Interval exactly.
    {S0, _} = init_core(#{persist_threshold => 100, persist_interval_ms => 5000}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Tick at exactly the interval boundary.
    Now = erlang:system_time(millisecond) + 5000,
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S1),
    Persists = [E || {start_persist, _, _, _, _, _} = E <- Effects],
    ?assertMatch([_], Persists).

retention_and_rebalance_same_persist_cycle(_Config) ->
    %% Scenario: apply enough fragments to trigger rebalance, complete the
    %% group upload, then apply a retention edit removing the group entry.
    %% All in one persist cycle. The broadcast must reproduce the manifest.
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    %% Apply Threshold fragments → triggers rebalance.
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 1)
    ),
    %% Complete the group upload. Now manifest has 1 group entry.
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(9999, S1),
    %% Apply more fragments so there's something after the group.
    S3 = cut_and_complete(S2, Threshold * 100, (Threshold + 1) * 100, 2001),
    S4 = cut_and_complete(S3, (Threshold + 1) * 100, (Threshold + 2) * 100, 2002),
    %% Now apply retention that removes the group entry (pos=0, len=ENTRY_B).
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(S4),
    <<_:64, _:64/signed, _:64/signed, _:8, _:40, _:32, Rest/binary>> =
        Manifest#manifest.entries,
    <<NewFirstOff:64, NewFirstTs:64/signed, NewFirstLTs:64/signed, _/binary>> = Rest,
    RetEdit = #edit{
        first_offset = NewFirstOff,
        first_timestamp = NewFirstTs,
        first_last_timestamp = NewFirstLTs,
        next_offset = undefined,
        size = 0,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    {S5, _} = rabbitmq_stream_s3_replica_reader_core:retention_complete(RetEdit, S4),
    %% Force persist via tick.
    Now = erlang:system_time(millisecond) + 999999,
    {S6, PersistEffects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S5),
    ?assertMatch([{start_persist, _, _, _, _, _}], PersistEffects),
    %% Complete persist and verify the invariant.
    From = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S6),
    {S7, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(2, S6),
    To = rabbitmq_stream_s3_replica_reader_core:persisted_manifest(S7),
    [Edits] = [Es || {broadcast, _, Es} <- Effects],
    assert_edits_reproduce_manifest(From, To, Edits).

persist_complete_defers_retention_during_rebalance(_Config) ->
    %% Reproduces the flake: a persist completes while a rebalance is in
    %% flight. persist_complete must NOT emit evaluate_retention, because
    %% retention could delete the entries the rebalance is factoring,
    %% causing group_upload_complete to crash with {badmatch, false}.
    %% After the rebalance completes and the next persist fires, retention
    %% is evaluated normally.
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 1}),
    %% Apply one fragment and persist it (so we have a persist in flight
    %% that will complete after the rebalance starts).
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Persist is now in flight (threshold=1 triggered it). Apply 3 more
    %% fragments to reach the rebalance threshold on the next drain.
    S2 = cut_and_complete(S1, 100, 200, 1002),
    S3 = cut_and_complete(S2, 200, 300, 1003),
    S4 = cut_and_complete(S3, 300, 400, 1004),
    %% Rebalance should now be in flight (4 fragments >= threshold).
    #{rebalance_in_flight := true} =
        rabbitmq_stream_s3_replica_reader_core:format_state(S4),
    %% Now the earlier persist completes. It must NOT emit evaluate_retention.
    {S5, Effects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(1, S4),
    ?assertEqual([], [E || {evaluate_retention, _, _} = E <- Effects]),
    %% Complete the rebalance. This clears rebalance_in_flight and triggers
    %% a new persist (since_persist > 0).
    {S6, RebalEffects} = rabbitmq_stream_s3_replica_reader_core:group_upload_complete(9999, S5),
    #{rebalance_in_flight := false} =
        rabbitmq_stream_s3_replica_reader_core:format_state(S6),
    %% The rebalance triggers persist (since_persist incremented).
    ?assertMatch([_ | _], [E || {start_persist, _, _, _, _, _} = E <- RebalEffects]),
    %% Complete that persist. NOW retention should be emitted.
    {_S7, FinalEffects} = rabbitmq_stream_s3_replica_reader_core:persist_complete(2, S6),
    ?assertMatch([_ | _], [E || {evaluate_retention, _, _} = E <- FinalEffects]).

rebalance_deferred_while_retention_in_flight(_Config) ->
    %% The mirror of persist_complete_defers_retention_during_rebalance: a
    %% remote retention evaluation is in flight when a drain reaches the
    %% rebalance threshold. maybe_start_rebalance must defer, because retention
    %% and rebalance rewrite the same leading entries (Single mutator). No
    %% upload_group is emitted and rebalance_in_flight stays false.
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    %% Apply Threshold - 1 fragments (below the rebalance threshold).
    S1 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S0,
        lists:seq(0, Threshold - 2)
    ),
    %% A remote retention evaluation is now in flight.
    S2 = rabbitmq_stream_s3_replica_reader_core:retention_started(S1),
    %% The Threshold-th completion would reach the rebalance threshold on drain.
    {S3, Effects} = cut_and_complete_effects(
        S2, (Threshold - 1) * 100, Threshold * 100, 2000
    ),
    ?assertEqual([], [E || {upload_group, _, _, _, _, _} = E <- Effects]),
    #{rebalance_in_flight := false} =
        rabbitmq_stream_s3_replica_reader_core:format_state(S3).

retention_complete_rechecks_deferred_rebalance(_Config) ->
    %% A rebalance deferred while retention was in flight must be re-checked
    %% once the retention edit is applied. Here the root stays oversized after
    %% retention removes one entry, so the rebalance fires.
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    %% Retention is in flight from the start, so the drains below defer the
    %% rebalance rather than starting it.
    S1 = rabbitmq_stream_s3_replica_reader_core:retention_started(S0),
    %% Apply Threshold + 1 fragments. Rebalance is deferred the whole time.
    S2 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S1,
        lists:seq(0, Threshold)
    ),
    #{rebalance_in_flight := false} =
        rabbitmq_stream_s3_replica_reader_core:format_state(S2),
    %% Retention removes the first entry, leaving Threshold fragments: still
    %% oversized. retention_complete clears the flag and re-checks rebalance.
    RetEdit = #edit{
        first_offset = 100,
        first_timestamp = 100 * 1000,
        first_last_timestamp = 199 * 1000,
        next_offset = undefined,
        size = -64_000_000,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    {S3, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_complete(RetEdit, S2),
    ?assertMatch(
        [{upload_group, <<"stream">>, ?MANIFEST_KIND_GROUP, _, 0, _}],
        [E || {upload_group, _, _, _, _, _} = E <- Effects]
    ),
    #{rebalance_in_flight := true} =
        rabbitmq_stream_s3_replica_reader_core:format_state(S3).

retention_failed_rechecks_deferred_rebalance(_Config) ->
    %% A retention evaluation that deferred a rebalance fails without changing
    %% the manifest. The deferred rebalance must still be re-checked so an
    %% oversized root does not wait for the next drain to compact.
    Threshold = 4,
    {S0, _} = init_core(#{rebalance_threshold => Threshold, persist_threshold => 100}),
    S1 = rabbitmq_stream_s3_replica_reader_core:retention_started(S0),
    %% Apply Threshold fragments while retention is in flight: rebalance defers.
    S2 = lists:foldl(
        fun(I, Acc) ->
            cut_and_complete(Acc, I * 100, (I + 1) * 100, 1000 + I)
        end,
        S1,
        lists:seq(0, Threshold - 1)
    ),
    #{rebalance_in_flight := false} =
        rabbitmq_stream_s3_replica_reader_core:format_state(S2),
    {S3, Effects} = rabbitmq_stream_s3_replica_reader_core:retention_failed(timeout, S2),
    ?assertMatch(
        [{upload_group, <<"stream">>, ?MANIFEST_KIND_GROUP, _, 0, _}],
        [E || {upload_group, _, _, _, _, _} = E <- Effects]
    ),
    #{rebalance_in_flight := true} =
        rabbitmq_stream_s3_replica_reader_core:format_state(S3).

retention_deletes_all_entries(_Config) ->
    %% Retention removes all entries. Manifest becomes empty.
    {S0, _} = init_core(#{persist_threshold => 100}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    %% Remove both entries.
    RetEdit = #edit{
        first_offset = 200,
        first_timestamp = -1,
        first_last_timestamp = -1,
        next_offset = undefined,
        size = -128_000_000,
        entries = <<>>,
        pos = 0,
        len = 2 * ?ENTRY_B
    },
    {S3, _Effects} = rabbitmq_stream_s3_replica_reader_core:retention_complete(RetEdit, S2),
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(S3),
    ?assertEqual(<<>>, Manifest#manifest.entries),
    ?assertEqual(200, Manifest#manifest.first_offset),
    ?assertEqual(200, Manifest#manifest.next_offset).

new_fragment_after_empty_manifest(_Config) ->
    %% After retention empties the manifest, new fragments append correctly.
    %% The first fragment sets first_offset (same as a fresh manifest).
    {S0, _} = init_core(#{persist_threshold => 100}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    RetEdit = #edit{
        first_offset = 100,
        first_timestamp = -1,
        first_last_timestamp = -1,
        next_offset = undefined,
        size = -64_000_000,
        entries = <<>>,
        pos = 0,
        len = ?ENTRY_B
    },
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:retention_complete(RetEdit, S1),
    %% Manifest is empty.
    M1 = rabbitmq_stream_s3_replica_reader_core:manifest(S2),
    ?assertEqual(<<>>, M1#manifest.entries),
    %% New fragment appends successfully and sets first_offset.
    S3 = cut_and_complete(S2, 200, 300, 1002),
    M2 = rabbitmq_stream_s3_replica_reader_core:manifest(S3),
    ?assertEqual(?ENTRY_B, byte_size(M2#manifest.entries)),
    ?assertEqual(200, M2#manifest.first_offset),
    ?assertEqual(300, M2#manifest.next_offset).

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

init_core() ->
    init_core(#{}).

init_core(Overrides) ->
    init_core_with_manifest(#manifest{}, Overrides).

init_core_with_manifest(Manifest) ->
    init_core_with_manifest(Manifest, #{}).

init_core_with_manifest(Manifest, Overrides) ->
    Opts = maps:merge(
        #{
            stream => <<"stream">>,
            dir => <<"/dir">>,
            epoch => 1,
            reference => test_ref,
            persist_threshold => 5,
            persist_interval_ms => 2000,
            rebalance_threshold => 1024
        },
        Overrides
    ),
    rabbitmq_stream_s3_replica_reader_core:init(Manifest, Opts).

meta(FirstOffset, NextOffset) ->
    #{
        first_offset => FirstOffset,
        first_timestamp => FirstOffset * 1000,
        last_timestamp => (NextOffset - 1) * 1000,
        next_offset => NextOffset,
        size => 64_000_000,
        num_chunks => 100,
        spans => [{0, 8, 64_000_008}]
    }.

%% Cut a fragment and immediately complete it. Returns new state.
cut_and_complete(State0, FirstOffset, NextOffset, Uid) ->
    {S1, _Effects} = cut_and_complete_effects(State0, FirstOffset, NextOffset, Uid),
    S1.

%% Cut a fragment and immediately complete it. Returns {state, effects from completion}.
cut_and_complete_effects(State0, FirstOffset, NextOffset, Uid) ->
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
        meta(FirstOffset, NextOffset), State0
    ),
    rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, Uid, S1).

manifest_next_offset(State) ->
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(State),
    Manifest#manifest.next_offset.

%% Extract the leading offset of every entry in a manifest entries binary,
%% in order. Used to assert that fragments form a contiguous chain.
entry_offsets(<<>>) ->
    [];
entry_offsets(<<Offset:64/unsigned, _:64, _:64, _:8, _:40, _:32, Rest/binary>>) ->
    [Offset | entry_offsets(Rest)].

%% Core invariant: applying broadcast edits to From must produce To.
%% This is the correctness guarantee for manifest replication. If this
%% fails, replicas will diverge from the writer.
assert_edits_reproduce_manifest(From, To, Edits) ->
    Result = lists:foldl(
        fun(Edit, Manifest) ->
            rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest)
        end,
        From,
        Edits
    ),
    ?assertEqual(
        To#manifest.entries,
        Result#manifest.entries,
        "Entries mismatch: edits did not reproduce the manifest"
    ),
    ?assertEqual(To#manifest.first_offset, Result#manifest.first_offset),
    ?assertEqual(To#manifest.next_offset, Result#manifest.next_offset),
    ?assertEqual(To#manifest.first_timestamp, Result#manifest.first_timestamp),
    ?assertEqual(To#manifest.total_size, Result#manifest.total_size).
