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
        first_fragment_starts_commit_timer,
        second_fragment_no_timer,
        in_order_completion_applies_immediately,
        out_of_order_completion_buffered,
        out_of_order_three_fragments,
        commit_triggered_by_threshold,
        commit_not_triggered_below_threshold,
        commit_not_triggered_while_in_flight,
        commit_triggered_by_tick,
        tick_no_op_when_nothing_applied,
        tick_no_op_when_commit_in_flight,
        tick_no_op_before_interval,
        commit_complete_emits_range_broadcast_retention,
        commit_complete_triggers_another_if_pending,
        commit_failed_conflict_returns_reinitialize,
        commit_failed_transient_retries,
        transfer_failed_retriable_resubmits,
        transfer_failed_fatal_accepts_gap,
        fatal_failure_drains_subsequent,
        fatal_failure_mid_sequence,
        await_offset_satisfied_immediately,
        await_offset_blocks_until_applied,
        multiple_waiters_notified,
        first_fragment_sets_manifest_metadata,
        interleaved_cut_complete_cut,
        timer_restarts_after_commit_and_new_cut,
        commit_broadcast_only_includes_committed_edits,
        many_in_flight_reverse_completion,
        fatal_only_transfer_then_new_cut_restarts_timer
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

first_fragment_starts_commit_timer(_Config) ->
    {S0, _} = init_core(),
    {_S1, _Ref, Effects} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    Timers = [E || {start_commit_timer, _} = E <- Effects],
    ?assertMatch([{start_commit_timer, 2000}], Timers).

second_fragment_no_timer(_Config) ->
    {S0, _} = init_core(),
    {S1, _Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {_S2, _Ref2, Effects} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    Timers = [E || {start_commit_timer, _} = E <- Effects],
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
    {S0, _} = init_core(#{durable_commit_threshold => 10}),
    {S1, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, Ref2, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    {S3, Ref3, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(200, 300), S2),
    %% Complete 3, then 1, then 2.
    {S4, E4} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref3, 3003, S3),
    ?assertEqual([], [E || {start_commit, _, _, _, _, _} = E <- E4]),
    {S5, E5} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S4),
    ?assertEqual([], [E || {start_commit, _, _, _, _, _} = E <- E5]),
    %% Completing 2 unblocks all three.
    {S6, _E6} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref2, 2002, S5),
    ?assertEqual(300, manifest_next_offset(S6)).

commit_triggered_by_threshold(_Config) ->
    %% Threshold = 3.
    {S0, _} = init_core(#{durable_commit_threshold => 3}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    {_S3, Effects} = cut_and_complete_effects(S2, 200, 300, 1003),
    Commits = [E || {start_commit, _, _, _, _, _} = E <- Effects],
    ?assertMatch([{start_commit, _, _, _, _, _}], Commits).

commit_not_triggered_below_threshold(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 5}),
    {_S1, Effects} = cut_and_complete_effects(S0, 0, 100, 1001),
    Commits = [E || {start_commit, _, _, _, _, _} = E <- Effects],
    ?assertEqual([], Commits).

commit_not_triggered_while_in_flight(_Config) ->
    %% Threshold = 2, so 2 completions would normally trigger.
    {S0, _} = init_core(#{durable_commit_threshold => 2}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% This triggers the commit (threshold reached).
    {S2, _} = cut_and_complete_effects(S1, 100, 200, 1002),
    %% Now commit is in flight. More completions should NOT trigger another.
    {_S3, Effects} = cut_and_complete_effects(S2, 200, 300, 1003),
    Commits = [E || {start_commit, _, _, _, _, _} = E <- Effects],
    ?assertEqual([], Commits).

commit_triggered_by_tick(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 100, durable_commit_interval_ms => 1000}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Tick with enough time elapsed.
    Now = erlang:system_time(millisecond) + 2000,
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S1),
    Commits = [E || {start_commit, _, _, _, _, _} = E <- Effects],
    ?assertMatch([{start_commit, _, _, _, _, _}], Commits).

tick_no_op_when_nothing_applied(_Config) ->
    {S0, _} = init_core(),
    Now = erlang:system_time(millisecond) + 100000,
    {_S1, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S0),
    ?assertEqual([], Effects).

tick_no_op_when_commit_in_flight(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 1}),
    %% This triggers a commit (threshold=1).
    _S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Tick should not trigger another commit.
    Now = erlang:system_time(millisecond) + 100000,
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, _S1),
    ?assertEqual([], Effects).

commit_complete_emits_range_broadcast_retention(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Commit is now in flight. Complete it.
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_complete(1, S1),
    ?assertMatch([_ | _], [E || {update_range, _, _} = E <- Effects]),
    ?assertMatch([_ | _], [E || {broadcast, _, _} = E <- Effects]),
    ?assertMatch([_ | _], [E || {evaluate_retention, _, _} = E <- Effects]),
    ?assertMatch([_ | _], [E || {cancel_commit_timer} = E <- Effects]).

commit_complete_triggers_another_if_pending(_Config) ->
    %% Threshold = 2. Apply 2 to trigger commit, then apply 2 more while in flight.
    {S0, _} = init_core(#{durable_commit_threshold => 2}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    S2 = cut_and_complete(S1, 100, 200, 1002),
    %% Commit is in flight. Apply more.
    S3 = cut_and_complete(S2, 200, 300, 1003),
    S4 = cut_and_complete(S3, 300, 400, 1004),
    %% Complete the first commit. Should trigger another.
    {_S5, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_complete(1, S4),
    Commits = [E || {start_commit, _, _, _, _, _} = E <- Effects],
    ?assertMatch([{start_commit, _, _, _, _, _}], Commits).

commit_failed_conflict_returns_reinitialize(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_failed(conflict, S1),
    ?assertMatch([{reinitialize}], Effects).

commit_failed_transient_retries(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_failed(s3_error, S1),
    %% Should retry: either start_commit or start_commit_timer.
    HasRetry = lists:any(
        fun
            ({start_commit, _, _, _, _, _}) -> true;
            ({start_commit_timer, _}) -> true;
            (_) -> false
        end,
        Effects
    ),
    ?assert(HasRetry).

transfer_failed_retriable_resubmits(_Config) ->
    {S0, _} = init_core(),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref, {http, 503}, S1),
    ?assertMatch([{resubmit_transfer, Ref, _, _, _}], Effects).

transfer_failed_fatal_accepts_gap(_Config) ->
    {S0, _} = init_core(),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, _Effects} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(
        Ref, checksum_mismatch, S1
    ),
    %% The in-flight queue should be empty (gap accepted).
    ?assertEqual(0, manifest_next_offset(S2)).

fatal_failure_drains_subsequent(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 10}),
    {S1, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, Ref2, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S1),
    %% Complete second.
    {S3, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref2, 2002, S2),
    ?assertEqual(0, manifest_next_offset(S3)),
    %% First fails fatally. Second should now drain.
    {S4, _} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref1, enoent, S3),
    ?assertEqual(200, manifest_next_offset(S4)).

await_offset_satisfied_immediately(_Config) ->
    {S0, _} = init_core_with_manifest(#manifest{next_offset = 500}),
    {_S1, Effects} = rabbitmq_stream_s3_replica_reader_core:await_offset(100, fake_from, S0),
    ?assertMatch([{reply_waiters, [{fake_from, ok}]}], Effects).

await_offset_blocks_until_applied(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 1}),
    {S1, []} = rabbitmq_stream_s3_replica_reader_core:await_offset(100, fake_from, S0),
    %% Complete a fragment that advances past the waiter's offset.
    {S2, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 200), S1),
    {S3, Effects1} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, 1001, S2),
    %% Threshold=1 triggers a commit. Complete it to notify waiters.
    ?assertMatch([{start_commit, _, _, _, _, _}], [
        E
     || {start_commit, _, _, _, _, _} = E <- Effects1
    ]),
    {_S4, Effects2} = rabbitmq_stream_s3_replica_reader_core:commit_complete(1, S3),
    Replies = [E || {reply_waiters, _} = E <- Effects2],
    ?assertMatch([{reply_waiters, [{fake_from, ok}]}], Replies).

multiple_waiters_notified(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 1}),
    {S1, []} = rabbitmq_stream_s3_replica_reader_core:await_offset(50, from1, S0),
    {S2, []} = rabbitmq_stream_s3_replica_reader_core:await_offset(150, from2, S1),
    %% First fragment satisfies from1 but not from2.
    {S3, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S2),
    {S4, Effects1} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S3),
    %% Commit fires (threshold=1). Complete it.
    {S5, Effects1b} = rabbitmq_stream_s3_replica_reader_core:commit_complete(1, S4),
    Replies1 = lists:flatten([Rs || {reply_waiters, Rs} <- Effects1 ++ Effects1b]),
    ?assertMatch([{from1, ok}], Replies1),
    %% Second fragment satisfies from2.
    {S6, Ref2, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(100, 200), S5),
    {S7, Effects2} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref2, 1002, S6),
    {_S8, Effects2b} = rabbitmq_stream_s3_replica_reader_core:commit_complete(2, S7),
    Replies2 = lists:flatten([Rs || {reply_waiters, Rs} <- Effects2 ++ Effects2b]),
    ?assertMatch([{from2, ok}], Replies2).

tick_no_op_before_interval(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 100, durable_commit_interval_ms => 5000}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Tick too soon. Should not trigger.
    Now = erlang:system_time(millisecond) + 100,
    {_S2, Effects} = rabbitmq_stream_s3_replica_reader_core:tick(Now, S1),
    ?assertEqual([], Effects).

first_fragment_sets_manifest_metadata(_Config) ->
    {S0, _} = init_core(#{durable_commit_threshold => 10}),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(42, 100), S0),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref, 1001, S1),
    Manifest = rabbitmq_stream_s3_replica_reader_core:manifest(S2),
    ?assertEqual(42, Manifest#manifest.first_offset),
    ?assertEqual(42 * 1000, Manifest#manifest.first_timestamp),
    ?assertEqual(100, Manifest#manifest.next_offset).

interleaved_cut_complete_cut(_Config) ->
    %% Cut 1, complete 1 (queue drains to empty), cut 2.
    %% Second cut should restart the commit timer since queue was empty.
    {S0, _} = init_core(#{durable_commit_threshold => 10}),
    {S1, Ref1, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:transfer_complete(Ref1, 1001, S1),
    %% since_commit is now 1, but queue is empty. Next cut should start timer.
    {_S3, _Ref2, Effects} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
        meta(100, 200), S2
    ),
    Timers = [E || {start_commit_timer, _} = E <- Effects],
    %% Timer should NOT restart here because since_commit > 0.
    %% The timer was already started with the first cut.
    ?assertEqual([], Timers).

timer_restarts_after_commit_and_new_cut(_Config) ->
    %% After a commit completes (timer cancelled), the next cut should
    %% restart the timer.
    {S0, _} = init_core(#{durable_commit_threshold => 1}),
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Commit is in flight. Complete it.
    {S2, Effects1} = rabbitmq_stream_s3_replica_reader_core:commit_complete(1, S1),
    ?assertMatch([_ | _], [E || {cancel_commit_timer} = E <- Effects1]),
    %% Now cut another fragment. Timer should restart.
    {_S3, _Ref, Effects2} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
        meta(100, 200), S2
    ),
    Timers = [E || {start_commit_timer, _} = E <- Effects2],
    ?assertMatch([{start_commit_timer, _}], Timers).

commit_broadcast_only_includes_committed_edits(_Config) ->
    %% Fragments applied during a commit should not appear in the broadcast
    %% for that commit.
    {S0, _} = init_core(#{durable_commit_threshold => 1}),
    %% This triggers a commit covering offset 0..100.
    S1 = cut_and_complete(S0, 0, 100, 1001),
    %% Apply another fragment while commit is in flight.
    S2 = cut_and_complete(S1, 100, 200, 1002),
    ?assertEqual(200, manifest_next_offset(S2)),
    %% Complete the commit. Broadcast should only cover 0..100.
    {_S3, Effects} = rabbitmq_stream_s3_replica_reader_core:commit_complete(1, S2),
    [BroadcastEdits] = [Edits || {broadcast, _, Edits} <- Effects],
    ?assertMatch([#edit{next_offset = 100}], BroadcastEdits).

fatal_failure_mid_sequence(_Config) ->
    %% Cut 4 fragments. Complete 1 and 4. Fragment 2 fails fatally.
    %% Fragment 3 was already completed (buffered). After the fatal failure
    %% of 2, fragments 3 and 4 should drain.
    {S0, _} = init_core(#{durable_commit_threshold => 10}),
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
    %% Fragment 2 fails fatally. 3 and 4 should drain.
    {S8, _} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref2, enoent, S7),
    ?assertEqual(400, manifest_next_offset(S8)).

many_in_flight_reverse_completion(_Config) ->
    %% 10 fragments cut, completed in reverse order. All should apply
    %% when the first (oldest) completes last.
    {S0, _} = init_core(#{durable_commit_threshold => 20}),
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

fatal_only_transfer_then_new_cut_restarts_timer(_Config) ->
    %% Single in-flight transfer fails fatally. Queue becomes empty.
    %% Next fragment_cut should restart the commit timer.
    {S0, _} = init_core(#{durable_commit_threshold => 10}),
    {S1, Ref, _} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(meta(0, 100), S0),
    {S2, _} = rabbitmq_stream_s3_replica_reader_core:transfer_failed(Ref, enoent, S1),
    %% since_commit is 0 (nothing was applied), queue is empty.
    %% Next cut should start the timer.
    {_S3, _Ref2, Effects} = rabbitmq_stream_s3_replica_reader_core:fragment_cut(
        meta(100, 200), S2
    ),
    Timers = [E || {start_commit_timer, _} = E <- Effects],
    ?assertMatch([{start_commit_timer, _}], Timers).

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
            durable_commit_threshold => 5,
            durable_commit_interval_ms => 2000,
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
