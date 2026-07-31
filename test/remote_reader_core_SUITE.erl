%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(remote_reader_core_SUITE).
-moduledoc """
Tests for the read path functional core.

These tests exercise the pure decision logic in rabbitmq_stream_s3_remote_reader_core
without any S3 interaction, timers, or processes.
""".

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbitmq_stream_s3/include/rabbitmq_stream_s3.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
        init_requests_data,
        read_served_from_buffer,
        read_awaits_when_buffer_empty,
        retry_after_transient_error,
        two_timeouts_then_retry_succeeds,
        fragment_transition_with_prefetch,
        become_local_at_end_of_manifest,
        not_found_triggers_refresh_iterator,
        %% New tests
        window_grows_on_miss,
        window_capped_at_window_max,
        window_decays_after_sustained_hits,
        pipeline_fills_to_window_and_depth,
        out_of_order_arrival_is_reassembled,
        mid_pipeline_error_reissues_only_that_range,
        failing_batch_arms_one_timer,
        fully_delivered_range_failing_before_fin_is_dropped,
        open_range_keeps_flushing_its_successors,
        open_range_does_not_pull_the_frontier_back,
        short_completion_refetches_the_gap,
        short_completion_at_the_frontier_refetches_the_gap,
        empty_completion_backs_off,
        over_delivery_is_clipped,
        stale_data_for_dropped_range_is_ignored,
        spill_stops_one_fragment_ahead,
        placement_pass_does_not_look_ahead,
        window_ceiling_below_the_request_size_is_clamped,
        next_fragment_404_refresh_reuses_the_peek,
        next_fragment_flushes_while_current_range_streams,
        exponential_backoff_caps_at_max,
        fatal_error_emits_stop,
        fatal_error_reports_reason_before_stop,
        multi_chunk_data_accumulation,
        prefetch_next_fragment_triggered,
        fragment_transition_without_prefetch_awaits,
        read_at_exact_fragment_boundary,
        mid_fragment_init_position,
        fragment_404_advances_to_next_fragment,
        retry_resets_delay_on_success,
        read_larger_than_buffer_awaits,
        header_overread_capped_at_index_boundary,
        tail_header_overread_below_guard_serves_remaining,
        next_fragment_404_triggers_refresh_iterator,
        prefetch_404_full_recovery,
        deadline_expired_replies_error_timeout,
        deadline_expired_keeps_the_buffer_for_the_retry,
        deadline_expired_at_a_fragment_boundary_refetches_nothing,
        deadline_expired_disowns_the_retry_timer,
        deadline_expired_clears_a_failed_look_ahead,
        deadline_expired_keeps_an_answered_look_ahead,
        deadline_expired_cancels_the_retry_timers,
        deadline_expired_drops_the_prefetched_next_fragment,
        deadline_expired_keeps_a_404_next_fragment,
        iterator_refresh_cancels_the_retry_timers,
        fragment_404_emits_refresh_iterator,
        known_404_fragment_is_not_refetched,
        last_fragment_404_no_pending_read_refreshes_past_current,
        current_fragment_404_no_pending_read_keeps_next_live_fragment,
        current_and_next_404_read_past_current_keeps_surviving_fragment,
        observe_effects_emitted_for_hit_miss_and_transition,
        %% A retryable error drops only the failed fragment, not all in-flight
        retryable_error_preserves_co_pending_request,
        %% pool_busy backoff (separate from the network-error backoff)
        pool_busy_backoff_capped_at_500,
        pool_busy_delay_resets_on_data,
        pool_busy_backoff_independent_of_network_errors,
        pool_busy_retry_does_not_release_a_throttled_range,
        partial_throttling_does_not_reset_the_backoff,
        retry_round_in_flight_does_not_reset_the_backoff,
        backoff_resets_once_the_retried_range_delivers,
        %% Looking one fragment ahead can cost an S3 GET, so it is memoised
        next_fragment_peek_is_fetched_once,
        failed_group_peek_is_retried_on_the_backoff,
        failed_group_peek_is_not_retried_on_the_pool_clock,
        failed_group_peek_arms_its_own_retry,
        group_fetch_failure_keeps_the_ranges_in_flight,
        read_larger_than_the_window_is_still_served,
        end_of_manifest_transition_drops_outstanding_requests,
        %% Transient group fetch errors must retry, not become local (F3)
        group_fetch_failure_retries_not_become_local,
        group_fetch_failure_on_refreshed_iterator_retries,
        group_fetch_failure_on_refreshed_iterator_drops_cancelled_requests,
        group_fetch_failure_backs_off_and_retries
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

stream_id() -> <<"test-stream">>.

%% Build a minimal fragment iterator with one or two entries.
%% Returns the iterator already advanced past the first entry (matching
%% the real behavior of find_position in the log reader).
mock_iterator(Entries) ->
    Manifest = build_manifest(Entries),
    GetGroupFun = fun(_) -> {error, not_found} end,
    FirstOffset =
        case Entries of
            [{Off, _, _} | _] -> Off;
            _ -> 0
        end,
    Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, FirstOffset, GetGroupFun),
    %% Advance past the current entry (same as advance_past_current in log_reader).
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
        {ok, _, It} -> It;
        _ -> Iterator0
    end.

build_manifest([]) ->
    #manifest{entries = <<>>};
build_manifest(Entries) ->
    #manifest{
        first_offset = element(1, hd(Entries)),
        next_offset = element(1, lists:last(Entries)) + 100,
        entries = entries_bin(Entries)
    }.

entries_bin(Entries) ->
    lists:foldl(
        fun({Offset, Size, Uid}, Acc) ->
            E = ?ENTRY(Offset, 0, 0, ?MANIFEST_KIND_FRAGMENT, Size, Uid),
            <<Acc/binary, E/binary>>
        end,
        <<>>,
        Entries
    ).

%% Build an iterator whose manifest is the given fragment entries followed by a
%% group entry, and whose group fetch fails transiently. After init advances
%% past the first fragment the iterator points at the group, so the next call to
%% `next/1` descends into it and returns `{error, {group_fetch_failed, _}}`.
mock_iterator_failing_group(FragEntries, {GroupOffset, _, GroupUid}) ->
    FragBin = entries_bin(FragEntries),
    GroupEntry = ?ENTRY(GroupOffset, 0, 0, ?MANIFEST_KIND_GROUP, 0, GroupUid),
    FirstOffset = element(1, hd(FragEntries)),
    Manifest = #manifest{
        first_offset = FirstOffset,
        next_offset = GroupOffset + 100,
        entries = <<FragBin/binary, GroupEntry/binary>>
    },
    %% A transient S3 error (not `not_found`, which would mean the group was
    %% deleted by retention and is skipped).
    GetGroupFun = fun(_) -> {error, slow_down} end,
    Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, FirstOffset, GetGroupFun),
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
        {ok, _, It} -> It;
        _ -> Iterator0
    end.

%% As `mock_iterator_failing_group/2`, but counting the attempts.
mock_iterator_counting_failing_group(FragEntries, {GroupOffset, _, GroupUid}, Counter) ->
    GroupEntry = ?ENTRY(GroupOffset, 0, 0, ?MANIFEST_KIND_GROUP, 0, GroupUid),
    FirstOffset = element(1, hd(FragEntries)),
    Manifest = #manifest{
        first_offset = FirstOffset,
        next_offset = GroupOffset + 100,
        entries = <<(entries_bin(FragEntries))/binary, GroupEntry/binary>>
    },
    GetGroupFun = fun(_) ->
        counters:add(Counter, 1, 1),
        {error, slow_down}
    end,
    Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, FirstOffset, GetGroupFun),
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
        {ok, _, It} -> It;
        _ -> Iterator0
    end.

%% Build an iterator whose manifest is the given fragment entries followed by
%% one group node per `Children` entry, each holding that one fragment. Every
%% descent into a group counts against `Counter`, which is what makes the cost
%% of looking one fragment ahead - a synchronous S3 GET in production - visible
%% to a test.
mock_iterator_counting_groups(FragEntries, Children, Counter) ->
    GroupsBin = lists:foldl(
        fun({Offset, _, Uid}, Acc) ->
            E = ?ENTRY(Offset, 0, 0, ?MANIFEST_KIND_GROUP, 0, Uid),
            <<Acc/binary, E/binary>>
        end,
        <<>>,
        Children
    ),
    FirstOffset = element(1, hd(FragEntries)),
    Manifest = #manifest{
        first_offset = FirstOffset,
        next_offset = element(1, lists:last(Children)) + 100,
        entries = <<(entries_bin(FragEntries))/binary, GroupsBin/binary>>
    },
    GetGroupFun = fun(#group_ref{offset = Offset}) ->
        counters:add(Counter, 1, 1),
        [Child] = [C || {O, _, _} = C <- Children, O =:= Offset],
        {ok, entries_bin([Child])}
    end,
    Iterator0 = rabbitmq_stream_s3_fragment_iterator:init(Manifest, FirstOffset, GetGroupFun),
    case rabbitmq_stream_s3_fragment_iterator:next(Iterator0) of
        {ok, _, It} -> It;
        _ -> Iterator0
    end.

frag_ref(Offset, Size, Uid) ->
    #fragment_ref{offset = Offset, uid = Uid, size = Size}.

init(StreamId, FragRef, Position, Iterator) ->
    init(StreamId, FragRef, Position, Iterator, #{}).

init(StreamId, FragRef, Position, Iterator, Opts) ->
    rabbitmq_stream_s3_remote_reader_core:init(StreamId, FragRef, Position, Iterator, Opts).

%% Requests are addressed by `{Fragment, RangeStart}`. Tests describe what S3
%% answers, not which byte range the core happened to ask for, so `deliver/4`
%% and `fail/3` resolve the range from the fragment's oldest outstanding
%% request. Tests that pipeline several ranges of one fragment address them
%% explicitly with `deliver/5` and `fail/4`.
deliver(State, Fragment, Data, DoneOrContinue) ->
    deliver(State, Fragment, range_start(State, Fragment), Data, DoneOrContinue).

deliver(State, Fragment, RangeStart, Data, DoneOrContinue) ->
    rabbitmq_stream_s3_remote_reader_core:step(
        State, {data, Fragment, RangeStart, Data, DoneOrContinue}
    ).

fail(State, Fragment, Reason) ->
    fail(State, Fragment, range_start(State, Fragment), Reason).

fail(State, Fragment, RangeStart, Reason) ->
    rabbitmq_stream_s3_remote_reader_core:step(
        State, {request_error, Fragment, RangeStart, Reason}
    ).

%% Fail a range and let its retry timer fire, which is the only thing that puts
%% the range back in flight. Tests that escalate a backoff must go through this:
%% a second error for a range already queued for retry is a stale duplicate and
%% is ignored, so back-to-back `fail/3` calls would not advance the delay.
%% Returns the failure's effects, not the retry's.
fail_then_retry(State0, Fragment, Reason) ->
    {State1, Effects} = fail(State0, Fragment, Reason),
    {State, _} = retry(State1, backoff_kind(Reason)),
    {State, Effects}.

%% Each backoff kind has its own timer, so a range waits on the one its failure
%% armed: releasing it means firing that kind's timer, not the other's.
backoff_kind(pool_busy) -> pool_busy;
backoff_kind(_Reason) -> fault.

retry(State, Kind) ->
    rabbitmq_stream_s3_remote_reader_core:step(State, {retry, Kind}).

range_start(State, Fragment) ->
    Ranges = rabbitmq_stream_s3_remote_reader_core:outstanding_ranges(State),
    case [Start || {F, Start, _} <- Ranges, F =:= Fragment] of
        [Start | _] -> Start;
        [] -> error({no_outstanding_range, Fragment, Ranges})
    end.

read(State, Offset, Bytes) ->
    rabbitmq_stream_s3_remote_reader_core:step(State, {read, Offset, Bytes, chunk_boundary}).

%% The ranges the core has asked for and not yet been answered.
outstanding_ranges(State) ->
    rabbitmq_stream_s3_remote_reader_core:outstanding_ranges(State).

fragment_ranges(State, Fragment) ->
    [{Start, End} || {F, Start, End} <- outstanding_ranges(State), F =:= Fragment].

%% The prefetch window: how far ahead of the consumer the reader will fetch.
window(State) ->
    rabbitmq_stream_s3_remote_reader_core:window_bytes(State).

%% What the window and the depth cap are bounding: `{Outstanding, InFlight}`.
load(State) ->
    rabbitmq_stream_s3_remote_reader_core:load(State).

%% Answer every outstanding range of a fragment, in queue order, with the
%% fragment's byte pattern.
serve_all(State0, Fragment) ->
    lists:foldl(
        fun({Start, End}, Acc) ->
            {Acc1, _} = deliver(Acc, Fragment, Start, pattern(Start, End - Start + 1), done),
            Acc1
        end,
        State0,
        fragment_ranges(State0, Fragment)
    ).

%% A fragment's bytes: each byte is its own offset modulo 251, so any read can
%% be checked against the position it claims to come from.
pattern(Pos, Len) ->
    <<<<(P rem 251)>> || P <- lists:seq(Pos, Pos + Len - 1)>>.

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

init_requests_data(_Config) ->
    %% On init, the core should emit a start_request effect for the current fragment.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {_State, Effects} = init(stream_id(), FragRef, 64, Iterator),
    ?assertMatch([{start_request, _, _, 0}], Effects).

read_served_from_buffer(_Config) ->
    %% After data arrives, a read at that position is served immediately.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, [{start_request, _Key, _Range, 0}]} =
        init(stream_id(), FragRef, 64, Iterator),

    %% Simulate data arriving (1024 bytes starting at position 64).
    Data = binary:copy(<<0>>, 1024),
    {S1, _} = deliver(S0, 0, Data, done),

    %% Now request a read at position 64, 100 bytes.
    {_S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {read, 64, 100, chunk_boundary}
    ),
    ?assertMatch([{reply, {ok, _}} | _], Effects),
    [{reply, {ok, ResultData}} | _] = Effects,
    ?assertEqual(100, byte_size(ResultData)).

read_awaits_when_buffer_empty(_Config) ->
    %% A read when no data is buffered produces no reply (awaiting data).
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    {_S1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S0, {read, 64, 100, chunk_boundary}
    ),
    %% No reply effect — the read is pending.
    Replies = [E || {reply, _} = E <- Effects],
    ?assertEqual([], Replies).

retry_after_transient_error(_Config) ->
    %% A transient error (slow_down) produces a set_timer effect.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    {_S1, Effects} = fail(S0, 0, slow_down),
    ?assertMatch([{set_timer, _, _}], Effects).

two_timeouts_then_retry_succeeds(_Config) ->
    %% Two timeouts increase retry delay. Then retry + data arrival serves the read.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    %% Issue a read.
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 100, chunk_boundary}),

    %% First timeout.
    {S2, E1} = fail_then_retry(S1, 0, timeout),
    ?assertMatch([{set_timer, fault, 1000}], E1),

    %% Second timeout — delay doubles.
    {S3, E2} = fail(S2, 0, timeout),
    ?assertMatch([{set_timer, fault, 2000}], E2),

    %% Retry fires, new request starts.
    {S4, E3} = retry(S3, fault),
    ?assertMatch([{start_request, _, _, 0} | _], E3),

    %% Data arrives, read is served.
    Data = binary:copy(<<0>>, 1024),
    {_S5, E4} = deliver(S4, 0, Data, done),
    Replies4 = [E || {reply, _} = E <- E4],
    ?assertMatch([{reply, {ok, _}}], Replies4).

fragment_transition_with_prefetch(_Config) ->
    %% When read position exceeds current fragment and next is pre-fetched,
    %% transition happens immediately.
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 300, 43}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    %% Fill current fragment buffer completely.
    Data = binary:copy(<<0>>, 200),
    {S1, _} = deliver(S0, 0, Data, done),

    %% Pre-fetch next fragment data.
    NextData = binary:copy(<<1>>, 300),
    {S2, _} = deliver(S1, 100, NextData, done),

    %% Read past end of current fragment (position 64 + 200 = 264 > 64 + 200).
    {_S3, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {read, 264, 50, chunk_boundary}
    ),
    ?assertMatch([{reply, {next_fragment, 100}} | _], Effects).

become_local_at_end_of_manifest(_Config) ->
    %% When iterator is exhausted and refresh returns end_of_manifest,
    %% the core signals become_local.
    FragRef = frag_ref(0, 100, 42),
    %% Single-entry manifest — iterator will be exhausted after init advances past it.
    Iterator = mock_iterator([{0, 100, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    %% Fill buffer to end of fragment.
    Data = binary:copy(<<0>>, 100),
    {S1, _} = deliver(S0, 0, Data, done),

    %% Read past end triggers refresh_iterator effect.
    {S2, E1} = rabbitmq_stream_s3_remote_reader_core:step(S1, {read, 164, 50, chunk_boundary}),
    ?assertMatch([{refresh_iterator, 0}], E1),

    %% Shell reports end_of_manifest.
    {_S3, E2} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {iterator_refreshed, end_of_manifest}
    ),
    ?assertMatch([{reply, {become_local, 0}}], E2).

not_found_triggers_refresh_iterator(_Config) ->
    %% A 404 on the current fragment triggers a refresh_iterator effect.
    %% If the refreshed iterator is exhausted, become_local.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    %% Issue a read.
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 100, chunk_boundary}),

    %% Request returns 404.
    {S2, E1} = fail(S1, 0, not_found),
    ?assertMatch([{cancel_requests, all}, {refresh_iterator, 0}], E1),

    %% Shell reports no more fragments → become_local.
    {_S3, E2} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {iterator_refreshed, end_of_manifest}
    ),
    ?assertMatch([{reply, {become_local, 0}}], E2).

%% Regression for #173. When the CURRENT fragment 404s while no read is pending
%% and the manifest has further live fragments, the refresh must advance past
%% the current (404'd) fragment's own offset, not the next fragment's offset.
%% The iterator is positioned past the current entry at init, so refreshing past
%% the next entry's offset would silently skip a live fragment.
current_fragment_404_no_pending_read_keeps_next_live_fragment(_Config) ->
    %% Three live fragments. The reader is on the first (offset 0); the iterator
    %% is positioned at the second (offset 100) after init advances past 0.
    Entries = [{0, 1_000_000, 1}, {100, 1_000_000, 2}, {200, 1_000_000, 3}],
    FragRef = frag_ref(0, 1_000_000, 1),
    Iterator = mock_iterator(Entries),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    %% The current fragment 404s before any read is pending: no effect yet, the
    %% reader just records current_not_found.
    {S1, E0} = fail(S0, 0, not_found),
    ?assertMatch([{cancel_requests, all}], E0),

    %% A later read wants bytes past the (empty) buffer, taking the
    %% current_not_found path. The refresh must target the current 404'd offset
    %% (0), NOT the next live fragment's offset (100). Before the fix this was
    %% {refresh_iterator, 100}, which made the shell skip fragment 100.
    {S2, E1} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {read, 64, 100, chunk_boundary}
    ),
    ?assertMatch([{refresh_iterator, 0}], E1),

    %% The shell refreshes past offset 0, leaving the iterator at the live
    %% fragment 100. The reader resumes there rather than skipping to 200.
    RefreshedIterator = mock_iterator(Entries),
    {_S3, E2} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {iterator_refreshed, RefreshedIterator}
    ),
    ?assertMatch(
        [{cancel_requests, all}, {cancel_timers, all}, {reply, {next_fragment, 100}} | _], E2
    ).

%% Regression for #173, the subtle interleaving: the current fragment 404s
%% (current_not_found = true) AND its prefetched next fragment also 404s
%% (next = not_found) before any read, then the consumer reads PAST the current
%% fragment. That read takes try_fragment_transition's `next = not_found` clause
%% (the path-1 producer of not_found_check_range), but current_not_found is also
%% set. The refresh must still target the current fragment's offset, not the
%% next's, so a fragment that survived retention after both 404'd offsets is not
%% skipped. Refreshing past the current (smaller) offset is always safe because
%% refresh_iterator re-derives position from the live manifest and lands on the
%% first surviving fragment greater than the target.
current_and_next_404_read_past_current_keeps_surviving_fragment(_Config) ->
    %% Fragments 0, 100, 200 live at init; the reader is on 0, iterator at 100.
    Entries = [{0, 1_000_000, 1}, {100, 1_000_000, 2}, {200, 1_000_000, 3}],
    FragRef = frag_ref(0, 1_000_000, 1),
    Iterator = mock_iterator(Entries),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    %% Fill the current fragment's buffer so a read past it transitions rather
    %% than awaiting more data for the current fragment.
    Data = binary:copy(<<0>>, 1_000_000),
    {S1, _} = deliver(S0, 0, Data, done),

    %% Current fragment (0) 404s with no read pending: records current_not_found.
    %% Its range is already complete, so the 404 is addressed explicitly.
    {S2, _} = fail(S1, 0, ?SEGMENT_HEADER_B, not_found),
    %% The prefetched next fragment (100) also 404s: sets next = not_found. Now
    %% both flags are set on the same state. The current fragment's 404 already
    %% dropped every queued range, so this one is addressed explicitly too.
    {S3, _} = fail(S2, 100, ?SEGMENT_HEADER_B, not_found),

    %% Read past the end of the current fragment (>= 64 + 1_000_000). This takes
    %% try_fragment_transition (next = not_found), but current_not_found is also
    %% set, so the refresh must target the current offset (0), not 100.
    {S4, E1} = rabbitmq_stream_s3_remote_reader_core:step(
        S3, {read, 64 + 1_000_000, 100, chunk_boundary}
    ),
    ?assertMatch([{refresh_iterator, 0}], E1),

    %% With 0 and 100 both gone from the tier, the refresh lands on the surviving
    %% fragment 200 - not skipped, not become_local.
    RefreshedIterator = mock_iterator([{0, 1_000_000, 1}, {200, 1_000_000, 3}]),
    {_S5, E2} = rabbitmq_stream_s3_remote_reader_core:step(
        S4, {iterator_refreshed, RefreshedIterator}
    ),
    ?assertMatch(
        [{cancel_requests, all}, {cancel_timers, all}, {reply, {next_fragment, 200}} | _], E2
    ).

%% ------------------------------------------------------------------
%% Prefetch window and retry tests
%% ------------------------------------------------------------------

window_grows_on_miss(_Config) ->
    %% A read that cannot be served means the reader is not fetching far enough
    %% ahead, so the window doubles. The read size this replaced halved on every
    %% miss, which drove it to its floor exactly when a consumer fell behind.
    {S0, _} = window_test_state(#{request_size => 1024, window_max => 65_536}),
    ?assertEqual(1024, window(S0)),
    {S1, E1} = read(S0, ?SEGMENT_HEADER_B, 50),
    ?assertMatch([{observe, miss, _} | _], E1),
    ?assertEqual(2048, window(S1)),
    %% Every delivery re-runs the serve attempt while a read waits, but only the
    %% first counts: one slow read must not run the window to its ceiling, nor
    %% inflate buffer_miss, which counts reads that waited rather than the
    %% deliveries they waited through.
    {S2, E2} = deliver(S1, 0, ?SEGMENT_HEADER_B, <<>>, continue),
    ?assertEqual([], [E || {observe, miss, _} = E <- E2]),
    ?assertEqual(2048, window(S2)),
    %% A new read that also misses does grow it again.
    {S3, _} = read(S2, ?SEGMENT_HEADER_B, 50),
    ?assertEqual(4096, window(S3)).

window_capped_at_window_max(_Config) ->
    {S0, _} = window_test_state(#{request_size => 1024, window_max => 4096}),
    S = lists:foldl(
        fun(I, Acc) ->
            {Acc1, _} = read(Acc, ?SEGMENT_HEADER_B + I, 50),
            Acc1
        end,
        S0,
        lists:seq(0, 9)
    ),
    ?assertEqual(4096, window(S)).

window_decays_after_sustained_hits(_Config) ->
    %% A consumer that outruns the reader drives the window to its ceiling; once
    %% it is being kept up with, each window's worth of bytes served without a
    %% miss hands one request back.
    {S0, _} = window_test_state(#{request_size => 1024, window_max => 16_384}),
    %% Five reads with nothing buffered: 1024 doubled five times, capped.
    S1 = lists:foldl(
        fun(I, Acc) ->
            {Acc1, _} = read(Acc, ?SEGMENT_HEADER_B + I, 50),
            Acc1
        end,
        S0,
        lists:seq(0, 4)
    ),
    ?assertEqual(16_384, window(S1)),
    %% Now answer everything the reader asks for and read it straight back, so
    %% every read is a hit.
    Final = lists:foldl(
        fun(I, State0) ->
            State1 = serve_all(State0, 0),
            {State, _} = read(State1, ?SEGMENT_HEADER_B + I * 512, 512),
            State
        end,
        S1,
        lists:seq(0, 199)
    ),
    ?assert(window(Final) < 16_384).

%% A reader on a fragment far larger than any window it can reach, so the
%% fragment's size never bounds what these tests observe.
window_test_state(Opts) ->
    FragRef = frag_ref(0, 100_000_000, 42),
    Iterator = mock_iterator([{0, 100_000_000, 42}]),
    init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator, Opts#{max_depth => 32}).

%% ------------------------------------------------------------------
%% Pipelining: several ranges of one fragment in flight at once
%% ------------------------------------------------------------------

pipeline_fills_to_window_and_depth(_Config) ->
    %% The window bounds the bytes outstanding and max_depth bounds the number
    %% of requests; whichever binds first stops the frontier.
    {ByWindow, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    ?assertEqual(
        [{8, 1007}, {1008, 2007}, {2008, 3007}, {3008, 4007}],
        fragment_ranges(ByWindow, 0)
    ),
    {ByDepth, _} = pipelined_state(#{window_max => 1_000_000, max_depth => 3}),
    ?assertEqual(3, length(fragment_ranges(ByDepth, 0))).

out_of_order_arrival_is_reassembled(_Config) ->
    %% Responses to concurrent range requests interleave, but the read buffer
    %% only takes contiguous appends. Bytes for a range whose predecessors are
    %% unfinished are held back and appended once it reaches the head.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    %% Answer the third and second ranges; nothing can be served yet.
    {S1, _} = deliver(S0, 0, 2008, pattern(2008, 1000), done),
    {S2, E2} = read(S1, 8, 3000),
    ?assertEqual([], [E || {reply, _} = E <- E2]),
    {S3, _} = deliver(S2, 0, 1008, pattern(1008, 1000), done),
    {S4, E4} = read(S3, 8, 3000),
    ?assertEqual([], [E || {reply, _} = E <- E4]),
    %% The first range closes the gap, and all three flush in order.
    {_S5, E5} = deliver(S4, 0, 8, pattern(8, 1000), done),
    ?assertEqual([{reply, {ok, pattern(8, 3000)}}], [E || {reply, _} = E <- E5]).

mid_pipeline_error_reissues_only_that_range(_Config) ->
    %% One range failing must not disturb the others, and the retry must re-issue
    %% exactly the failed range rather than the frontier.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    Before = fragment_ranges(S0, 0),
    {S1, E1} = fail(S0, 0, 1008, slow_down),
    ?assertMatch([{set_timer, fault, 1000}], E1),
    ?assertEqual(Before, fragment_ranges(S1, 0)),
    {_S2, E2} = retry(S1, fault),
    ?assertEqual(
        [{start_request, key(), {1008, 2007}, 0}],
        [E || {start_request, _, _, _} = E <- E2]
    ).

failing_batch_arms_one_timer(_Config) ->
    %% Every range failing at once must arm a single retry timer: one per
    %% failure would drive a retry pass each and multiply the backoff.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    {S, Timers} = lists:foldl(
        fun({Start, _End}, {Acc, TimerAcc}) ->
            {Acc1, Effects} = fail(Acc, 0, Start, slow_down),
            {Acc1, TimerAcc ++ [E || {set_timer, _, _} = E <- Effects]}
        end,
        {S0, []},
        fragment_ranges(S0, 0)
    ),
    ?assertEqual([{set_timer, fault, 1000}], Timers),
    %% The one retry puts every failed range back in flight.
    {S1, E} = retry(S, fault),
    ?assertEqual(4, length([X || {start_request, _, _, _} = X <- E])),
    %% One fault failed four ranges but cost one doubling, not four. Doubling per
    %% range would put the next round at 16s (or the 30s cap) after a single
    %% blip, most of a read deadline spent waiting on nothing.
    {_S2, E2} = fail(S1, 0, slow_down),
    ?assertEqual([{set_timer, fault, 2000}], [X || {set_timer, _, _} = X <- E2]).

fully_delivered_range_failing_before_fin_is_dropped(_Config) ->
    %% A range can deliver every byte it owes and still be in flight, waiting on
    %% the frame that closes it. If it then fails there is nothing to retry:
    %% restarting it at the last flushed byte would leave `range_start` one past
    %% `range_end`, which is both a backwards range to ask S3 for and, because
    %% ranges are contiguous, a duplicate of its successor's key - so the
    %% successor's bytes would be routed to the dead request and dropped.
    {S0, _} = pipelined_state(#{window_max => 8000, max_depth => 4}),
    ?assertEqual([{8, 1007}, {1008, 2007}, {2008, 3007}, {3008, 4007}], fragment_ranges(S0, 0)),
    %% The whole first range arrives, but the response does not close.
    {S1, _} = deliver(S0, 0, 8, pattern(8, 1000), continue),
    {S2, E2} = fail(S1, 0, 8, connection_error),
    %% Nothing failed to arrive, so nothing is re-requested and no backoff runs.
    %% The depth slot it held is freed, which lets the frontier extend.
    ?assertEqual([], [X || {set_timer, _, _} = X <- E2]),
    ?assertEqual([{1008, 2007}, {2008, 3007}, {3008, 4007}, {4008, 5007}], fragment_ranges(S2, 0)),
    %% The successor's bytes still reach the buffer, and the read is served.
    {S3, _} = deliver(S2, 0, 1008, pattern(1008, 1000), done),
    {_S4, E4} = read(S3, 8, 2000),
    ?assertEqual([{reply, {ok, pattern(8, 2000)}}], [X || {reply, _} = X <- E4]).

open_range_keeps_flushing_its_successors(_Config) ->
    %% The same range that owes nothing while its closing frame is outstanding,
    %% but left in the queue rather than failed. It stays at the head of its
    %% fragment, and the requests behind it flush past it - which moves the
    %% buffer's end beyond the byte that range flushed up to. Deciding whether
    %% the head can flush by comparing those two positions therefore called it
    %% blocked from that moment on, and since a blocked head skips the rest of
    %% its fragment's queue, no byte of the fragment could ever reach the buffer
    %% again. Reads stalled until the deadline, and the closing frame arriving
    %% did not release them: the head was just as blocked once complete.
    {S0, _} = pipelined_state(#{window_max => 8000, max_depth => 4}),
    ?assertEqual([{8, 1007}, {1008, 2007}, {2008, 3007}, {3008, 4007}], fragment_ranges(S0, 0)),
    %% The head delivers everything it owes without closing, and its successor
    %% flushes past it.
    {S1, _} = deliver(S0, 0, 8, pattern(8, 1000), continue),
    {S2, _} = deliver(S1, 0, 1008, pattern(1008, 1000), done),
    ?assertEqual([{8, 1007}, {2008, 3007}, {3008, 4007}, {4008, 5007}], fragment_ranges(S2, 0)),
    %% Everything after it must still flush, on this delivery and on the next.
    {S3, _} = deliver(S2, 0, 2008, pattern(2008, 1000), done),
    {S4, E4} = read(S3, 8, 3000),
    ?assertEqual([{reply, {ok, pattern(8, 3000)}}], [X || {reply, _} = X <- E4]),
    %% Closing the head at last drops it and leaves the queue readable.
    {S5, _} = deliver(S4, 0, 8, <<>>, done),
    ?assertEqual([], [R || {_, 8, 1007} = R <- outstanding_ranges(S5)]),
    {S6, _} = deliver(S5, 0, 3008, pattern(3008, 1000), done),
    {_S7, E7} = read(S6, 3008, 1000),
    ?assertEqual([{reply, {ok, pattern(3008, 1000)}}], [X || {reply, _} = X <- E7]).

open_range_does_not_pull_the_frontier_back(_Config) ->
    %% Where the next range starts cannot be read off the queue alone. A range
    %% that owes nothing stays queued until its closing frame arrives while the
    %% ranges behind it flush and are dropped, so the highest range end left in
    %% the queue can sit *below* the buffer's end. Taking the queue's answer
    %% then walked the frontier backwards and re-requested bytes the buffer
    %% already held - and that duplicate could never flush (its start is behind
    %% the buffer's end for good), so it wedged the queue for the rest of the
    %% fragment on top of wasting the fetch.
    {S0, _} = pipelined_state(#{window_max => 2000, max_depth => 4}),
    ?assertEqual([{8, 1007}, {1008, 2007}], fragment_ranges(S0, 0)),
    %% The head owes nothing but stays open; its successor delivers, flushes
    %% past it and is dropped. The queue now ends at 1007, the buffer at 2008.
    {S1, _} = deliver(S0, 0, 8, pattern(8, 1000), continue),
    {S2, _} = deliver(S1, 0, 1008, pattern(1008, 1000), done),
    ?assertEqual([{8, 1007}], fragment_ranges(S2, 0)),
    %% The consumer walks through the buffered bytes.
    {S3, E3} = read(S2, 8, 1000),
    ?assertEqual([{reply, {ok, pattern(8, 1000)}}], [X || {reply, _} = X <- E3]),
    {S4, E4} = read(S3, 1008, 1000),
    ?assertEqual([{reply, {ok, pattern(1008, 1000)}}], [X || {reply, _} = X <- E4]),
    %% Reading on past them is what asks for the next range. It must start at
    %% the buffer's end, not after the stale queue entry - asking for 1008 again
    %% would re-fetch bytes the buffer already holds.
    {S5, _} = read(S4, 2008, 1000),
    ?assertEqual([{8, 1007}, {2008, 3007}], fragment_ranges(S5, 0)),
    %% And it flushes, so the fragment stays readable.
    {S6, _} = deliver(S5, 0, 2008, pattern(2008, 1000), done),
    {_S7, E7} = read(S6, 2008, 1000),
    ?assertEqual([{reply, {ok, pattern(2008, 1000)}}], [X || {reply, _} = X <- E7]).

short_completion_refetches_the_gap(_Config) ->
    %% A response that ends before its range does must not leave a hole: the
    %% missing bytes are requested again, and the ranges queued behind it keep
    %% the bytes they have already received.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    {S1, _} = deliver(S0, 0, 1008, pattern(1008, 1000), done),
    {S2, _} = deliver(S1, 0, 8, pattern(8, 400), done),
    %% The delivered prefix is complete and already in the buffer, so what is
    %% left in the queue is the gap ahead of the untouched successors - including
    %% the one whose bytes have arrived but cannot flush until the gap closes.
    ?assertEqual([{408, 1007}, {1008, 2007}, {2008, 3007}, {3008, 4007}], fragment_ranges(S2, 0)),
    %% Filling the gap releases the bytes staged behind it too.
    {S3, _} = deliver(S2, 0, 408, pattern(408, 600), done),
    {_S4, E4} = read(S3, 8, 2000),
    ?assertEqual([{reply, {ok, pattern(8, 2000)}}], [X || {reply, _} = X <- E4]).

short_completion_at_the_frontier_refetches_the_gap(_Config) ->
    %% The same hole, but in the last range of a fragment, with the window
    %% already full of the next fragment's prefetch. Leaving the gap to the
    %% frontier does not work here: `extend_frontier/1` is window-gated and a
    %% short completion frees only the bytes that never came, so a response
    %% short by less than one request leaves no room to ask for them again. The
    %% hole would then never close - the fragment can never be read to its end,
    %% so the transition that would free the next fragment's bytes can never
    %% happen either - and every read behind it would stall until the deadline.
    FragRef = frag_ref(0, 2000, 42),
    Iterator = mock_iterator([{0, 2000, 42}, {100, 100_000, 43}]),
    Opts = #{request_size => 1000, window_max => 2000, max_depth => 8},
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator, Opts),
    %% A miss takes the window to its ceiling, which covers the whole fragment.
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 3000),
    ?assertEqual([{8, 1007}, {1008, 2007}], fragment_ranges(S1, 0)),
    {S2, _} = deliver(S1, 0, 8, pattern(8, 1000), done),
    %% Serving a read advances the consumer, which frees window for the spill
    %% into the next fragment. The window is now full and every byte of the
    %% current fragment has been asked for.
    {S3, _} = read(S2, 1000, 8),
    ?assertEqual([{8, 1007}], fragment_ranges(S3, 100)),
    %% The fragment's last range comes back one byte short.
    {S4, E4} = deliver(S3, 0, 1008, pattern(1008, 999), done),
    ?assertEqual([{2007, 2007}], fragment_ranges(S4, 0)),
    ?assertEqual(
        [{start_request, key(), {2007, 2007}, 0}],
        [E || {start_request, _, _, _} = E <- E4]
    ),
    %% The byte arrives and the fragment can be read to its end.
    {S5, _} = deliver(S4, 0, 2007, pattern(2007, 1), done),
    {_S6, E6} = read(S5, 1008, 1000),
    ?assertEqual([{reply, {ok, pattern(1008, 1000)}}], [E || {reply, _} = E <- E6]).

empty_completion_backs_off(_Config) ->
    %% A response that closes without a byte would otherwise be re-issued
    %% immediately, spinning against whatever is answering that way.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    {S1, E1} = deliver(S0, 0, 8, <<>>, done),
    ?assertEqual([{set_timer, fault, 1000}], [E || {set_timer, _, _} = E <- E1]),
    ?assertEqual([], [E || {start_request, _, _, _} = E <- E1]),
    ?assertEqual({8, 1007}, hd(fragment_ranges(S1, 0))).

over_delivery_is_clipped(_Config) ->
    %% A backend that answers with more than the range asked for must not push
    %% this request's bytes over its successor's range and have them appended
    %% twice.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    {S1, _} = deliver(S0, 0, 8, pattern(8, 2500), done),
    {S2, _} = deliver(S1, 0, 1008, pattern(1008, 1000), done),
    {_S3, E3} = read(S2, 8, 2000),
    ?assertEqual([{reply, {ok, pattern(8, 2000)}}], [E || {reply, _} = E <- E3]).

stale_data_for_dropped_range_is_ignored(_Config) ->
    %% A range cancelled by a read deadline can still have frames in flight.
    %% Appending those bytes would corrupt the buffer's addressing.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    {S1, _} = read(S0, 8, 100),
    {S2, E2} = rabbitmq_stream_s3_remote_reader_core:step(S1, deadline_expired),
    ?assertMatch(
        [{cancel_requests, all}, {cancel_timers, all}, {reply, {error, timeout}}], E2
    ),
    ?assertEqual([], fragment_ranges(S2, 0)),
    {S3, E3} = deliver(S2, 0, 1008, pattern(1008, 1000), done),
    ?assertEqual([], [E || {reply, _} = E <- E3]),
    %% The reader refills from the end of the buffer it kept - here the position
    %% it started at, since nothing had been delivered - and keeps nothing of the
    %% stale range, so a read still has to wait.
    ?assertMatch([{?SEGMENT_HEADER_B, _} | _], fragment_ranges(S3, 0)),
    {_S4, E4} = read(S3, 8, 100),
    ?assertEqual([], [E || {reply, _} = E <- E4]).

spill_stops_one_fragment_ahead(_Config) ->
    %% The frontier spills into the next fragment once the current one is fully
    %% spoken for, but never further: the reader holds one prefetched fragment.
    FragRef = frag_ref(0, 500, 42),
    Iterator = mock_iterator([{0, 500, 42}, {100, 500, 43}, {200, 500, 44}]),
    Opts = #{request_size => 1000, window_max => 1_000_000, max_depth => 32},
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator, Opts),
    %% The placement pass stays inside the current fragment
    %% (`placement_pass_does_not_look_ahead`); the read that follows spills. The
    %% miss it takes leaves room for a third fragment, so the queue stopping at
    %% the second is the memo's doing rather than the window's.
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 3000),
    ?assertEqual([{8, 507}], fragment_ranges(S1, 0)),
    ?assertEqual([{8, 507}], fragment_ranges(S1, 100)),
    ?assertEqual([], fragment_ranges(S1, 200)).

window_ceiling_below_the_request_size_is_clamped(_Config) ->
    %% `prefetch_window_max` and `prefetch_request_size` are independent
    %% settings with no schema to reject a ceiling below the floor. Unclamped,
    %% the window starts at one request and `note_miss/1`'s
    %% `min(WindowMax, Window * 2)` then *shrinks* it on a miss - the opposite
    %% of what a miss means - and never admits a second range.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    Opts = #{request_size => 1000, window_max => 100},
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator, Opts),
    ?assertEqual(1000, window(S0)),
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 5000),
    ?assert(window(S1) >= 1000).

next_fragment_404_refresh_reuses_the_peek(_Config) ->
    %% Deciding which offset to refresh past means naming the fragment that
    %% 404'd, which is the one the look-ahead already resolved - it is why the
    %% reader asked for it. Asking the iterator again spends a second group GET
    %% inside the core, blocking the reader while the caller's deadline burns.
    Fetches = counters:new(1, []),
    Iterator = mock_iterator_counting_groups([{0, 500, 42}], [{100, 500, 43}], Fetches),
    {S0, _} = init(stream_id(), frag_ref(0, 500, 42), ?SEGMENT_HEADER_B, Iterator, #{
        request_size => 1000
    }),
    %% The read spills into the next fragment, which resolves the peek.
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 50),
    ?assertEqual(1, counters:get(Fetches, 1)),
    %% That prefetch 404s, and the consumer then reads past the current
    %% fragment, which is what asks for the refresh.
    {S2, _} = fail(S1, 100, not_found),
    S3 = serve_all(S2, 0),
    {_S4, E} = read(S3, ?SEGMENT_HEADER_B + 500, 50),
    ?assertEqual([{refresh_iterator, 100}], [Eff || {refresh_iterator, _} = Eff <- E]),
    ?assertEqual(1, counters:get(Fetches, 1)).

placement_pass_does_not_look_ahead(_Config) ->
    %% `init/5` runs in the consumer process - the shell calls it from
    %% `gen_server:init/1`, and `gen_server:start/3` holds the consumer until it
    %% returns - so the placement pass must not resolve the next-fragment peek:
    %% that descends into a group object, which is a synchronous S3 GET. A
    %% consumer attaching to a fragment shorter than one request is the case
    %% that reaches it: the current fragment is fully spoken for by the first
    %% range, so the frontier is ready to spill.
    Fetches = counters:new(1, []),
    Iterator = mock_iterator_counting_groups([{0, 100, 42}], [{2000, 1000, 7}], Fetches),
    {S0, E0} = init(stream_id(), frag_ref(0, 100, 42), ?SEGMENT_HEADER_B, Iterator),
    ?assertEqual(0, counters:get(Fetches, 1)),
    ?assertEqual([0], [F || {start_request, _, _, F} <- E0]),
    %% The first read is what spills, from the reader process.
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 50),
    ?assertEqual(1, counters:get(Fetches, 1)),
    ?assertMatch([{0, _, _}, {2000, _, _}], outstanding_ranges(S1)).

next_fragment_flushes_while_current_range_streams(_Config) ->
    %% Each fragment's queue is independent. A current-fragment range that is
    %% still streaming blocks only the ranges behind it in its own fragment; the
    %% prefetched next fragment must keep filling its own buffer rather than
    %% holding its bytes staged until the current fragment closes.
    FragRef = frag_ref(0, 2000, 42),
    Iterator = mock_iterator([{0, 2000, 42}, {100, 100_000, 43}]),
    Opts = #{request_size => 1000, window_max => 4000, max_depth => 8},
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator, Opts),
    %% Two misses grow the window past the current fragment, so the frontier
    %% spills into the next one.
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 3000),
    {S2, _} = read(S1, ?SEGMENT_HEADER_B, 3000),
    ?assertEqual([{8, 1007}, {1008, 2007}], fragment_ranges(S2, 0)),
    ?assertEqual([{8, 1007}, {1008, 2007}], fragment_ranges(S2, 100)),
    %% The current fragment's head range is still streaming.
    {S3, _} = deliver(S2, 0, 8, pattern(8, 500), continue),
    %% The next fragment's head range completes. Its bytes reach the prefetch
    %% buffer, so the request is done with and leaves the queue.
    {S4, _} = deliver(S3, 100, 8, pattern(8, 1000), done),
    ?assertEqual([{1008, 2007}], fragment_ranges(S4, 100)),
    ?assertEqual([{8, 1007}, {1008, 2007}], fragment_ranges(S4, 0)).

%% A reader with several ranges of one fragment outstanding. The window has to
%% be grown by misses first, which is what a consumer outrunning the reader
%% does; ten reads take it to its ceiling for any window_max these tests use.
%%
%% The reads are small on purpose. Any unservable read grows the window, and a
%% large one would additionally raise the fetch ceiling to its own size (see
%% `fetch_ceiling/1`) - which is correct, but would mean these tests observed a
%% frontier set by the read rather than by the window they are about.
pipelined_state(Opts) ->
    FragRef = frag_ref(0, 100_000_000, 42),
    Iterator = mock_iterator([{0, 100_000_000, 42}]),
    {S0, _} = init(
        stream_id(),
        FragRef,
        ?SEGMENT_HEADER_B,
        Iterator,
        maps:merge(#{request_size => 1000}, Opts)
    ),
    lists:foldl(
        fun(_, {Acc, _}) -> read(Acc, ?SEGMENT_HEADER_B, 100) end,
        {S0, []},
        lists:seq(1, 10)
    ).

key() ->
    rabbitmq_stream_s3:fragment_key(stream_id(), 0, 42).

exponential_backoff_caps_at_max(_Config) ->
    %% Repeated errors double the delay up to 30_000ms max.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Fire enough errors to exceed 30s cap: 1000, 2000, 4000, 8000, 16000, 32000→30000
    {S1, [{set_timer, fault, 1000}]} = fail_then_retry(S0, 0, slow_down),
    {S2, [{set_timer, fault, 2000}]} = fail_then_retry(S1, 0, slow_down),
    {S3, [{set_timer, fault, 4000}]} = fail_then_retry(S2, 0, slow_down),
    {S4, [{set_timer, fault, 8000}]} = fail_then_retry(S3, 0, slow_down),
    {S5, [{set_timer, fault, 16000}]} = fail_then_retry(S4, 0, slow_down),
    %% Next would be 32000 but capped at 30000.
    {_S6, [{set_timer, fault, 30000}]} = fail_then_retry(S5, 0, slow_down),
    ok.

fatal_error_emits_stop(_Config) ->
    %% A non-retryable error reports the reason (for log + metric) and then
    %% stops. The report effect must precede stop so the shutdown is not silent.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    {_S1, Effects} = fail(S0, 0, {unexpected, boom}),
    ?assertEqual([{fatal_error, {unexpected, boom}}, stop], Effects).

fatal_error_reports_reason_before_stop(_Config) ->
    %% The real-world trigger: a 403 AccessDenied (e.g. credential/policy
    %% change) mid-read. The reason must be carried in the report effect so
    %% the shell can log it and bump the fatal-error metric, rather than the
    %% reader stopping silently.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    {_S1, Effects} = fail(S0, 0, access_denied),
    ?assertEqual([{fatal_error, access_denied}, stop], Effects).

multi_chunk_data_accumulation(_Config) ->
    %% Data arrives in 3 chunks (continue, continue, done). Read served after all arrive.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Issue read first.
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 300, chunk_boundary}),
    %% Data arrives in 3 pieces of 100 bytes each.
    Chunk = binary:copy(<<$A>>, 100),
    {S2, E1} = deliver(S1, 0, Chunk, continue),
    ?assertEqual([], [E || {reply, _} = E <- E1]),
    {S3, E2} = deliver(S2, 0, Chunk, continue),
    ?assertEqual([], [E || {reply, _} = E <- E2]),
    {_S4, E3} = deliver(S3, 0, Chunk, done),
    Replies = [E || {reply, _} = E <- E3],
    ?assertMatch([{reply, {ok, _}}], Replies),
    [{reply, {ok, ResultData}}] = Replies,
    ?assertEqual(300, byte_size(ResultData)).

%% ------------------------------------------------------------------
%% Fragment navigation tests
%% ------------------------------------------------------------------

retryable_error_preserves_co_pending_request(_Config) ->
    %% A retryable request error must put back only the failed range, leaving
    %% every co-pending request in flight. Wiping them orphaned a live request:
    %% the core forgot it, the retry re-issued it, the shell overwrote the
    %% still-live original, and the pooled connection leaked. Here fragment 0's
    %% range and the prefetch of fragment 100 are both in flight; an error on 0
    %% must leave 100 untouched, so the retry does not duplicate it.
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% The first read spills the frontier into fragment 100, so both are in
    %% flight when the error arrives.
    {S1, E1} = read(S0, 64, 50),
    ?assertMatch([{_, _, 100}], [{K, R, F} || {start_request, K, R, F} <- E1, F =:= 100]),
    %% Fragment 0's request errors (retryable); only its range is put back.
    {S2, _} = fail(S1, 0, slow_down),
    ?assertEqual([{?SEGMENT_HEADER_B, ?SEGMENT_HEADER_B + 500 - 1}], fragment_ranges(S2, 100)),
    %% On retry, the surviving prefetch of fragment 100 must NOT be re-issued.
    {_S3, E3} = retry(S2, fault),
    ?assertEqual([0], [F || {start_request, _, _, F} <- E3]).

prefetch_next_fragment_triggered(_Config) ->
    %% Once every byte of the current fragment has been asked for, the frontier
    %% spills into the next fragment rather than waiting for the current one to
    %% arrive first. Fragment 0 is 200 bytes, so one request covers it and the
    %% first read's pass issues the prefetch.
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    {_S1, Effects} = read(S0, 64, 50),
    NextRequests = [R || {start_request, _, R, F} <- Effects, F =:= 100],
    %% The range is clamped to the next fragment's own data region. Asking past
    %% it (the old behaviour) would draw a short response that, under the
    %% re-request rule for short completions, could never be satisfied.
    ?assertEqual([{?SEGMENT_HEADER_B, ?SEGMENT_HEADER_B + 500 - 1}], NextRequests).

fragment_transition_without_prefetch_awaits(_Config) ->
    %% When next fragment data is not yet available, core awaits (no reply).
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Fill current fragment but do NOT provide next fragment data.
    Data = binary:copy(<<0>>, 200),
    {S1, _} = deliver(S0, 0, Data, done),
    %% Read past end of current fragment.
    {_S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {read, 264, 50, chunk_boundary}
    ),
    %% No reply — awaiting next fragment data.
    Replies = [E || {reply, _} = E <- Effects],
    ?assertEqual([], Replies).

read_at_exact_fragment_boundary(_Config) ->
    %% Read at offset == SEGMENT_HEADER_B + FragSize triggers transition.
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Fill current + prefetch next.
    Data = binary:copy(<<0>>, 200),
    {S1, _} = deliver(S0, 0, Data, done),
    NextData = binary:copy(<<1>>, 500),
    {S2, _} = deliver(S1, 100, NextData, done),
    %% Read at exactly 64 + 200 = 264 (the boundary).
    {_S3, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {read, 264, 50, chunk_boundary}
    ),
    ?assertMatch([{reply, {next_fragment, 100}} | _], Effects).

mid_fragment_init_position(_Config) ->
    %% When init position > SEGMENT_HEADER_B, first request starts at that position.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    MidPos = 5000,
    {_S0, Effects} = init(stream_id(), FragRef, MidPos, Iterator),
    ?assertMatch([{start_request, _, {5000, _}, 0}], Effects).

fragment_404_advances_to_next_fragment(_Config) ->
    %% 404 on current fragment → refresh_iterator → iterator_refreshed with
    %% next fragment → reply with next_fragment + new request started.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}, {500, 2_000_000, 99}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Issue read, then 404.
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 100, chunk_boundary}),
    {S2, [{cancel_requests, all}, {refresh_iterator, 0}]} = fail(S1, 0, not_found),
    %% Shell refreshes iterator past offset 0. The iterator's next entry
    %% is fragment 500. Build an iterator where next/1 returns fragment 500.
    Manifest500 = build_manifest([{500, 2_000_000, 99}]),
    GetGroupFun = fun(_) -> {error, not_found} end,
    NewIterator = rabbitmq_stream_s3_fragment_iterator:init(Manifest500, 0, GetGroupFun),
    {_S3, E1} = rabbitmq_stream_s3_remote_reader_core:step(S2, {iterator_refreshed, NewIterator}),
    %% Should reply with next_fragment and start a request for the new fragment.
    ?assertMatch(
        [{cancel_requests, all}, {cancel_timers, all}, {reply, {next_fragment, 500}} | _], E1
    ),
    Requests = [F || {start_request, _, _, F} <- E1],
    ?assertEqual([500], Requests).

retry_resets_delay_on_success(_Config) ->
    %% After backoff escalates, successful data resets delay to minimum.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Escalate: 1s, 2s, 4s.
    {S1, [{set_timer, fault, 1000}]} = fail_then_retry(S0, 0, slow_down),
    {S2, [{set_timer, fault, 2000}]} = fail_then_retry(S1, 0, slow_down),
    {S3, [{set_timer, fault, 4000}]} = fail_then_retry(S2, 0, slow_down),
    %% Data arrives successfully.
    Data = binary:copy(<<0>>, 1024),
    {S4, _} = deliver(S3, 0, Data, done),
    %% Next error should start back at 1000ms (reset).
    {_S5, [{set_timer, fault, 1000}]} = fail(S4, 0, slow_down),
    ok.

read_larger_than_buffer_awaits(_Config) ->
    %% Request more bytes than are buffered. Core awaits, does not crash or
    %% return partial data.
    FragRef = frag_ref(0, 10_000_000, 42),
    Iterator = mock_iterator([{0, 10_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Provide only 100 bytes.
    Data = binary:copy(<<0>>, 100),
    {S1, _} = deliver(S0, 0, Data, done),
    %% Request 5000 bytes at position 64. Only 100 available.
    {_S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {read, 64, 5000, chunk_boundary}
    ),
    %% No reply — awaiting more data. A new request may or may not be emitted
    %% (depends on whether one was already started when data arrived).
    Replies = [E || {reply, _} = E <- Effects],
    ?assertEqual([], Replies).

header_overread_capped_at_index_boundary(_Config) ->
    %% The log reader over-reads headers (64 + filter bytes). When the buffer
    %% extends past the index boundary, the core caps the returned data at
    %% the index start. SEGMENT_HEADER_B = 8, so IdxStartPos = 8 + FragSize.
    FragSize = 500,
    FragRef = frag_ref(0, FragSize, 42),
    Iterator = mock_iterator([{0, FragSize, 42}]),
    %% Start at position 8 (real SEGMENT_HEADER_B).
    {S0, _} = init(stream_id(), FragRef, 8, Iterator),
    %% Fill buffer to cover the full fragment data region (8 + 500 = 508).
    Data = binary:copy(<<0>>, FragSize),
    {S1, _} = deliver(S0, 0, Data, done),
    %% Read 200 bytes starting at position 400. IdxStartPos = 508.
    %% Requested end = 600 > EndPos = 508. Cap path fires.
    %% Final cap: min(508 - 8 - 400 + 8, 508 - 400) = min(108, 108) = 108.
    %% Actually: cap to EndPos - Offset = 508 - 400 = 108, then
    %% final clause: min(108, IdxStartPos - Offset) = min(108, 508 - 400) = 108.
    {_S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {read, 400, 200, chunk_boundary}
    ),
    Replies = [E || {reply, _} = E <- Effects],
    ?assertMatch([{reply, {ok, _}}], Replies),
    [{reply, {ok, ResultData}}] = Replies,
    ?assertEqual(108, byte_size(ResultData)).

tail_header_overread_below_guard_serves_remaining(_Config) ->
    %% Regression: a small final chunk at the fragment tail must still be
    %% served. The consumer over-reads a chunk header at the last chunk's
    %% boundary; when the remaining chunk data (plus index) is smaller than the
    %% over-read size, the read overshoots the buffered end. Because all chunk
    %% data is buffered (EndPos >= IdxStartPos), the core must cap at the index
    %% boundary and serve the remaining bytes, not await data that will never
    %% arrive (which hung the consumer until timeout).
    %%
    %% FragSize = 500, SEGMENT_HEADER_B = 8, so IdxStartPos = 508. Fill the full
    %% chunk-data region (EndPos = 508). Read at offset 460 (a last-chunk
    %% boundary leaving 48 bytes of chunk data) for a 64-byte header over-read:
    %% 460 + 64 = 524 > EndPos = 508, so the over-read branch fires. With the
    %% old `Offset + 64 =< EndPos` guard this awaited (524 > 508); now it serves
    %% the 48 remaining bytes (508 - 460).
    FragSize = 500,
    FragRef = frag_ref(0, FragSize, 42),
    Iterator = mock_iterator([{0, FragSize, 42}]),
    {S0, _} = init(stream_id(), FragRef, 8, Iterator),
    Data = binary:copy(<<0>>, FragSize),
    {S1, _} = deliver(S0, 0, Data, done),
    {_S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {read, 460, 64, chunk_boundary}
    ),
    Replies = [E || {reply, _} = E <- Effects],
    ?assertMatch([{reply, {ok, _}}], Replies),
    [{reply, {ok, ResultData}}] = Replies,
    ?assertEqual(48, byte_size(ResultData)).

next_fragment_404_triggers_refresh_iterator(_Config) ->
    %% Prefetch of next fragment returns 404. When the consumer reads past
    %% the current fragment, the core triggers a refresh_iterator past the
    %% 404'd fragment's offset.
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 8, Iterator),
    %% Fill current fragment.
    Data = binary:copy(<<0>>, 200),
    {S1, _} = deliver(S0, 0, Data, done),
    %% Next fragment prefetch returns 404 (fragment 100, not current fragment 0).
    {S2, _} = fail(S1, 100, not_found),
    %% Read past end of current fragment. Next is not_found → refresh past offset 100.
    {_S3, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {read, 208, 50, chunk_boundary}
    ),
    ?assertMatch([{refresh_iterator, 100}], Effects).

prefetch_404_full_recovery(_Config) ->
    %% Full cycle: prefetch of next fragment returns 404, consumer reads past
    %% current fragment, refresh returns a surviving fragment further ahead,
    %% consumer continues reading from that fragment.
    FragRef = frag_ref(0, 200, 42),
    %% Three fragments: 0, 100, 200. Fragment 100 will be deleted by retention.
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}, {200, 300, 44}]),
    {S0, _} = init(stream_id(), FragRef, 8, Iterator),
    %% Fill current fragment.
    Data = binary:copy(<<0>>, 200),
    {S1, _} = deliver(S0, 0, Data, done),
    %% Prefetch of fragment 100 returns 404.
    {S2, _} = fail(S1, 100, not_found),
    %% Consumer reads past current fragment → refresh_iterator.
    {S3, E1} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {read, 208, 50, chunk_boundary}
    ),
    ?assertMatch([{refresh_iterator, 100}], E1),
    %% Shell provides a refreshed iterator starting at fragment 200.
    RefreshedManifest = build_manifest([{200, 300, 44}]),
    GetGroupFun = fun(_) -> {error, not_found} end,
    RefreshedIterator = rabbitmq_stream_s3_fragment_iterator:init(
        RefreshedManifest, 0, GetGroupFun
    ),
    {S4, E2} = rabbitmq_stream_s3_remote_reader_core:step(
        S3, {iterator_refreshed, RefreshedIterator}
    ),
    %% Core replies with next_fragment pointing to 200 and starts a request.
    ?assertMatch(
        [{cancel_requests, all}, {cancel_timers, all}, {reply, {next_fragment, 200}} | _], E2
    ),
    Requests = [F || {start_request, _, _, F} <- E2],
    ?assertEqual([200], Requests),
    %% Data arrives for fragment 200. Consumer can read from it.
    NextData = binary:copy(<<1>>, 300),
    {S5, _} = deliver(S4, 200, NextData, done),
    {_S6, E3} = rabbitmq_stream_s3_remote_reader_core:step(
        S5, {read, 8, 50, chunk_boundary}
    ),
    ?assertMatch([{reply, {ok, _}} | _], E3).

deadline_expired_replies_error_timeout(_Config) ->
    %% When the shell fires deadline_expired, the core replies {error, timeout}
    %% and clears the pending read.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Issue a read (pending, no data yet).
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 100, chunk_boundary}),
    ?assertMatch({64, 100}, rabbitmq_stream_s3_remote_reader_core:pending(S1)),
    %% Deadline fires.
    {S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(S1, deadline_expired),
    ?assertMatch(
        [{cancel_requests, all}, {cancel_timers, all}, {reply, {error, timeout}}], Effects
    ),
    %% Pending is cleared.
    ?assertEqual(undefined, rabbitmq_stream_s3_remote_reader_core:pending(S2)).

deadline_expired_keeps_the_buffer_for_the_retry(_Config) ->
    %% The deadline drops what is in flight, not what has already been buffered:
    %% those bytes are a contiguous run of the current fragment that nothing in
    %% flight contributed to, so they are as valid after the deadline as before
    %% it. The log reader's retry restarts at the chunk header it last read,
    %% which is a read the buffer answers outright, and the frontier resumes at
    %% the buffer's end rather than at the fragment's first byte.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    {S1, _} = deliver(S0, 0, pattern(64, 5000), continue),
    {S2, E1} = read(S1, 64, 100),
    ?assertMatch([{reply, {ok, _}} | _], E1),
    %% A read past everything buffered is pending when the deadline fires.
    {S3, _} = read(S2, 5000, 1000),
    {S4, _} = rabbitmq_stream_s3_remote_reader_core:step(S3, deadline_expired),
    ?assertEqual([], outstanding_ranges(S4)),
    %% The retry re-reads the chunk header, behind the read that timed out.
    {S5, E5} = read(S4, 64, 100),
    ?assertEqual([{reply, {ok, pattern(64, 100)}}], [E || {reply, _} = E <- E5]),
    %% Nothing the consumer has already read through is fetched again.
    ?assertMatch([{5064, _} | _], fragment_ranges(S5, 0)),
    ?assertEqual([], [Range || {start_request, _, {Start, _} = Range, _} <- E5, Start < 5064]).

deadline_expired_at_a_fragment_boundary_refetches_nothing(_Config) ->
    %% The read that timed out was past the current fragment's data region,
    %% waiting on the next fragment's prefetch. The current fragment is fully
    %% buffered, so the retry has to ask for the next fragment - not walk the
    %% frontier back over the fragment the consumer has finished with, which is
    %% a whole `fragment_target_size` of refetching before the range the read is
    %% actually waiting on is even requested.
    FragRef = frag_ref(0, 2000, 42),
    Iterator = mock_iterator([{0, 2000, 42}, {100, 100_000, 43}]),
    Opts = #{request_size => 1000, window_max => 4000, max_depth => 8},
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator, Opts),
    %% Buffer the fragment's whole data region: [8, 2008).
    {S1, _} = deliver(S0, 0, ?SEGMENT_HEADER_B, pattern(8, 1000), done),
    {S2, _} = read(S1, ?SEGMENT_HEADER_B, 1500),
    {S3, E3} = deliver(S2, 0, 1008, pattern(1008, 1000), done),
    ?assertEqual([{reply, {ok, pattern(8, 1500)}}], [E || {reply, _} = E <- E3]),
    %% The consumer reads past the fragment and waits on the transition.
    {S4, _} = read(S3, ?SEGMENT_HEADER_B + 2000, 100),
    {S5, _} = rabbitmq_stream_s3_remote_reader_core:step(S4, deadline_expired),
    {S6, E6} = read(S5, ?SEGMENT_HEADER_B + 2000, 100),
    ?assertEqual([], [F || {start_request, _, _, F} <- E6, F =:= 0]),
    ?assertEqual([], fragment_ranges(S6, 0)),
    ?assertMatch([{?SEGMENT_HEADER_B, _} | _], fragment_ranges(S6, 100)).

deadline_expired_disowns_the_retry_timer(_Config) ->
    %% The deadline drops every outstanding request, so the timer armed for them
    %% is no longer anyone's. Keeping it marked pending would silence
    %% `arm_retry/3` for the next read's failures, which would then wait out a
    %% timer armed for requests that no longer exist - up to `max_retry_delay_ms`
    %% of a 40s read deadline spent with nothing in flight.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    {S1, E1} = fail(S0, 0, slow_down),
    ?assertEqual([{set_timer, fault, 1000}], [E || {set_timer, _, _} = E <- E1]),
    {S2, _} = rabbitmq_stream_s3_remote_reader_core:step(S1, deadline_expired),
    %% The next read starts a fresh request; failing it arms a timer again, at
    %% the reset minimum delay.
    {S3, _} = read(S2, 64, 100),
    {_S4, E4} = fail(S3, 0, slow_down),
    ?assertEqual([{set_timer, fault, 1000}], [E || {set_timer, _, _} = E <- E4]).

deadline_expired_clears_a_failed_look_ahead(_Config) ->
    %% The `failed` look-ahead memo is cleared by exactly one thing, the fault
    %% clock (`clear_failed_peek/2`), and the deadline disowns that clock. Left
    %% behind, the memo is permanent: `arm_peek_retry/4` only arms when the peek
    %% has just turned `failed`, so an already-`failed` one arms nothing and
    %% nothing else clears it. The frontier would stop spilling into the next
    %% fragment for the rest of the current one - the stall `arm_peek_retry/4`
    %% exists to prevent, reached through the deadline instead.
    Fetches = counters:new(1, []),
    Iterator = mock_iterator_counting_failing_group([{0, 1000, 42}], {2000, 0, 7}, Fetches),
    {S0, _} = init(stream_id(), frag_ref(0, 1000, 42), ?SEGMENT_HEADER_B, Iterator, #{
        request_size => 4000
    }),
    %% The look-ahead fails and arms its round.
    {S1, E1} = read(S0, ?SEGMENT_HEADER_B, 50),
    ?assertEqual(1, counters:get(Fetches, 1)),
    ?assertEqual([{set_timer, fault, 1000}], [E || {set_timer, _, _} = E <- E1]),
    %% The deadline takes that clock away with everything else it disowns.
    {S2, _} = rabbitmq_stream_s3_remote_reader_core:step(S1, deadline_expired),
    %% The retry's read re-attempts the fetch rather than finding the memo stuck,
    %% and arms a fresh round when it fails again.
    {_S3, E3} = read(S2, ?SEGMENT_HEADER_B, 50),
    ?assertEqual(2, counters:get(Fetches, 1)),
    ?assertEqual([{set_timer, fault, 1000}], [E || {set_timer, _, _} = E <- E3]).

deadline_expired_keeps_an_answered_look_ahead(_Config) ->
    %% Only a failed peek goes. An answered one is the truth about an iterator
    %% the deadline does not touch, so re-resolving it would spend a group GET -
    %% synchronous, inside the core - on an answer already in hand.
    Fetches = counters:new(1, []),
    Iterator = mock_iterator_counting_groups([{0, 1000, 42}], [{2000, 1000, 7}], Fetches),
    {S0, _} = init(stream_id(), frag_ref(0, 1000, 42), ?SEGMENT_HEADER_B, Iterator, #{
        request_size => 4000
    }),
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 50),
    ?assertEqual(1, counters:get(Fetches, 1)),
    {S2, _} = rabbitmq_stream_s3_remote_reader_core:step(S1, deadline_expired),
    {S3, _} = read(S2, ?SEGMENT_HEADER_B, 50),
    ?assertEqual(1, counters:get(Fetches, 1)),
    %% The prefetch the deadline dropped is re-issued from the memo.
    ?assertMatch([{2000, _, _} | _], [R || {2000, _, _} = R <- outstanding_ranges(S3)]).

deadline_expired_cancels_the_retry_timers(_Config) ->
    %% Disowning the armed timers is not enough on its own. A stale one carries
    %% the delay it was armed with - up to `max_retry_delay_ms` - so it can land
    %% part-way through a *later* backoff round, release that round's ranges
    %% before the pause S3 asked for has elapsed, and clear the kind from
    %% `timers` so the round's own timer is then taken for a fresh one. The
    %% shell is told to cancel them.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    {S1, _} = read(S0, 64, 100),
    {S2, E2} = fail(S1, 0, slow_down),
    ?assertEqual([{set_timer, fault, 1000}], [E || {set_timer, _, _} = E <- E2]),
    {_S3, E3} = rabbitmq_stream_s3_remote_reader_core:step(S2, deadline_expired),
    ?assertEqual(
        [{cancel_requests, all}, {cancel_timers, all}, {reply, {error, timeout}}], E3
    ).

deadline_expired_drops_the_prefetched_next_fragment(_Config) ->
    %% The deadline drops every outstanding request, so the bytes prefetched for
    %% the next fragment go with them: their own ranges are cancelled with the
    %% rest, but they keep counting against the prefetch window
    %% (`outstanding/1`). Holding window space that nothing is left to fetch into
    %% is what wedges the reader - with the window at its ceiling nothing has
    %% room to be issued however many misses follow, so every later read waits
    %% out a deadline of its own.
    FragRef = frag_ref(0, 2000, 42),
    Iterator = mock_iterator([{0, 2000, 42}, {100, 100_000, 43}]),
    Opts = #{request_size => 1000, window_max => 4000, max_depth => 8},
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator, Opts),
    %% Two misses grow the window past the current fragment, so the frontier
    %% spills into the next one, whose head range then completes.
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 3000),
    {S2, _} = read(S1, ?SEGMENT_HEADER_B, 3000),
    {S3, _} = deliver(S2, 100, 8, pattern(8, 1000), done),
    ?assertMatch({Outstanding, _} when Outstanding >= 1000, load(S3)),
    %% The deadline fires: nothing is in flight and nothing is buffered, so
    %% nothing may be counted against the window either.
    {S4, _} = rabbitmq_stream_s3_remote_reader_core:step(S3, deadline_expired),
    ?assertEqual({0, 0}, load(S4)),
    %% The reader is not wedged: the next read fetches the current fragment
    %% again.
    {_S5, E5} = read(S4, ?SEGMENT_HEADER_B, 100),
    ?assertMatch([0 | _], [F || {start_request, _, _, F} <- E5]).

deadline_expired_keeps_a_404_next_fragment(_Config) ->
    %% A next fragment retention has deleted holds no bytes, so it costs the
    %% window nothing and is kept across the deadline: a 404 is durable, and
    %% forgetting it would spend another GET learning the same thing.
    FragRef = frag_ref(0, 2000, 42),
    Iterator = mock_iterator([{0, 2000, 42}, {100, 100_000, 43}]),
    Opts = #{request_size => 1000, window_max => 4000, max_depth => 8},
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator, Opts),
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 3000),
    {S2, _} = read(S1, ?SEGMENT_HEADER_B, 3000),
    {S3, _} = fail(S2, 100, 8, not_found),
    {S4, _} = rabbitmq_stream_s3_remote_reader_core:step(S3, deadline_expired),
    %% Reading past the current fragment asks for a manifest refresh rather than
    %% re-fetching the fragment that is known to be gone.
    {_S5, E5} = read(S4, ?SEGMENT_HEADER_B + 2000, 100),
    ?assertEqual([], [F || {start_request, _, _, F} <- E5, F =:= 100]),
    ?assertMatch([{refresh_iterator, 100}], [E || {refresh_iterator, _} = E <- E5]).

iterator_refresh_cancels_the_retry_timers(_Config) ->
    %% Refreshing the iterator rebuilds the core state from scratch, which
    %% resets both backoff delays and disowns the timers armed against the old
    %% fragment - the same reset `deadline_expired` performs, and the same
    %% hazard if a timer is left running: it would land part-way through a
    %% later round at the new fragment and release its ranges early.
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 8, Iterator),
    %% Arm the fault timer, then 404 the fragment it was armed for.
    {S1, _} = read(S0, 8, 50),
    {S2, E2} = fail(S1, 0, slow_down),
    ?assertEqual([{set_timer, fault, 1000}], [E || {set_timer, _, _} = E <- E2]),
    {S3, _} = fail(S2, 0, not_found),
    RefreshedIterator = rabbitmq_stream_s3_fragment_iterator:init(
        build_manifest([{100, 500, 43}]), 0, fun(_) -> {error, not_found} end
    ),
    {_S4, E4} = rabbitmq_stream_s3_remote_reader_core:step(
        S3, {iterator_refreshed, RefreshedIterator}
    ),
    ?assertMatch(
        [{cancel_requests, all}, {cancel_timers, all}, {reply, {next_fragment, 100}} | _], E4
    ).

fragment_404_emits_refresh_iterator(_Config) ->
    %% When the current fragment returns 404, the core emits
    %% {refresh_iterator, Offset} to advance past the dead fragment.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Issue read, then 404 on current fragment.
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 100, chunk_boundary}),
    {_S2, Effects} = fail(S1, 0, not_found),
    ?assertMatch([{cancel_requests, all}, {refresh_iterator, 0}], Effects).

known_404_fragment_is_not_refetched(_Config) ->
    %% Retention deleting the current fragment mid-read leaves the reader
    %% holding buffered bytes that still sit below the fragment's index
    %% boundary, so the fetch frontier still points into an object that is gone.
    %% The reads those bytes can serve must not restart the pipeline against it:
    %% each one would fire a whole `max_depth` of range GETs, and every 404 they
    %% earn wipes the queue and cancels the next fragment's prefetch with it.
    %% The refresh is driven by the first read that cannot be served.
    Opts = #{request_size => 1_000, max_depth => 8, window_max => 1_000_000},
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}, {100, 1_000_000, 43}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator, Opts),
    [{Start, End}] = fragment_ranges(S0, 0),
    %% A read that misses grows the window, so a second range joins the first.
    {S1, _} = read(S0, 64, 100),
    ?assertMatch([_, _], fragment_ranges(S1, 0)),
    %% The first range is answered in full, which serves the read and leaves
    %% the reader holding bytes below the fragment's index boundary.
    {S2, E2} = deliver(S1, 0, Start, pattern(Start, End - Start + 1), done),
    ?assertMatch([{reply, {ok, _}} | _], E2),
    %% The fragment is deleted. The range behind it 404s while no read is
    %% pending, so nothing refreshes the iterator yet.
    {S3, _} = fail(S2, 0, not_found),
    ?assertEqual([], outstanding_ranges(S3)),
    %% A read the buffer can still serve is served, and asks S3 for nothing.
    {S4, E4} = read(S3, 164, 100),
    ?assertMatch([{reply, {ok, _}} | _], E4),
    ?assertEqual([], [E || {start_request, _, _, _} = E <- E4]),
    ?assertEqual([], outstanding_ranges(S4)),
    %% The first read that cannot be served is what refreshes the iterator.
    {_S5, E5} = read(S4, End + 1, 100),
    ?assertEqual([{refresh_iterator, 0}], E5).

last_fragment_404_no_pending_read_refreshes_past_current(_Config) ->
    %% Regression test for issue #193. When the current fragment is the last
    %% in the manifest and 404s while no read is pending, a subsequent read
    %% must not crash with {badmatch, end_of_manifest}. Instead it should
    %% emit {refresh_iterator, CurrentOffset}.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Partially fill buffer so the fragment has some data but not all.
    Data = binary:copy(<<0>>, 500),
    {S1, _} = deliver(S0, 0, Data, done),
    %% Read from buffered range succeeds (consuming the pending read slot).
    {S2, E1} = rabbitmq_stream_s3_remote_reader_core:step(S1, {read, 64, 100, chunk_boundary}),
    ?assertMatch([{reply, {ok, _}} | _], E1),
    %% Current fragment 404s while no read is pending.
    {S3, E2} = fail(S2, 0, not_found),
    %% No refresh_iterator emitted (pending = undefined).
    ?assertEqual([], [Eff || {refresh_iterator, _} = Eff <- E2]),
    %% A new read beyond the buffer triggers not_found_check_range.
    %% Before the fix, this crashed with {badmatch, end_of_manifest}.
    {_S4, E3} = rabbitmq_stream_s3_remote_reader_core:step(S3, {read, 600, 100, chunk_boundary}),
    ?assertMatch([{refresh_iterator, 0}], E3).

observe_effects_emitted_for_hit_miss_and_transition(_Config) ->
    %% Verify the core emits one `{observe, Kind, ReadSize}` effect per
    %% buffer hit, miss, and fragment transition. The shell increments
    %% Prometheus counters from these effects (see
    %% https://github.com/amazon-mq/rabbitmq-stream-s3/issues/175).
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 8, Iterator),

    %% Read against an empty buffer emits a miss observation.
    {S1, MissEffects} = rabbitmq_stream_s3_remote_reader_core:step(
        S0, {read, 8, 100, chunk_boundary}
    ),
    [{observe, miss, MissReadSize}] = [E || E = {observe, _, _} <- MissEffects],
    ?assert(is_integer(MissReadSize) andalso MissReadSize > 0),

    %% Data arrives, then a read against the buffered range emits a hit.
    Data = binary:copy(<<0>>, 200),
    {S2, _} = deliver(S1, 0, Data, done),
    {S3, HitEffects} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {read, 8, 100, chunk_boundary}
    ),
    [{observe, hit, HitReadSize}] = [E || E = {observe, _, _} <- HitEffects],
    ?assert(is_integer(HitReadSize) andalso HitReadSize > 0),

    %% Pre-fetch the next fragment, then read past the current fragment's end
    %% to trigger a fragment transition observation.
    NextData = binary:copy(<<1>>, 200),
    {S4, _} = deliver(S3, 100, NextData, done),
    {_S5, TransEffects} = rabbitmq_stream_s3_remote_reader_core:step(
        S4, {read, 208, 50, chunk_boundary}
    ),
    [{observe, fragment_transition, TransReadSize}] =
        [E || E = {observe, _, _} <- TransEffects],
    ?assert(is_integer(TransReadSize) andalso TransReadSize > 0).

pool_busy_backoff_capped_at_500(_Config) ->
    %% pool_busy means the pool is growing (a connection's TLS handshake is in
    %% progress), so it uses a mild backoff that doubles from 25ms and caps at
    %% 500ms, not the network-error exponential sequence.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    {S1, [{set_timer, pool_busy, 25}]} = fail_then_retry(S0, 0, pool_busy),
    {S2, [{set_timer, pool_busy, 50}]} = fail_then_retry(S1, 0, pool_busy),
    {S3, [{set_timer, pool_busy, 100}]} = fail_then_retry(S2, 0, pool_busy),
    {S4, [{set_timer, pool_busy, 200}]} = fail_then_retry(S3, 0, pool_busy),
    {S5, [{set_timer, pool_busy, 400}]} = fail_then_retry(S4, 0, pool_busy),
    {S6, [{set_timer, pool_busy, 500}]} = fail_then_retry(S5, 0, pool_busy),
    %% Capped: stays at 500.
    {_S7, [{set_timer, pool_busy, 500}]} = fail_then_retry(S6, 0, pool_busy),
    ok.

pool_busy_delay_resets_on_data(_Config) ->
    %% A successful data arrival resets the pool_busy backoff to its minimum.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Escalate the pool_busy delay: 25, 50, 100.
    {S1, [{set_timer, pool_busy, 25}]} = fail_then_retry(S0, 0, pool_busy),
    {S2, [{set_timer, pool_busy, 50}]} = fail_then_retry(S1, 0, pool_busy),
    {S3, [{set_timer, pool_busy, 100}]} = fail_then_retry(S2, 0, pool_busy),
    %% Data arrives successfully.
    Data = binary:copy(<<0>>, 1024),
    {S4, _} = deliver(S3, 0, Data, done),
    %% The next pool_busy starts back at 25ms.
    {_S5, [{set_timer, pool_busy, 25}]} = fail(S4, 0, pool_busy),
    ok.

pool_busy_backoff_independent_of_network_errors(_Config) ->
    %% The pool_busy backoff and the network-error backoff track separate
    %% delays. Interleaving them does not advance the other. This is the
    %% behaviour the fix restores: previously pool_busy shared the 1s/2s/4s
    %% network-error clause and burned the read deadline.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% pool_busy uses its own 25ms start.
    {S1, [{set_timer, pool_busy, 25}]} = fail_then_retry(S0, 0, pool_busy),
    %% A network error still starts at the 1000ms minimum, unaffected.
    {S2, [{set_timer, fault, 1000}]} = fail_then_retry(S1, 0, slow_down),
    %% pool_busy continues from 50ms, not reset by the network error.
    {S3, [{set_timer, pool_busy, 50}]} = fail_then_retry(S2, 0, pool_busy),
    %% The network backoff likewise continues from 2000ms.
    {_S4, [{set_timer, fault, 2000}]} = fail_then_retry(S3, 0, slow_down),
    ok.

group_fetch_failure_retries_not_become_local(_Config) ->
    %% Advancing past the current fragment, the next entry is a group object
    %% whose fetch fails transiently. The core must retry (set_timer), not route
    %% the consumer to the local tier: the group is part of the remote tier
    %% being read, so becoming local could serve missing or wrong data (F3,
    %% Tier overlap).
    FragRef = frag_ref(0, 100, 42),
    Iterator = mock_iterator_failing_group([{0, 100, 42}], {100, 0, 99}),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% The frontier looks one fragment ahead as soon as the current one is
    %% spoken for, so the failing group fetch is met on this delivery's pass and
    %% arms the round's timer (see `failed_group_peek_arms_its_own_retry`).
    Data = binary:copy(<<0>>, 100),
    {S1, E1} = deliver(S0, 0, Data, done),
    ?assertEqual([{set_timer, fault, 1000}], [E || {set_timer, _, _} = E <- E1]),
    %% Read past the end → fragment transition → group fetch fails. The round's
    %% timer is still pending, so this must not arm a second one; what matters
    %% is that the consumer is not routed to the local tier.
    {_S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {read, 164, 50, chunk_boundary}
    ),
    ?assertEqual([], Effects).

group_fetch_failure_on_refreshed_iterator_retries(_Config) ->
    %% After a refresh, advancing the refreshed iterator hits a transient group
    %% fetch error. Retry rather than becoming local. Without the fix the
    %% `{iterator_refreshed, _}` handler collapsed any non-{ok} result to
    %% become_local.
    FragRef = frag_ref(0, 100, 42),
    Iterator = mock_iterator([{0, 100, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% A read is pending (the situation in which a refresh is requested).
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 50, chunk_boundary}),
    %% The shell feeds back a refreshed iterator whose group fetch fails.
    Refreshed = mock_iterator_failing_group([{0, 100, 42}], {100, 0, 99}),
    {_S2, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {iterator_refreshed, Refreshed}
    ),
    ?assertMatch([{set_timer, _, _}], Effects),
    ?assertEqual([], [E || {reply, {become_local, _}} = E <- Effects]).

group_fetch_failure_on_refreshed_iterator_drops_cancelled_requests(_Config) ->
    %% The shell cancels every in-flight request before feeding back a refreshed
    %% iterator, so a request left in the queue could never be answered: nothing
    %% re-issues it and `flushable/2` reports it blocked, which wedges its
    %% fragment until the read deadline. The group-fetch-failed branch is the one
    %% that keeps its state rather than rebuilding it, so it has to drop them.
    FragRef = frag_ref(0, 100, 42),
    Iterator = mock_iterator([{0, 100, 42}]),
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator),
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 50),
    ?assertMatch([_ | _], outstanding_ranges(S1)),
    Refreshed = mock_iterator_failing_group([{0, 100, 42}], {100, 0, 99}),
    {S2, _} = rabbitmq_stream_s3_remote_reader_core:step(S1, {iterator_refreshed, Refreshed}),
    ?assertEqual([], outstanding_ranges(S2)),
    %% The reader is not wedged: the retry puts the current fragment back on the
    %% wire rather than waiting out the deadline.
    {S3, E} = retry(S2, fault),
    ?assertMatch([_ | _], [Eff || {start_request, _, _, _} = Eff <- E]),
    ?assertMatch([_ | _], outstanding_ranges(S3)).

group_fetch_failure_backs_off_and_retries(_Config) ->
    %% A persistently-failing group fetch backs off exponentially and keeps
    %% re-attempting the fetch (the retry re-enters the transition and calls the
    %% iterator again), rather than giving up to local. The read deadline, not a
    %% local fallback, bounds the loop.
    FragRef = frag_ref(0, 100, 42),
    Iterator = mock_iterator_failing_group([{0, 100, 42}], {100, 0, 99}),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% First failure: the frontier's look-ahead → retry at the minimum delay.
    Data = binary:copy(<<0>>, 100),
    {S1, E0} = deliver(S0, 0, Data, done),
    [{set_timer, _, D1}] = [E || {set_timer, _, _} = E <- E0],
    %% The transition meets the same failure while that round is still pending,
    %% so it neither re-arms nor becomes local.
    {S2, E1} = rabbitmq_stream_s3_remote_reader_core:step(S1, {read, 164, 50, chunk_boundary}),
    ?assertEqual([], E1),
    %% The retry re-attempts the fetch (still failing) → retry at a larger delay.
    {_S3, E2} = retry(S2, fault),
    [{set_timer, _, D2}] = [E || {set_timer, _, _} = E <- E2],
    ?assert(D2 > D1),
    ?assertEqual([], [E || {reply, {become_local, _}} = E <- E2]).

next_fragment_peek_is_fetched_once(_Config) ->
    %% Looking one fragment ahead descends into a group node, which is a
    %% synchronous S3 GET. The current fragment is fully spoken for from the
    %% first pass here, so every later pass - one per delivered frame - reaches
    %% that peek. Fetching the group each time would block the reader on S3 for
    %% every frame it receives, with the caller's read deadline running.
    Fetches = counters:new(1, []),
    Iterator = mock_iterator_counting_groups(
        [{0, 1000, 42}], [{2000, 1000, 7}, {4000, 1000, 9}], Fetches
    ),
    {S0, _} = init(stream_id(), frag_ref(0, 1000, 42), ?SEGMENT_HEADER_B, Iterator, #{
        request_size => 4000
    }),
    %% The first delivery's pass covers the whole current fragment and spills
    %% into the next, which is where the group is descended into.
    {S1, _} = deliver(S0, 0, 8, pattern(8, 100), continue),
    ?assertEqual(1, counters:get(Fetches, 1)),
    ?assertMatch([{0, _, _}, {2000, _, _}], outstanding_ranges(S1)),
    %% Ten more passes with both fragments fully spoken for and the window still
    %% open: every one of them reaches the peek.
    S2 = lists:foldl(
        fun(_, Acc) ->
            {Acc1, _} = deliver(Acc, 0, 8, pattern(8, 100), continue),
            Acc1
        end,
        S1,
        lists:seq(1, 10)
    ),
    ?assertEqual(1, counters:get(Fetches, 1)),
    %% Moving on replaces the iterator, so the memo is dropped and the fragment
    %% after the new current one is looked up afresh.
    {S3, _} = deliver(S2, 2000, 8, pattern(8, 1000), done),
    {_S4, E} = read(S3, ?SEGMENT_HEADER_B + 1000, 50),
    ?assertMatch([{next_fragment, 2000}], [R || {reply, R} <- E]),
    ?assertEqual(2, counters:get(Fetches, 1)).

failed_group_peek_is_retried_on_the_backoff(_Config) ->
    %% A group fetch that fails transiently is not an answer worth remembering,
    %% but it is a reason not to ask again until the retry timer fires: asking
    %% per pass would put a group GET behind every frame the reader receives
    %% while S3 is already having trouble.
    Fetches = counters:new(1, []),
    Iterator = mock_iterator_counting_failing_group([{0, 1000, 42}], {2000, 0, 7}, Fetches),
    {S0, _} = init(stream_id(), frag_ref(0, 1000, 42), ?SEGMENT_HEADER_B, Iterator, #{
        request_size => 4000
    }),
    {S1, _} = deliver(S0, 0, 8, pattern(8, 100), continue),
    ?assertEqual(1, counters:get(Fetches, 1)),
    S2 = lists:foldl(
        fun(_, Acc) ->
            {Acc1, _} = deliver(Acc, 0, 8, pattern(8, 100), continue),
            Acc1
        end,
        S1,
        lists:seq(1, 5)
    ),
    ?assertEqual(1, counters:get(Fetches, 1)),
    %% A retry round is when it is worth another attempt.
    {_S3, _} = retry(S2, fault),
    ?assertEqual(2, counters:get(Fetches, 1)).

failed_group_peek_is_not_retried_on_the_pool_clock(_Config) ->
    %% The `pool_busy` clock runs 25-500ms and fires for as long as the pool has
    %% no free connection, which says nothing about whether the group object can
    %% be fetched. Clearing the memo on it re-attempted the fetch up to 40x more
    %% often than the fault backoff it is paced by, and `arm_peek_retry/4` could
    %% not slow it down because the fault timer it arms was already pending.
    %% Every attempt is a synchronous group GET inside the core, so it blocks
    %% the reader while the caller's read deadline burns.
    Fetches = counters:new(1, []),
    Iterator = mock_iterator_counting_failing_group([{0, 1000, 42}], {2000, 0, 7}, Fetches),
    {S0, _} = init(stream_id(), frag_ref(0, 1000, 42), ?SEGMENT_HEADER_B, Iterator, #{
        request_size => 4000
    }),
    {S1, _} = deliver(S0, 0, 8, pattern(8, 100), continue),
    ?assertEqual(1, counters:get(Fetches, 1)),
    S2 = lists:foldl(
        fun(_, Acc) ->
            {Acc1, _} = retry(Acc, pool_busy),
            Acc1
        end,
        S1,
        lists:seq(1, 5)
    ),
    ?assertEqual(1, counters:get(Fetches, 1)),
    %% The clock the retry is paced by still clears it.
    {_S3, _} = retry(S2, fault),
    ?assertEqual(2, counters:get(Fetches, 1)).

failed_group_peek_arms_its_own_retry(_Config) ->
    %% Nothing else arms a timer on this path: the ranges already queued are
    %% healthy, so no request error runs, and the `failed` memo is only ever
    %% cleared by a retry round. Without a timer of its own the memo would be
    %% permanent for the life of the iterator - the frontier would stop spilling
    %% into the next fragment, the pipeline would drain as the queued ranges
    %% completed, and the window would sit idle for the rest of the current
    %% fragment, the stall ending only when the consumer reached the boundary.
    Fetches = counters:new(1, []),
    Iterator = mock_iterator_counting_failing_group([{0, 1000, 42}], {2000, 0, 7}, Fetches),
    {S0, _} = init(stream_id(), frag_ref(0, 1000, 42), ?SEGMENT_HEADER_B, Iterator, #{
        request_size => 4000
    }),
    %% The peek failed during a frontier pass whose queued range - the current
    %% fragment's, issued by the placement pass - is perfectly healthy.
    {S1, E1} = deliver(S0, 0, ?SEGMENT_HEADER_B, pattern(8, 100), continue),
    ?assertEqual(1, counters:get(Fetches, 1)),
    ?assertMatch([{0, ?SEGMENT_HEADER_B, _}], outstanding_ranges(S1)),
    ?assertEqual([{set_timer, fault, 1000}], [E || {set_timer, _, _} = E <- E1]),
    %% One timer for the round, not one per pass: further passes see the memo
    %% and must not re-arm (which would also re-grow the backoff).
    {S2, E2} = deliver(S1, 0, ?SEGMENT_HEADER_B, pattern(8, 100), continue),
    ?assertEqual([], [E || {set_timer, _, _} = E <- E2]),
    %% The retry clears the memo, re-attempts the fetch and, since it fails
    %% again, arms the next round at the grown delay.
    {_S3, E3} = retry(S2, fault),
    ?assertEqual(2, counters:get(Fetches, 1)),
    ?assertEqual([{set_timer, fault, 2000}], [E || {set_timer, _, _} = E <- E3]).

read_larger_than_the_window_is_still_served(_Config) ->
    %% The prefetch window bounds prefetch; the read in hand is not optional. A
    %% read of N bytes cannot be served while fewer than N are outstanding, so
    %% gating fetches on the window alone trapped every read larger than
    %% `window_max`: at the ceiling `has_room/1` stayed false, `note_miss/1`
    %% could not widen the window further, and nothing was ever issued again.
    %% Reads are chunk sized, so one chunk bigger than the window was enough.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator, #{
        request_size => 1000, window_max => 2000, max_depth => 8
    }),
    %% A read half again the window's ceiling.
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 3000),
    %% Answering everything it asks for must serve the read. Before the fix it
    %% asked for nothing at all from here on, so no number of rounds served it.
    {Served, S} = answer_until_served(S1, 20),
    ?assertEqual(pattern(?SEGMENT_HEADER_B, 3000), Served),
    %% Exceeding the window is confined to the read that needed it: once served,
    %% the ceiling governs again.
    ?assertEqual([], outstanding_ranges(S)),
    {_S2, E} = read(S, ?SEGMENT_HEADER_B + 3000, 10),
    ?assertEqual([], [R || {reply, {ok, _}} = R <- E]),
    {Outstanding, _} = load(S),
    ?assert(Outstanding >= 3000).

%% Answer every outstanding range in full, round after round, until the pending
%% read is replied to. Returns the served bytes.
answer_until_served(_State, 0) ->
    error(never_served);
answer_until_served(State0, Rounds) ->
    case outstanding_ranges(State0) of
        [] ->
            error({nothing_outstanding_and_no_reply, Rounds});
        Ranges ->
            {State, Effects} = lists:foldl(
                fun({Fragment, Start, End}, {Acc, EffAcc}) ->
                    {Acc1, Eff} = deliver(
                        Acc, Fragment, Start, pattern(Start, End - Start + 1), done
                    ),
                    {Acc1, EffAcc ++ Eff}
                end,
                {State0, []},
                Ranges
            ),
            case [D || {reply, {ok, D}} <- Effects] of
                [Served] -> {Served, State};
                [] -> answer_until_served(State, Rounds - 1)
            end
    end.

group_fetch_failure_keeps_the_ranges_in_flight(_Config) ->
    %% The failure is in advancing the iterator, which says nothing about the
    %% fragment GETs already on the wire: they may still be delivering the bytes
    %% the pending read is waiting for. Dropping them also closes their pooled
    %% connections (`cancel_async/2` must, to stop the response draining), so a
    %% failed group fetch used to tear down a full depth of healthy connections
    %% out of a pool shared with the manifest, group and index GETs.
    Iterator = mock_iterator_failing_group([{0, 100, 42}], {100, 0, 99}),
    {S0, _} = init(stream_id(), frag_ref(0, 100, 42), ?SEGMENT_HEADER_B, Iterator, #{
        request_size => 50
    }),
    %% A miss widens the window to two requests, so the fragment's second half
    %% is on the wire as well.
    {S1, _} = read(S0, ?SEGMENT_HEADER_B, 50),
    ?assertEqual([{0, 8, 57}, {0, 58, 107}], outstanding_ranges(S1)),
    %% Answer the first range only, then read past the fragment's end. That is
    %% the transition, and its look-ahead is the failing group fetch - with the
    %% second range still streaming.
    {S2, _} = deliver(S1, 0, 8, pattern(8, 50), done),
    Before = outstanding_ranges(S2),
    ?assertEqual([{0, 58, 107}], Before),
    {S3, E3} = read(S2, ?SEGMENT_HEADER_B + 100, 50),
    ?assertEqual([], [E || {cancel_requests, _} = E <- E3]),
    ?assertEqual([], [E || {cancel_request, _} = E <- E3]),
    ?assertEqual([], [E || {reply, {become_local, _}} = E <- E3]),
    %% The range is still outstanding and was not re-issued: it never stopped
    %% streaming, and its pooled connection was not closed under it.
    ?assertEqual(Before, outstanding_ranges(S3)),
    ?assertEqual([], [E || {start_request, _, _, _} = E <- E3]).

pool_busy_retry_does_not_release_a_throttled_range(_Config) ->
    %% Each backoff kind arms its own timer. A pool_busy failure must not stand
    %% in for an S3 fault: with one shared timer the fault's 1s backoff was
    %% neither armed nor grown, and the pool's 25ms timer put the range S3 had
    %% just throttled straight back on the wire.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    [{0, A, _}, {0, B, _} | _] = outstanding_ranges(S0),
    {S1, E1} = fail(S0, 0, A, pool_busy),
    ?assertEqual([{set_timer, pool_busy, 25}], E1),
    %% The S3 fault arms its own timer at its own minimum.
    {S2, E2} = fail(S1, 0, B, slow_down),
    ?assertEqual([{set_timer, fault, 1000}], E2),
    %% The pool's timer releases only the range that waited on the pool.
    {S3, E3} = retry(S2, pool_busy),
    ?assertEqual(
        [{start_request, key(), {A, A + 999}, 0}],
        [E || {start_request, _, _, _} = E <- E3]
    ),
    %% The throttled range goes back only on its own timer, and the fault
    %% backoff has grown in the meantime rather than being lost.
    {S4, E4} = retry(S3, fault),
    ?assertEqual(
        [{start_request, key(), {B, B + 999}, 0}],
        [E || {start_request, _, _, _} = E <- E4]
    ),
    {_S5, E5} = fail(S4, 0, B, slow_down),
    ?assertEqual([{set_timer, fault, 2000}], E5).

partial_throttling_does_not_reset_the_backoff(_Config) ->
    %% S3 answering some ranges while throttling others is what throttling looks
    %% like to a pipelined reader. A delivery must not hand back the delay the
    %% throttled ranges just earned, or the reader retries a throttling S3 at
    %% the minimum delay for as long as anything at all is getting through.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    [{0, A, _}, {0, B, _} | _] = outstanding_ranges(S0),
    {S1, E1} = fail(S0, 0, A, slow_down),
    ?assertEqual([{set_timer, fault, 1000}], E1),
    %% A sibling range delivers while A is still queued for retry.
    {S2, _} = deliver(S1, 0, B, pattern(B, 1000), done),
    %% A's retry fires and it fails again: the backoff continues from 2s.
    {S3, _} = retry(S2, fault),
    {_S4, E2} = fail(S3, 0, A, slow_down),
    ?assertEqual([{set_timer, fault, 2000}], E2).

retry_round_in_flight_does_not_reset_the_backoff(_Config) ->
    %% The moment a retry timer fires, no timer is armed and nothing is left in
    %% backoff - but the round it released has proven nothing yet. A sibling
    %% delivering in that window must not hand the clock back to its minimum:
    %% under a throttling S3, some ranges of every round are answered and the
    %% rest are not, so the delay would oscillate between its first two steps
    %% for as long as the throttling lasted.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    [{0, A, _}, {0, B, _} | _] = outstanding_ranges(S0),
    {S1, [{set_timer, fault, 1000}]} = fail(S0, 0, A, slow_down),
    {S2, _} = retry(S1, fault),
    %% A is back on the wire; B answers while it is still outstanding.
    {S3, _} = deliver(S2, 0, B, pattern(B, 1000), done),
    %% A is throttled again: the backoff continues from 2s.
    {_S4, E} = fail(S3, 0, A, slow_down),
    ?assertEqual([{set_timer, fault, 2000}], E).

backoff_resets_once_the_retried_range_delivers(_Config) ->
    %% The other side of the same rule: once the round has come back, the clock
    %% is reset by that very delivery, so a reader that has recovered does not
    %% carry a grown delay into its next unrelated failure.
    {S0, _} = pipelined_state(#{window_max => 4000, max_depth => 32}),
    [{0, A, _}, {0, B, _} | _] = outstanding_ranges(S0),
    {S1, [{set_timer, fault, 1000}]} = fail(S0, 0, A, slow_down),
    {S2, _} = retry(S1, fault),
    {S3, _} = deliver(S2, 0, A, pattern(A, 1000), done),
    {_S4, E} = fail(S3, 0, B, slow_down),
    ?assertEqual([{set_timer, fault, 1000}], E).

end_of_manifest_transition_drops_outstanding_requests(_Config) ->
    %% The transition has nowhere to go, so it stays on the current fragment and
    %% resets its buffer. Any range still reading that fragment could never
    %% flush into the reset buffer again - it would wedge the reassembly queue
    %% and hold its pooled connection - so the requests go with the buffer.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, ?SEGMENT_HEADER_B, Iterator),
    ?assertMatch([_ | _], outstanding_ranges(S0)),
    {S1, E} = rabbitmq_stream_s3_remote_reader_core:step(
        S0, {iterator_refreshed, end_of_manifest}
    ),
    ?assertEqual([{cancel_requests, all}, {reply, {become_local, 0}}], E),
    ?assertEqual([], outstanding_ranges(S1)).
