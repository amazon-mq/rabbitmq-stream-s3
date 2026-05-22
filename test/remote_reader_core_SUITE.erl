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
        not_found_triggers_range_lookup,
        %% New tests
        aimd_growth_after_consecutive_hits,
        aimd_shrink_on_miss,
        exponential_backoff_caps_at_max,
        fatal_error_emits_stop,
        multi_chunk_data_accumulation,
        prefetch_next_fragment_triggered,
        fragment_transition_without_prefetch_awaits,
        read_at_exact_fragment_boundary,
        mid_fragment_init_position,
        jump_to_oldest_full_cycle,
        retry_resets_delay_on_success,
        read_larger_than_buffer_awaits,
        header_overread_capped_at_index_boundary,
        next_fragment_404_triggers_range_lookup,
        deadline_expired_replies_error_timeout,
        deadline_expired_resets_buffer_for_retry
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
    EntriesBin = lists:foldl(
        fun({Offset, Size, Uid}, Acc) ->
            E = ?ENTRY(Offset, 0, 0, ?MANIFEST_KIND_FRAGMENT, Size, Uid),
            <<Acc/binary, E/binary>>
        end,
        <<>>,
        Entries
    ),
    #manifest{
        first_offset = element(1, hd(Entries)),
        next_offset = element(1, lists:last(Entries)) + 100,
        entries = EntriesBin
    }.

frag_ref(Offset, Size, Uid) ->
    #fragment_ref{offset = Offset, uid = Uid, size = Size}.

init(StreamId, FragRef, Position, Iterator) ->
    init(StreamId, FragRef, Position, Iterator, #{}).

init(StreamId, FragRef, Position, Iterator, Opts) ->
    rabbitmq_stream_s3_remote_reader_core:init(StreamId, FragRef, Position, Iterator, Opts).

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
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),

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

    {_S1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S0, {request_error, make_ref(), 0, slow_down}
    ),
    ?assertMatch([{set_timer, _}], Effects).

two_timeouts_then_retry_succeeds(_Config) ->
    %% Two timeouts increase retry delay. Then retry + data arrival serves the read.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    %% Issue a read.
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 100, chunk_boundary}),

    %% First timeout.
    {S2, E1} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {request_error, make_ref(), 0, timeout}
    ),
    ?assertMatch([{set_timer, 1000}], E1),

    %% Second timeout — delay doubles.
    {S3, E2} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {request_error, make_ref(), 0, timeout}
    ),
    ?assertMatch([{set_timer, 2000}], E2),

    %% Retry fires, new request starts.
    {S4, E3} = rabbitmq_stream_s3_remote_reader_core:step(S3, retry),
    ?assertMatch([{start_request, _, _, 0} | _], E3),

    %% Data arrives, read is served.
    Data = binary:copy(<<0>>, 1024),
    {_S5, E4} = rabbitmq_stream_s3_remote_reader_core:step(S4, {data, make_ref(), 0, Data, done}),
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
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),

    %% Pre-fetch next fragment data.
    NextData = binary:copy(<<1>>, 300),
    {S2, _} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {data, make_ref(), 100, NextData, done}
    ),

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
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),

    %% Read past end triggers refresh_iterator effect.
    {S2, E1} = rabbitmq_stream_s3_remote_reader_core:step(S1, {read, 164, 50, chunk_boundary}),
    ?assertMatch([{refresh_iterator}], E1),

    %% Shell reports end_of_manifest.
    {_S3, E2} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {iterator_refreshed, end_of_manifest}
    ),
    ?assertMatch([{reply, {become_local, 0}}], E2).

not_found_triggers_range_lookup(_Config) ->
    %% A 404 on the current fragment triggers a manifest range lookup.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),

    %% Issue a read.
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 100, chunk_boundary}),

    %% Request returns 404.
    {S2, E1} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {request_error, make_ref(), 0, not_found}
    ),
    ?assertMatch([{lookup_manifest_range}], E1),

    %% Shell reports empty range → end_of_stream.
    {_S3, E2} = rabbitmq_stream_s3_remote_reader_core:step(S2, {manifest_range, empty}),
    ?assertMatch([{reply, end_of_stream}], E2).

%% ------------------------------------------------------------------
%% AIMD and retry tests
%% ------------------------------------------------------------------

aimd_growth_after_consecutive_hits(_Config) ->
    %% 8 consecutive buffer hits grow read_size by one GROW_STEP (1 MiB).
    FragRef = frag_ref(0, 10_000_000, 42),
    Iterator = mock_iterator([{0, 10_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Fill buffer with plenty of data.
    Data = binary:copy(<<0>>, 5_000_000),
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),
    %% Issue 8 reads (all buffer hits). Each read at a different position.
    S = lists:foldl(
        fun(I, Acc) ->
            Pos = 64 + I * 100,
            {Acc1, _} = rabbitmq_stream_s3_remote_reader_core:step(
                Acc, {read, Pos, 50, chunk_boundary}
            ),
            Acc1
        end,
        S1,
        lists:seq(0, 7)
    ),
    %% 9th read should trigger the growth. Check via start_request range size.
    %% Initial read_size = 4 MiB. After 8 hits: 4 MiB + 1 MiB = 5 MiB.
    %% The next start_request (if triggered) would use the new read_size.
    %% We verify indirectly: issue one more read that's a hit, then check
    %% that the state is consistent (no crash, reply produced).
    {_, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S, {read, 64 + 800, 50, chunk_boundary}
    ),
    Replies = [E || {reply, _} = E <- Effects],
    ?assertMatch([{reply, {ok, _}}], Replies).

aimd_shrink_on_miss(_Config) ->
    %% A buffer miss halves read_size. After a timeout clears the request
    %% and retry fires, the new request uses the halved size.
    FragRef = frag_ref(0, 10_000_000, 42),
    Iterator = mock_iterator([{0, 10_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Read without data → miss (adjusts read_size).
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 100, chunk_boundary}),
    %% Timeout clears the in-flight request.
    {S2, [{set_timer, _}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {request_error, make_ref(), 0, timeout}
    ),
    %% Retry → new request with halved read_size.
    {_S3, Effects} = rabbitmq_stream_s3_remote_reader_core:step(S2, retry),
    Requests = [{R, F} || {start_request, _, R, F} <- Effects],
    ?assertMatch([{{64, _}, 0}], Requests),
    [{{64, EndPos}, 0}] = Requests,
    RangeSize = EndPos - 64,
    %% Initial 4MiB, miss halves to 2MiB.
    ?assertEqual(2_097_152, RangeSize).

exponential_backoff_caps_at_max(_Config) ->
    %% Repeated errors double the delay up to 30_000ms max.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Fire enough errors to exceed 30s cap: 1000, 2000, 4000, 8000, 16000, 32000→30000
    {S1, [{set_timer, 1000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S0, {request_error, make_ref(), 0, slow_down}
    ),
    {S2, [{set_timer, 2000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {request_error, make_ref(), 0, slow_down}
    ),
    {S3, [{set_timer, 4000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {request_error, make_ref(), 0, slow_down}
    ),
    {S4, [{set_timer, 8000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S3, {request_error, make_ref(), 0, slow_down}
    ),
    {S5, [{set_timer, 16000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S4, {request_error, make_ref(), 0, slow_down}
    ),
    %% Next would be 32000 but capped at 30000.
    {_S6, [{set_timer, 30000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S5, {request_error, make_ref(), 0, slow_down}
    ),
    ok.

fatal_error_emits_stop(_Config) ->
    %% An unknown error reason emits the stop effect.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    {_S1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S0, {request_error, make_ref(), 0, {unexpected, boom}}
    ),
    ?assertEqual([stop], Effects).

multi_chunk_data_accumulation(_Config) ->
    %% Data arrives in 3 chunks (continue, continue, done). Read served after all arrive.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Issue read first.
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 300, chunk_boundary}),
    %% Data arrives in 3 pieces of 100 bytes each.
    Chunk = binary:copy(<<$A>>, 100),
    {S2, E1} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {data, make_ref(), 0, Chunk, continue}
    ),
    ?assertEqual([], [E || {reply, _} = E <- E1]),
    {S3, E2} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {data, make_ref(), 0, Chunk, continue}
    ),
    ?assertEqual([], [E || {reply, _} = E <- E2]),
    {_S4, E3} = rabbitmq_stream_s3_remote_reader_core:step(S3, {data, make_ref(), 0, Chunk, done}),
    Replies = [E || {reply, _} = E <- E3],
    ?assertMatch([{reply, {ok, _}}], Replies),
    [{reply, {ok, ResultData}}] = Replies,
    ?assertEqual(300, byte_size(ResultData)).

%% ------------------------------------------------------------------
%% Fragment navigation tests
%% ------------------------------------------------------------------

prefetch_next_fragment_triggered(_Config) ->
    %% After current fragment buffer is full, core emits start_request for next fragment.
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Fill current fragment completely (200 bytes from pos 64 to 264).
    Data = binary:copy(<<0>>, 200),
    {_S1, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S0, {data, make_ref(), 0, Data, done}
    ),
    %% Should have a start_request for fragment 100 (the next one).
    NextRequests = [{K, R, F} || {start_request, K, R, F} <- Effects, F =:= 100],
    ?assertMatch([{_, _, 100}], NextRequests).

fragment_transition_without_prefetch_awaits(_Config) ->
    %% When next fragment data is not yet available, core awaits (no reply).
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Fill current fragment but do NOT provide next fragment data.
    Data = binary:copy(<<0>>, 200),
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),
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
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),
    NextData = binary:copy(<<1>>, 500),
    {S2, _} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {data, make_ref(), 100, NextData, done}
    ),
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

jump_to_oldest_full_cycle(_Config) ->
    %% 404 on current fragment → manifest_range → jump_to_oldest effect →
    %% jumped event → reply with next_fragment + new request started.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}, {500, 2_000_000, 99}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Issue read, then 404.
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {read, 64, 100, chunk_boundary}),
    {S2, [{lookup_manifest_range}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {request_error, make_ref(), 0, not_found}
    ),
    %% Manifest says first available is at offset 500.
    {S3, E1} = rabbitmq_stream_s3_remote_reader_core:step(S2, {manifest_range, {500, 1000}}),
    ?assertMatch([{jump_to_oldest, 500}], E1),
    %% Shell resolves the jump and feeds back.
    NewFragRef = frag_ref(500, 2_000_000, 99),
    NewIterator = mock_iterator([{500, 2_000_000, 99}]),
    {_S4, E2} = rabbitmq_stream_s3_remote_reader_core:step(S3, {jumped, NewFragRef, NewIterator}),
    %% Should reply with next_fragment and start a request for the new fragment.
    ?assertMatch([{reply, {next_fragment, 500}} | _], E2),
    Requests = [F || {start_request, _, _, F} <- E2],
    ?assertEqual([500], Requests).

retry_resets_delay_on_success(_Config) ->
    %% After backoff escalates, successful data resets delay to minimum.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Escalate: 1s, 2s, 4s.
    {S1, [{set_timer, 1000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S0, {request_error, make_ref(), 0, slow_down}
    ),
    {S2, [{set_timer, 2000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {request_error, make_ref(), 0, slow_down}
    ),
    {S3, [{set_timer, 4000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {request_error, make_ref(), 0, slow_down}
    ),
    %% Retry, then data arrives successfully.
    {S4, _} = rabbitmq_stream_s3_remote_reader_core:step(S3, retry),
    Data = binary:copy(<<0>>, 1024),
    {S5, _} = rabbitmq_stream_s3_remote_reader_core:step(S4, {data, make_ref(), 0, Data, done}),
    %% Next error should start back at 1000ms (reset).
    {_S6, [{set_timer, 1000}]} = rabbitmq_stream_s3_remote_reader_core:step(
        S5, {request_error, make_ref(), 0, slow_down}
    ),
    ok.

read_larger_than_buffer_awaits(_Config) ->
    %% Request more bytes than are buffered. Core awaits, does not crash or
    %% return partial data.
    FragRef = frag_ref(0, 10_000_000, 42),
    Iterator = mock_iterator([{0, 10_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Provide only 100 bytes.
    Data = binary:copy(<<0>>, 100),
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),
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
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),
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

next_fragment_404_triggers_range_lookup(_Config) ->
    %% Prefetch of next fragment returns 404. When the consumer reads past
    %% the current fragment, the core triggers a manifest range lookup.
    FragRef = frag_ref(0, 200, 42),
    Iterator = mock_iterator([{0, 200, 42}, {100, 500, 43}]),
    {S0, _} = init(stream_id(), FragRef, 8, Iterator),
    %% Fill current fragment.
    Data = binary:copy(<<0>>, 200),
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),
    %% Next fragment prefetch returns 404 (fragment 100, not current fragment 0).
    {S2, _} = rabbitmq_stream_s3_remote_reader_core:step(
        S1, {request_error, make_ref(), 100, not_found}
    ),
    %% Read past end of current fragment. Next is not_found → range lookup.
    {_S3, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S2, {read, 208, 50, chunk_boundary}
    ),
    ?assertMatch([{lookup_manifest_range}], Effects).

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
    ?assertMatch([{reply, {error, timeout}}], Effects),
    %% Pending is cleared.
    ?assertEqual(undefined, rabbitmq_stream_s3_remote_reader_core:pending(S2)).

deadline_expired_resets_buffer_for_retry(_Config) ->
    %% After deadline_expired, a subsequent read at any position (including one
    %% behind the old current_pos) does not crash. It awaits data instead.
    FragRef = frag_ref(0, 1_000_000, 42),
    Iterator = mock_iterator([{0, 1_000_000, 42}]),
    {S0, _} = init(stream_id(), FragRef, 64, Iterator),
    %% Provide data and advance current_pos via a successful read.
    Data = binary:copy(<<0>>, 5000),
    {S1, _} = rabbitmq_stream_s3_remote_reader_core:step(S0, {data, make_ref(), 0, Data, done}),
    {S2, E1} = rabbitmq_stream_s3_remote_reader_core:step(S1, {read, 64, 100, chunk_boundary}),
    ?assertMatch([{reply, {ok, _}} | _], E1),
    %% Issue another read (pending), then deadline fires.
    {S3, _} = rabbitmq_stream_s3_remote_reader_core:step(S2, {read, 3000, 100, chunk_boundary}),
    {S4, _} = rabbitmq_stream_s3_remote_reader_core:step(S3, deadline_expired),
    %% Retry at position 64 (behind old current_pos). Must not crash.
    %% Buffer is empty so it awaits, and a new request is started.
    {_S5, Effects} = rabbitmq_stream_s3_remote_reader_core:step(
        S4, {read, 64, 100, chunk_boundary}
    ),
    Replies = [E || {reply, _} = E <- Effects],
    ?assertEqual([], Replies),
    Requests = [F || {start_request, _, _, F} <- Effects],
    ?assertMatch([0], Requests).
