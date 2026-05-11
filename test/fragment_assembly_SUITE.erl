%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(fragment_assembly_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [
        empty_not_cut,
        single_chunk_below_target,
        cuts_on_target,
        cuts_on_exceed,
        tracks_timestamps,
        single_segment_span,
        multi_segment_spans,
        metadata_correct,
        index_records_single_span,
        index_records_multi_span
    ].

init_per_suite(Config) -> Config.
end_per_suite(Config) -> Config.

%% ------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------

empty_not_cut(_Config) ->
    S = rabbitmq_stream_s3_fragment_assembly:new(1000),
    ?assertNot(rabbitmq_stream_s3_fragment_assembly:is_cut(S)).

single_chunk_below_target(_Config) ->
    S0 = rabbitmq_stream_s3_fragment_assembly:new(1000),
    S1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(chunk(0, 100, 500), S0),
    ?assertNot(rabbitmq_stream_s3_fragment_assembly:is_cut(S1)).

cuts_on_target(_Config) ->
    S0 = rabbitmq_stream_s3_fragment_assembly:new(1000),
    S1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(chunk(0, 100, 1000), S0),
    ?assert(rabbitmq_stream_s3_fragment_assembly:is_cut(S1)).

cuts_on_exceed(_Config) ->
    S0 = rabbitmq_stream_s3_fragment_assembly:new(1000),
    S1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(chunk(0, 100, 600), S0),
    ?assertNot(rabbitmq_stream_s3_fragment_assembly:is_cut(S1)),
    S2 = rabbitmq_stream_s3_fragment_assembly:add_chunk(chunk(100, 200, 600), S1),
    ?assert(rabbitmq_stream_s3_fragment_assembly:is_cut(S2)).

tracks_timestamps(_Config) ->
    S0 = rabbitmq_stream_s3_fragment_assembly:new(10000),
    S1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(chunk(0, 1000, 100), S0),
    S2 = rabbitmq_stream_s3_fragment_assembly:add_chunk(chunk(50, 2000, 100), S1),
    S3 = rabbitmq_stream_s3_fragment_assembly:add_chunk(chunk(100, 3000, 100), S2),
    Meta = rabbitmq_stream_s3_fragment_assembly:metadata(S3),
    ?assertEqual(0, maps:get(first_offset, Meta)),
    ?assertEqual(1000, maps:get(first_timestamp, Meta)),
    ?assertEqual(3000, maps:get(last_timestamp, Meta)).

single_segment_span(_Config) ->
    S0 = rabbitmq_stream_s3_fragment_assembly:new(10000),
    S1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(0, 100, 500, 0, 8, 508), S0
    ),
    S2 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(50, 200, 600, 0, 508, 1108), S1
    ),
    Meta = rabbitmq_stream_s3_fragment_assembly:metadata(S2),
    ?assertEqual([{0, 8, 1108}], maps:get(spans, Meta)).

multi_segment_spans(_Config) ->
    S0 = rabbitmq_stream_s3_fragment_assembly:new(10000),
    S1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(0, 100, 500, 0, 8, 508), S0
    ),
    S2 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(50, 200, 600, 512000, 8, 608), S1
    ),
    Meta = rabbitmq_stream_s3_fragment_assembly:metadata(S2),
    ?assertEqual([{0, 8, 508}, {512000, 8, 608}], maps:get(spans, Meta)).

metadata_correct(_Config) ->
    S0 = rabbitmq_stream_s3_fragment_assembly:new(1000),
    S1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(0, 1000, 400, 0, 8, 408), S0
    ),
    S2 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(50, 2000, 700, 0, 408, 1108), S1
    ),
    ?assert(rabbitmq_stream_s3_fragment_assembly:is_cut(S2)),
    Meta = rabbitmq_stream_s3_fragment_assembly:metadata(S2),
    ?assertEqual(0, maps:get(first_offset, Meta)),
    ?assertEqual(100, maps:get(next_offset, Meta)),
    ?assertEqual(1000, maps:get(first_timestamp, Meta)),
    ?assertEqual(2000, maps:get(last_timestamp, Meta)),
    ?assertEqual(1100, maps:get(size, Meta)),
    ?assertEqual(2, maps:get(num_chunks, Meta)),
    ?assertEqual([{0, 8, 1108}], maps:get(spans, Meta)).

index_records_single_span(_Config) ->
    %% Two chunks in one segment. Span starts at pos 8.
    %% Chunk 0 at pos 8, chunk 50 at pos 508.
    %% Fragment positions: 0, 500 (relative to data start after 8-byte header).
    S0 = rabbitmq_stream_s3_fragment_assembly:new(10000),
    S1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(0, 1000, 500, 0, 8, 508), S0
    ),
    S2 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(50, 2000, 600, 0, 508, 1108), S1
    ),
    Idx = rabbitmq_stream_s3_fragment_assembly:index_records(S2),
    ?assertEqual(
        <<0:64/unsigned, 1000:64/signed, 0:32/unsigned, 50:64/unsigned, 2000:64/signed,
            500:32/unsigned>>,
        Idx
    ).

index_records_multi_span(_Config) ->
    %% Chunk 0 in segment 0 at pos 8 (size 500).
    %% Chunk 50 in segment 512000 at pos 8 (size 600).
    %% Span 0: start=8, end=508, length=500. Span 1: start=8, end=608, length=600.
    %% Fragment positions: chunk 0 -> 0, chunk 50 -> 500 (span0 length + offset in span1).
    S0 = rabbitmq_stream_s3_fragment_assembly:new(10000),
    S1 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(0, 1000, 500, 0, 8, 508), S0
    ),
    S2 = rabbitmq_stream_s3_fragment_assembly:add_chunk(
        chunk(50, 2000, 600, 512000, 8, 608), S1
    ),
    Idx = rabbitmq_stream_s3_fragment_assembly:index_records(S2),
    ?assertEqual(
        <<0:64/unsigned, 1000:64/signed, 0:32/unsigned, 50:64/unsigned, 2000:64/signed,
            500:32/unsigned>>,
        Idx
    ).

%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

chunk(Offset, Ts, DataSize) ->
    chunk(Offset, Ts, DataSize, 0, 8, 8 + DataSize).

chunk(Offset, Ts, DataSize, SegOffset, Pos, NextPos) ->
    #{
        chunk_id => Offset,
        timestamp => Ts,
        num_records => 50,
        data_size => DataSize,
        position => Pos,
        next_position => NextPos,
        segment_offset => SegOffset,
        crc => 0
    }.
