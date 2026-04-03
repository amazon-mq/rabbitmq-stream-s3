%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

%% Tracks S3 request duration distribution as a histogram.
%% A histogram is represented by a set of counters, one per bucket.
-module(rabbitmq_stream_s3_request_metrics).

-export([
    init/0,
    observe/2,
    prometheus_format/0
]).

-type kind() :: read | write.
-export_type([kind/0]).

%% Buckets in milliseconds (standard Prometheus HTTP duration buckets).
-define(BUCKET_1, 10).
-define(BUCKET_2, 50).
-define(BUCKET_3, 100).
-define(BUCKET_4, 250).
-define(BUCKET_5, 500).
-define(BUCKET_6, 1_000).
-define(BUCKET_7, 2_500).
-define(BUCKET_8, 5_000).
-define(BUCKET_9, 10_000).
-define(BUCKET_10, infinity).

-define(BUCKETS, [
    {1, ?BUCKET_1},
    {2, ?BUCKET_2},
    {3, ?BUCKET_3},
    {4, ?BUCKET_4},
    {5, ?BUCKET_5},
    {6, ?BUCKET_6},
    {7, ?BUCKET_7},
    {8, ?BUCKET_8},
    {9, ?BUCKET_9},
    {10, ?BUCKET_10}
]).

-define(POS_SUM, 11).
-define(COUNTER_SIZE, 11).

-spec init() -> ok.
init() ->
    lists:foreach(
        fun(Kind) ->
            Counters = counters:new(?COUNTER_SIZE, [write_concurrency]),
            persistent_term:put({?MODULE, Kind}, Counters)
        end,
        [read, write]
    ),
    ok.

-spec observe(kind(), non_neg_integer()) -> ok.
observe(Kind, DurationMs) ->
    Pos = find_bucket_pos(DurationMs),
    Counters = persistent_term:get({?MODULE, Kind}),
    counters:add(Counters, Pos, 1),
    counters:add(Counters, ?POS_SUM, DurationMs).

-spec prometheus_format() -> map().
prometheus_format() ->
    Values = lists:map(
        fun(Kind) ->
            Counters = persistent_term:get({?MODULE, Kind}),
            {Buckets, Count} = lists:mapfoldl(
                fun({UpperBound, NumObservations}, Acc0) ->
                    Acc = Acc0 + NumObservations,
                    {{UpperBound, Acc}, Acc}
                end,
                0,
                raw_buckets(Counters)
            ),
            Sum = counters:get(Counters, ?POS_SUM) / 1000,
            {[{kind, Kind}], Buckets, Count, Sum}
        end,
        [read, write]
    ),
    #{
        request_duration_seconds =>
            #{
                type => histogram,
                help => <<"Duration of S3 API requests in seconds">>,
                values => Values
            }
    }.

find_bucket_pos(Ms) when Ms =< ?BUCKET_1 -> 1;
find_bucket_pos(Ms) when Ms =< ?BUCKET_2 -> 2;
find_bucket_pos(Ms) when Ms =< ?BUCKET_3 -> 3;
find_bucket_pos(Ms) when Ms =< ?BUCKET_4 -> 4;
find_bucket_pos(Ms) when Ms =< ?BUCKET_5 -> 5;
find_bucket_pos(Ms) when Ms =< ?BUCKET_6 -> 6;
find_bucket_pos(Ms) when Ms =< ?BUCKET_7 -> 7;
find_bucket_pos(Ms) when Ms =< ?BUCKET_8 -> 8;
find_bucket_pos(Ms) when Ms =< ?BUCKET_9 -> 9;
find_bucket_pos(_) -> 10.

raw_buckets(Counters) ->
    [
        {bucket_upper_bound(UpperBound), counters:get(Counters, Pos)}
     || {Pos, UpperBound} <- ?BUCKETS
    ].

bucket_upper_bound(infinity) -> infinity;
bucket_upper_bound(Ms) -> Ms / 1000.
