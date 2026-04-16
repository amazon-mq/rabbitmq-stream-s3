%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_histogram).
-moduledoc """
Generic histogram backed by a counters array.
One slot per bucket plus a sum slot at the end.
""".

-export([new/2, observe/2, prometheus_format/3]).

-type bucket() :: number() | infinity.
-type key() :: term().

-spec new(key(), [bucket()]) -> ok.
new(Key, Buckets) ->
    Size = length(Buckets) + 1,
    Counters = counters:new(Size, [write_concurrency]),
    persistent_term:put(Key, {Counters, Buckets}),
    ok.

-spec observe(key(), number()) -> ok.
observe(Key, Value) ->
    {Counters, Buckets} = persistent_term:get(Key),
    Pos = find_pos(Value, Buckets, 1),
    counters:add(Counters, Pos, 1),
    counters:add(Counters, length(Buckets) + 1, Value).

-doc """
Returns `{CumulativeBuckets, Count, Sum}` where `CumulativeBuckets` is
`[{UpperBound, CumulativeCount}]` suitable for Prometheus histogram format.
SumTransform is applied to the raw sum (e.g. `fun(X) -> X / 1000 end`).
""".
-spec prometheus_format(key(), fun((number()) -> number()), [bucket()]) ->
    {[{bucket(), non_neg_integer()}], non_neg_integer(), number()}.
prometheus_format(Key, SumTransform, Buckets) ->
    {Counters, _} = persistent_term:get(Key),
    SumPos = length(Buckets) + 1,
    Indexed = lists:enumerate(Buckets),
    {Cumulative, Count} = lists:mapfoldl(
        fun({Pos, UB}, Acc) ->
            N = Acc + counters:get(Counters, Pos),
            TransformedUB = case UB of infinity -> infinity; _ -> SumTransform(UB) end,
            {{TransformedUB, N}, N}
        end,
        0,
        Indexed
    ),
    {Cumulative, Count, SumTransform(counters:get(Counters, SumPos))}.

find_pos(Value, [UB | _], Pos) when UB =:= infinity orelse Value =< UB -> Pos;
find_pos(Value, [_ | Rest], Pos) -> find_pos(Value, Rest, Pos + 1).
