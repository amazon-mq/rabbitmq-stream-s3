%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_token_bucket).
-moduledoc """
Pure token bucket for byte-rate limiting.

Tokens represent bytes. The bucket refills at a configured rate
(bytes per second) and is capped at a burst size. Callers request
tokens before sending data. If insufficient tokens are available,
the caller learns the deficit and can wait for the next refill.
""".

-export([new/2, request/2, refill/1, info/1]).
-export_type([t/0]).

-record(bucket, {
    %% Bytes per second.
    rate :: non_neg_integer(),
    %% Maximum tokens (burst cap).
    burst :: non_neg_integer(),
    %% Currently available tokens.
    available :: integer(),
    %% Timestamp of last refill (native monotonic).
    last_refill :: integer()
}).

-opaque t() :: #bucket{}.

-spec new(Rate :: non_neg_integer(), Burst :: non_neg_integer()) -> t().
new(Rate, Burst) ->
    #bucket{
        rate = Rate,
        burst = Burst,
        available = Burst,
        last_refill = erlang:monotonic_time()
    }.

-spec request(Tokens :: non_neg_integer(), t()) ->
    {ok, t()} | {insufficient, Deficit :: non_neg_integer(), t()}.
request(Tokens, #bucket{available = Available} = Bucket) when Tokens =< Available ->
    {ok, Bucket#bucket{available = Available - Tokens}};
request(Tokens, #bucket{available = Available} = Bucket) ->
    {insufficient, Tokens - Available, Bucket}.

-spec refill(t()) -> t().
refill(#bucket{rate = Rate, burst = Burst, available = Available, last_refill = Last} = Bucket) ->
    Now = erlang:monotonic_time(),
    Elapsed = rabbitmq_stream_s3_util:elapsed_ms(Now, Last),
    Added = (Rate * Elapsed) div 1000,
    NewAvailable = min(Burst, Available + Added),
    Bucket#bucket{available = NewAvailable, last_refill = Now}.

-spec info(t()) -> #{rate := non_neg_integer(), burst := non_neg_integer(), available := integer()}.
info(#bucket{rate = Rate, burst = Burst, available = Available}) ->
    #{rate => Rate, burst => Burst, available => Available}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

new_bucket_has_full_tokens_test() ->
    B = new(1000, 500),
    {ok, B1} = request(500, B),
    {insufficient, _, _} = request(1, B1).

request_reduces_available_test() ->
    B = new(1000, 1000),
    {ok, B1} = request(300, B),
    {ok, B2} = request(300, B1),
    {ok, B3} = request(300, B2),
    {insufficient, _, _} = request(200, B3).

request_zero_always_succeeds_test() ->
    B = new(1000, 0),
    {ok, _} = request(0, B).

insufficient_returns_deficit_test() ->
    B = new(1000, 100),
    {insufficient, 50, _} = request(150, B).

refill_adds_tokens_test() ->
    B0 = new(10_000_000, 10_000_000),
    {ok, B1} = request(10_000_000, B0),
    %% Simulate time passing by sleeping briefly.
    timer:sleep(10),
    B2 = refill(B1),
    %% Should have some tokens back (at 10M/s, 10ms = ~100K tokens).
    {ok, _} = request(1, B2).

refill_caps_at_burst_test() ->
    B0 = new(10_000_000, 500),
    %% Already full. Refill should not exceed burst.
    timer:sleep(10),
    B1 = refill(B0),
    ?assert(B1#bucket.available =< 500).

-endif.
