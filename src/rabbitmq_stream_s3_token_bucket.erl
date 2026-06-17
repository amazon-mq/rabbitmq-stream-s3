%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_token_bucket).
-moduledoc """
Pure token bucket for byte-rate limiting.

Tokens represent bytes. The bucket refills at a configured rate
(bytes per second) and is capped at a burst size. Callers request
tokens before sending data. If insufficient tokens are available,
the caller learns the deficit and can wait for the next refill.

`burst` caps how many tokens may be *accumulated* while idle. It does
not cap the size of an individual request: a request larger than
`burst` is admitted once the bucket has saved up all it can
(`available == burst`) and is allowed to drive `available` negative,
going into debt. Subsequent refills repay the debt before anything
else is admitted, so the long-run rate is preserved exactly regardless
of request size. This matters because a transfer is a whole fragment,
and a fragment is not bounded by `fragment_target_size`: that size is a
soft floor for *when to cut*, and a single trailing chunk (which batches
records) can be arbitrarily large. Any assumption that a request fits
under `burst` would deadlock the day a large batch is written.
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
%% Admit when the bucket holds at least `min(Tokens, Burst)` tokens.
%% For a normal request (Tokens =< Burst) this is identical to "have
%% enough tokens". For an oversized request (Tokens > Burst) the
%% threshold saturates at Burst, so the request is admitted once the
%% bucket is fully saved up and then pays its full cost, driving
%% `available` negative (debt). Refill repays the debt before the next
%% admission, preserving the long-run rate. `min/2` is allowed in guards
%% since OTP 26.
request(Tokens, #bucket{available = Available, burst = Burst} = Bucket) when
    min(Tokens, Burst) =< Available
->
    {ok, Bucket#bucket{available = Available - Tokens}};
request(Tokens, #bucket{available = Available, burst = Burst} = Bucket) ->
    {insufficient, min(Tokens, Burst) - Available, Bucket}.

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
    %% Drain the bucket below burst first; a full bucket can never report
    %% insufficient now (a request larger than burst is admitted on credit).
    %% With 40 tokens left, a 90-token request reports a deficit of 50.
    B0 = new(1000, 100),
    {ok, B1} = request(60, B0),
    {insufficient, 50, _} = request(90, B1).

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

%% An oversized request (larger than burst) must be serviceable, not a
%% permanent deadlock. It is admitted once the bucket is full and then
%% drives `available` negative (debt).
oversized_request_admitted_when_full_test() ->
    B = new(1000, 1000),
    %% 1500 > burst 1000, but the bucket is full (available == burst).
    {ok, B1} = request(1500, B),
    ?assertEqual(-500, B1#bucket.available).

%% While in debt (available < burst) nothing is admitted, including a
%% small request. The bucket must refill back up first.
oversized_request_blocks_until_debt_repaid_test() ->
    B = new(1000, 1000),
    {ok, B1} = request(1500, B),
    %% available is -500; even a tiny request must wait.
    {insufficient, _, _} = request(1, B1),
    %% Another oversized request also waits: threshold saturates at burst,
    %% and available (-500) is below burst.
    {insufficient, _, _} = request(5000, B1).

%% No upper bound on serviceable size: a request far larger than burst is
%% still admitted when the bucket is full, and the debt repays via refill.
arbitrarily_large_request_serviceable_test() ->
    B0 = new(1_000_000, 1_000_000),
    {ok, B1} = request(50_000_000, B0),
    ?assertEqual(-49_000_000, B1#bucket.available),
    %% Debt repays over time; eventually the bucket climbs back toward burst.
    timer:sleep(10),
    B2 = refill(B1),
    ?assert(B2#bucket.available > B1#bucket.available).

%% burst = 0 (degenerate config, e.g. rate < 5) must not deadlock either.
%% With zero burst the threshold is always 0, so the first request is
%% admitted immediately and pacing is purely rate-driven thereafter.
zero_burst_paces_without_deadlock_test() ->
    B0 = new(1000, 0),
    {ok, B1} = request(500, B0),
    ?assertEqual(-500, B1#bucket.available),
    {insufficient, _, _} = request(500, B1).

-endif.
