%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_util).
-moduledoc "Shared utility functions.".

-export([now/0, elapsed_ms/1, elapsed_ms/2]).
-export([backoff_delay/4, equal_jitter/1]).

-spec now() -> integer().
now() ->
    erlang:monotonic_time().

-spec elapsed_ms(integer()) -> non_neg_integer().
elapsed_ms(StartTs) ->
    erlang:convert_time_unit(erlang:monotonic_time() - StartTs, native, millisecond).

-spec elapsed_ms(integer(), integer()) -> non_neg_integer().
elapsed_ms(Now, StartTs) ->
    erlang:convert_time_unit(Now - StartTs, native, millisecond).

-doc """
Exponential backoff delay for a given attempt, capped at Max.

`Attempt` is 1-based: the first retry uses `Base`, the second `Base * Exp`, and
so on, growing as `Base * Exp^(Attempt - 1)` until it reaches `Max`. Pure and
deterministic; apply equal_jitter/1 to the result where jitter is wanted.
""".
-spec backoff_delay(
    Attempt :: pos_integer(),
    Base :: non_neg_integer(),
    Exp :: pos_integer(),
    Max :: non_neg_integer()
) -> non_neg_integer().
backoff_delay(Attempt, Base, Exp, Max) when
    is_integer(Attempt), Attempt >= 1, Base >= 0, Exp >= 1, Max >= 0
->
    %% Cap the exponent so Exp^(Attempt - 1) cannot overflow into a huge
    %% bignum before min/2 clamps it; once Base * Exp^N reaches Max, further
    %% growth is irrelevant.
    Factor = pow_capped(Exp, Attempt - 1, Max, Base),
    min(Max, Base * Factor).

%% Integer power Exp^N, stopping early once Base * accumulator would exceed Cap.
pow_capped(_Exp, 0, _Cap, _Base) ->
    1;
pow_capped(Exp, N, Cap, Base) when N > 0 ->
    pow_capped(Exp, N, Cap, Base, 1).

pow_capped(_Exp, 0, _Cap, _Base, Acc) ->
    Acc;
pow_capped(_Exp, _N, Cap, Base, Acc) when Base * Acc >= Cap, Base > 0 ->
    Acc;
pow_capped(Exp, N, Cap, Base, Acc) ->
    pow_capped(Exp, N - 1, Cap, Base, Acc * Exp).

-doc """
Apply equal jitter to a delay: half the delay plus a random amount in
`[0, half]`, so the result is uniformly distributed in `[Delay/2, Delay]`.

This spreads retries that would otherwise fire in lockstep across many streams
after a shared outage, without ever exceeding the backoff ceiling.
""".
-spec equal_jitter(non_neg_integer()) -> non_neg_integer().
equal_jitter(0) ->
    0;
equal_jitter(Delay) when is_integer(Delay), Delay > 0 ->
    Half = Delay div 2,
    Half + rand:uniform(Delay - Half + 1) - 1.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

backoff_delay_grows_then_caps_test() ->
    %% Base 10, exponent 2, cap 5000: 10, 20, 40, ... doubling each attempt.
    ?assertEqual(10, backoff_delay(1, 10, 2, 5000)),
    ?assertEqual(20, backoff_delay(2, 10, 2, 5000)),
    ?assertEqual(40, backoff_delay(3, 10, 2, 5000)),
    ?assertEqual(80, backoff_delay(4, 10, 2, 5000)),
    %% Eventually clamps at the cap and never exceeds it, even for large attempts.
    ?assertEqual(5000, backoff_delay(100, 10, 2, 5000)),
    ?assertEqual(5000, backoff_delay(1000, 10, 2, 5000)).

backoff_delay_first_attempt_is_base_test() ->
    ?assertEqual(1000, backoff_delay(1, 1000, 2, 30000)),
    ?assertEqual(2000, backoff_delay(2, 1000, 2, 30000)),
    ?assertEqual(30000, backoff_delay(10, 1000, 2, 30000)).

backoff_delay_base_above_cap_is_clamped_test() ->
    ?assertEqual(500, backoff_delay(1, 1000, 2, 500)).

equal_jitter_bounds_test() ->
    Delay = 1000,
    Half = Delay div 2,
    [
        begin
            J = equal_jitter(Delay),
            ?assert(J >= Half andalso J =< Delay)
        end
     || _ <- lists:seq(1, 1000)
    ],
    ?assertEqual(0, equal_jitter(0)).

-endif.
