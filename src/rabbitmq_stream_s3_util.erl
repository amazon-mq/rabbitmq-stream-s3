%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_util).
-moduledoc "Shared utility functions.".

-export([now/0, elapsed_ms/1, elapsed_ms/2]).

-spec now() -> integer().
now() ->
    erlang:monotonic_time().

-spec elapsed_ms(integer()) -> non_neg_integer().
elapsed_ms(StartTs) ->
    erlang:convert_time_unit(erlang:monotonic_time() - StartTs, native, millisecond).

-spec elapsed_ms(integer(), integer()) -> non_neg_integer().
elapsed_ms(Now, StartTs) ->
    erlang:convert_time_unit(Now - StartTs, native, millisecond).
