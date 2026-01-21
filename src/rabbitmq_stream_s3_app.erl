%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_app).

-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    rabbitmq_stream_s3_sup:start_link().

stop(_State) ->
    ok.
