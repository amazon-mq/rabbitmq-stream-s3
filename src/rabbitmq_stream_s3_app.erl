%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_app).

-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    case rabbitmq_stream_s3_sup:start_link() of
        {ok, Pid} ->
            rabbitmq_stream_s3_hooks:discover(),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    application:unset_env(osiris, log_hooks),
    application:unset_env(osiris, log_reader),
    rabbitmq_stream_s3_prometheus_collector:deregister(),
    ok.
