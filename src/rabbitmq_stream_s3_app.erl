%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_app).

-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    _ = seshat:new_group(rabbitmq_stream_s3),
    ok = rabbitmq_stream_s3_api:init(),
    application:set_env(osiris, log_hooks, rabbitmq_stream_s3_hooks),
    application:set_env(osiris, log_reader, rabbitmq_stream_s3_log_reader),
    catch rabbitmq_stream_s3_prometheus_collector:register(),
    rabbitmq_stream_s3_sup:start_link().

stop(_State) ->
    rabbitmq_stream_s3_prometheus_collector:deregister(),
    ok.
