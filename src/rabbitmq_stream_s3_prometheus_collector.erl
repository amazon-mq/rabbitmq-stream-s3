%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_prometheus_collector).

-behaviour(prometheus_collector).

-export([
    register/0,
    deregister/0,
    deregister_cleanup/1,
    collect_mf/2
]).

-import(prometheus_model_helpers, [create_mf/4]).

-define(METRIC_NAME_PREFIX, <<"rabbitmq_stream_s3_">>).

register() ->
    ok = prometheus_registry:register_collector(?MODULE).

deregister() ->
    prometheus_registry:deregister_collector(?MODULE).

deregister_cleanup(_) ->
    ok.

collect_mf(_Registry, Callback) ->
    maps:foreach(
        fun(Name, #{type := Type, help := Help, values := Values}) ->
            Callback(
                create_mf(
                    <<(?METRIC_NAME_PREFIX)/binary, Name/binary>>,
                    Help,
                    Type,
                    Values
                )
            )
        end,
        seshat:format(rabbitmq_stream_s3, #{labels => as_binary})
    ),
    maps:foreach(
        fun(Name, #{type := Type, help := Help, values := Values}) ->
            Callback(
                create_mf(
                    <<(?METRIC_NAME_PREFIX)/binary, (atom_to_binary(Name))/binary>>,
                    Help,
                    Type,
                    Values
                )
            )
        end,
        rabbitmq_stream_s3_request_metrics:prometheus_format()
    ).
