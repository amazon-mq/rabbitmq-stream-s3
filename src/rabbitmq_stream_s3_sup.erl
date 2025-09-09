%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% TODO we don't really need this outer supervisor anymore now that
    %% the manifest worker is started by a boot step.
    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    LogReaderSup = #{
        id => rabbitmq_stream_s3_log_reader_sup,
        type => supervisor,
        start => {rabbitmq_stream_s3_log_reader_sup, start_link, []}
    },
    Procs = [LogReaderSup],
    {ok, {SupFlags, Procs}}.
