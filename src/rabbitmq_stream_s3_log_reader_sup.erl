%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_log_reader_sup).

-behaviour(supervisor).

-define(SERVER, ?MODULE).

-export([start_link/0]).

-export([init/1]).

-export([add_child/2]).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

add_child(Reader, Key) ->
    supervisor:start_child(?MODULE, [Reader, Key]).

init([]) ->
    ChildSpec = #{
        id => rabbitmq_stream_s3_log_reader,
        start => {rabbitmq_stream_s3_log_reader, start_link, []},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [rabbitmq_stream_s3_log_reader]
    },
    {ok, {{simple_one_for_one, 3, 10}, [ChildSpec]}}.
