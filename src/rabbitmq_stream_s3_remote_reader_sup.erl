%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_remote_reader_sup).

-behaviour(supervisor).

-define(SERVER, ?MODULE).

-export([start_link/0]).

-export([init/1]).

-export([add_child/1]).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec add_child(rabbitmq_stream_s3_remote_reader:config()) ->
    supervisor:startchild_ret().
add_child(Config) ->
    supervisor:start_child(?MODULE, [Config]).

init([]) ->
    ChildSpec = #{
        id => rabbitmq_stream_s3_remote_reader,
        start => {rabbitmq_stream_s3_remote_reader, start_link, []},
        %% Any error is fatal since the reader starts at a location passed in
        %% its config map. Don't attempt to restart readers:
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [rabbitmq_stream_s3_remote_reader]
    },
    {ok, {{simple_one_for_one, 3, 10}, [ChildSpec]}}.
