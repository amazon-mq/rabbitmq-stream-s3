%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_replica_reader_sup).
-behaviour(supervisor).

-export([start_link/0, start_child/1, stop_child/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child(rabbitmq_stream_s3_replica_reader:config()) -> {ok, pid()} | {error, term()}.
start_child(Args) ->
    supervisor:start_child(?MODULE, [Args]).

-spec stop_child(pid()) -> ok.
stop_child(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).

init([]) ->
    ChildSpec = #{
        id => rabbitmq_stream_s3_replica_reader,
        start => {rabbitmq_stream_s3_replica_reader, start_link, []},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [rabbitmq_stream_s3_replica_reader]
    },
    {ok, {{simple_one_for_one, 3, 10}, [ChildSpec]}}.
