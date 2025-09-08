%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%%%-------------------------------------------------------------------
%%% @author  Simon Unge <simunge@amazon.com>
%%% @copyright (C) 2025,
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(rabbitmq_stream_s3_log_reader_sup).

-behaviour(supervisor).

-define(SERVER, ?MODULE).

-export([start_link/0]).

-export([init/1]).

-export([add_child/2]).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

add_child(Bucket, Key) ->
    supervisor:start_child(?MODULE, [Bucket, Key]).

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
