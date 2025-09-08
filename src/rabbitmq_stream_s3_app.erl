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

-module(rabbitmq_stream_s3_app).

-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _StartArgs) ->
    {ok, _} = application:ensure_all_started(rabbitmq_aws),
    rabbitmq_stream_s3_sup:start_link().

stop(_State) ->
    ok.
