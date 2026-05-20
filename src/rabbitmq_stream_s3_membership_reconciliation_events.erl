%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_membership_reconciliation_events).
-moduledoc """
A gen_event implementation which notifies the membership reconciliation server
that membership should be evaluated.

This gen_event subscribes to:

* `rabbit_alarm` for node up/down notifications
* `rabbit_event` for queue creation and policy change notifications

The `rabbit_event` subscription is not really necessary at the moment since
the plugin does not check for policies and the hooks trigger an evaluation
upon writer spawn, but it is left here to match membership reconciliation
for quorum queues.
""".

-behaviour(gen_event).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([register/1, unregister/1]).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2]).

%%----------------------------------------------------------------------------

register(Server) ->
    gen_event:add_handler(rabbit_alarm, ?MODULE, Server).

unregister(Server) ->
    gen_event:delete_handler(rabbit_alarm, ?MODULE, Server),
    ok.

%%----------------------------------------------------------------------------

init(Server) ->
    {ok, Server}.

handle_event({node_up, Node}, Server) ->
    gen_server:cast(Server, {schedule, {node_up, Node}}),
    {ok, Server};
handle_event({node_down, Node}, Server) ->
    gen_server:cast(Server, {schedule, {node_down, Node}}),
    {ok, Server};
handle_event(#event{type = queue_created}, Server) ->
    gen_server:cast(Server, {schedule, queue_created}),
    {ok, Server};
handle_event(#event{type = policy_set}, Server) ->
    gen_server:cast(Server, {schedule, policy_set}),
    {ok, Server};
handle_event(#event{type = operator_policy_set}, Server) ->
    gen_server:cast(Server, {schedule, policy_set}),
    {ok, Server};
handle_event(_Event, Server) ->
    {ok, Server}.

handle_call(_Msg, State) ->
    {ok, ok, State}.
handle_info(_Msg, State) ->
    {ok, State}.
terminate(_Reason, _State) ->
    ok.
