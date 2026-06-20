%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_reconciler).
-moduledoc """
Periodically reconciles the plugin's attachment to local osiris processes.

The plugin attaches to osiris event-driven: a writer's `on_init` hook starts its
replica reader, and an acceptor's `on_init` hook registers its `manifest_replica`
context. Those events can be missed or undone:

- A writer that crashes and is respawned at the same epoch can leave its new
  incarnation with no replica reader, because the old reader was still
  registered when the new writer's `on_init` ran (the `{StreamId, node()}`
  registry name does not distinguish writer incarnations). The old reader then
  stops on its writer's `DOWN`, leaving the live writer un-tiered with nothing
  to start a reader.
- A reader that exhausts its per-stream restart budget parks with no reader.
- A `manifest_replica` restart drops every per-stream context and the manifest
  cache on the node, and nothing re-registers them.

This server runs `rabbitmq_stream_s3_hooks:reconcile/0` on a timer to re-attach
anything left detached. It is the "reconciliation" the per-stream supervisors'
parking behaviour assumes exists. Steady-state ticks skip already-attached
streams, so they neither restart readers nor re-run retention updates.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("include/logging.hrl").

-record(?MODULE, {timer :: reference() | undefined}).

-define(SERVER, ?MODULE).
-define(RECONCILE, reconcile).

-export([start_link/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    case rabbitmq_stream_s3_config:reconciliation_enabled() of
        true ->
            {ok, #?MODULE{timer = schedule()}};
        false ->
            ?LOG_INFO("Tiered storage reconciliation is disabled."),
            ignore
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(?RECONCILE, #?MODULE{} = State) ->
    %% A reconciliation error must never kill the timer loop: catch everything
    %% and reschedule regardless.
    try
        rabbitmq_stream_s3_hooks:reconcile()
    catch
        Class:Reason:Stack ->
            ?LOG_WARNING(
                "Tiered storage reconciliation tick failed: ~ts:~p~n~p",
                [Class, Reason, Stack]
            )
    end,
    {noreply, State#?MODULE{timer = schedule()}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

schedule() ->
    erlang:send_after(rabbitmq_stream_s3_config:reconciliation_interval(), self(), ?RECONCILE).
