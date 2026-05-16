%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_reaper).
-moduledoc """
Batched S3 object deletion.

Collects keys from replica readers, retention evaluation, and stream
deletion tasks, then issues batched DeleteObjects calls (up to 1000 keys
per request).

For stream deletion, a short-lived task pages through LIST on the
stream's S3 prefix and sends discovered keys back to this process
for batched deletion. The task exits when the listing is exhausted.
""".

-behaviour(gen_batch_server).

-include("include/rabbitmq_stream_s3.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([delete_objects/2, delete_stream/1]).
-export([init/1, handle_batch/2, terminate/2]).

-define(MAX_BATCH, 1000).

-record(state, {}).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_batch_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Send object keys for batched deletion.".
-spec delete_objects(stream_id(), [rabbitmq_stream_s3:key()]) -> ok.
delete_objects(_StreamId, []) ->
    ok;
delete_objects(_StreamId, Keys) ->
    gen_batch_server:cast(?MODULE, {delete, Keys}).

-doc "Delete all remote tier objects for a stream (async).".
-spec delete_stream(stream_id()) -> ok.
delete_stream(StreamId) ->
    gen_batch_server:cast(?MODULE, {delete_stream, StreamId}).

init([]) ->
    {ok, #state{}}.

handle_batch(Ops, State) ->
    {Keys, Streams} = collect(Ops),
    _ = delete_batched(Keys),
    [spawn_list_task(S) || S <- Streams],
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

collect(Ops) ->
    lists:foldl(
        fun
            ({cast, {delete, K}}, {Keys, Streams}) ->
                {K ++ Keys, Streams};
            ({cast, {delete_stream, StreamId}}, {Keys, Streams}) ->
                {Keys, [StreamId | Streams]};
            (_, Acc) ->
                Acc
        end,
        {[], []},
        Ops
    ).

delete_batched([]) ->
    ok;
delete_batched(Keys) when length(Keys) =< ?MAX_BATCH ->
    rabbitmq_stream_s3_api:delete(Keys);
delete_batched(Keys) ->
    {Batch, Rest} = lists:split(?MAX_BATCH, Keys),
    _ = rabbitmq_stream_s3_api:delete(Batch),
    delete_batched(Rest).

spawn_list_task(StreamId) ->
    spawn_link(fun() -> list_and_delete(StreamId) end).

list_and_delete(StreamId) ->
    Prefix = rabbitmq_stream_s3:stream_prefix(StreamId),
    list_and_delete_loop(Prefix, start).

list_and_delete_loop(_Prefix, done) ->
    ok;
list_and_delete_loop(Prefix, Continuation) ->
    case rabbitmq_stream_s3_api:list(Prefix, Continuation) of
        {ok, [], done} ->
            ok;
        {ok, Keys, NextContinuation} ->
            gen_batch_server:cast(?MODULE, {delete, Keys}),
            list_and_delete_loop(Prefix, NextContinuation);
        {error, Reason} ->
            ?LOG_WARNING("Failed to list objects for prefix ~ts: ~p", [Prefix, Reason]),
            %% Best effort - orphan GC will catch anything we miss.
            ok
    end.
