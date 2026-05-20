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
-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([delete_objects/2, delete_stream/1]).
-export([init/1, handle_batch/2, terminate/2]).
-export([init_counters/0]).

-define(MAX_BATCH, 1000).

-define(C_OBJECTS_DELETED, 1).
-define(C_STREAMS_DELETED, 2).
-define(COUNTERS, [
    {objects_deleted, ?C_OBJECTS_DELETED, counter,
        "Total individual objects deleted via the reaper (retention or stream deletion)"},
    {streams_deleted, ?C_STREAMS_DELETED, counter, "Streams whose deletion task ran to completion"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

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
    spawn(fun() ->
        logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
        list_and_delete(StreamId)
    end),
    ok.

init([]) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    {ok, #state{}}.

handle_batch(Ops, State) ->
    Keys = collect(Ops),
    _ = delete_batched(Keys),
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

collect(Ops) ->
    lists:append([K || {cast, {delete, K}} <- Ops]).

delete_batched([]) ->
    ok;
delete_batched(Keys) when length(Keys) =< ?MAX_BATCH ->
    inc(?C_OBJECTS_DELETED, length(Keys)),
    rabbitmq_stream_s3_api:delete(Keys);
delete_batched(Keys) ->
    {Batch, Rest} = lists:split(?MAX_BATCH, Keys),
    inc(?C_OBJECTS_DELETED, length(Batch)),
    _ = rabbitmq_stream_s3_api:delete(Batch),
    delete_batched(Rest).

list_and_delete(StreamId) ->
    Prefix = rabbitmq_stream_s3:stream_prefix(StreamId),
    Result = list_and_delete_loop(Prefix, start),
    case Result of
        ok -> inc(?C_STREAMS_DELETED, 1);
        _ -> ok
    end.

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
            error
    end.

%% ------------------------------------------------------------------
%% Counters
%% ------------------------------------------------------------------

-spec init_counters() -> ok.
init_counters() ->
    Cnt = seshat:new(rabbitmq_stream_s3, ?MODULE, ?COUNTERS, #{module => ?MODULE}),
    persistent_term:put(?COUNTER_KEY, Cnt),
    ok.

inc(Idx, N) ->
    case persistent_term:get(?COUNTER_KEY, undefined) of
        undefined -> ok;
        Cnt -> counters:add(Cnt, Idx, N)
    end.
