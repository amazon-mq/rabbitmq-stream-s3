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
%% The reaper has no upstream caller waiting on a deadline. Under sustained
%% S3 throttling the pool's default 5s checkout timeout can starve the reaper
%% and crash the gen_batch_server. Use a generous timeout consistent with
%% other write-side background work (see `retention_task_timeout`).
-define(DELETE_TIMEOUT_MS, 60_000).

-define(C_OBJECTS_DELETED, 1).
-define(C_STREAMS_DELETED, 2).
-define(C_OBJECTS_DELETE_FAILED, 3).
-define(COUNTERS, [
    {objects_deleted, ?C_OBJECTS_DELETED, counter,
        "Individual objects confirmed deleted via the reaper (retention or stream deletion)"},
    {streams_deleted, ?C_STREAMS_DELETED, counter, "Streams whose deletion task ran to completion"},
    {objects_delete_failed, ?C_OBJECTS_DELETE_FAILED, counter,
        "Individual objects the reaper could not confirm deleted, left for orphan GC"}
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
    %% Reply to any flush barrier in this batch only after the deletes above
    %% have run. gen_batch_server processes a batch in order and a caller's
    %% {delete, _} casts are enqueued before its flush call, so a returning
    %% flush guarantees those deletes are complete (see list_and_delete/1).
    _ = [gen:reply(From, ok) || {call, From, flush} <- Ops],
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
    delete_batch(Keys);
delete_batched(Keys) ->
    {Batch, Rest} = lists:split(?MAX_BATCH, Keys),
    delete_batch(Batch),
    delete_batched(Rest).

%% Count only objects S3 confirms deleted. A DeleteObjects request can return a
%% 200 while reporting per-key failures, and the whole request can fail outright;
%% in both cases the unconfirmed objects are left for orphan GC, so they must not
%% inflate the deleted counter.
delete_batch(Batch) ->
    N = length(Batch),
    case rabbitmq_stream_s3_api:delete(Batch, #{timeout => ?DELETE_TIMEOUT_MS}) of
        ok ->
            inc(?C_OBJECTS_DELETED, N);
        {error, {delete_errors, Errors}} ->
            Failed = length(Errors),
            inc(?C_OBJECTS_DELETED, N - Failed),
            inc(?C_OBJECTS_DELETE_FAILED, Failed),
            %% A delete failure is a routine transient (throttling, a transient
            %% S3 error) and orphan GC reclaims what we miss, so this is info, not
            %% a warning. The objects_delete_failed counter is the alertable signal.
            ?LOG_INFO(
                "Reaper could not delete ~b of ~b objects; leaving them for GC. "
                "First failures: ~p",
                [Failed, N, lists:sublist(Errors, 5)]
            );
        {error, Reason} ->
            inc(?C_OBJECTS_DELETE_FAILED, N),
            ?LOG_INFO(
                "Reaper delete request for ~b objects failed: ~p; leaving them for GC",
                [N, Reason]
            )
    end.

list_and_delete(StreamId) ->
    Prefix = rabbitmq_stream_s3:stream_prefix(StreamId),
    %% Record the tombstone before sweeping. This runs in the reaper's spawned
    %% task (a normal process), not the Khepri trigger sproc, so it can write to
    %% Khepri. The tombstone is the durable "stream deleted" signal that lets GC
    %% reclaim a fragment whose upload completes after this one-shot sweep lists
    %% the prefix (issue #196). Best effort: if it fails the sweep still runs,
    %% and the worst case is the pre-existing leak this addresses.
    _ = rabbitmq_stream_s3_db:write_tombstone(StreamId),
    Result = list_and_delete_loop(Prefix, start),
    case Result of
        ok ->
            inc(?C_STREAMS_DELETED, 1),
            maybe_clear_tombstone(StreamId, Prefix);
        _ ->
            %% The sweep hit a LIST error and is incomplete; leave the tombstone
            %% so GC reclaims whatever remains and clears it later.
            ok
    end.

%% After a clean sweep, confirm the prefix is empty and drop the tombstone so it
%% does not accumulate. The deletes are async casts to this server, so flush
%% first to ensure they have run, then re-LIST. If anything remains (an upload
%% that completed after the sweep listed its page, the #196 race), keep the
%% tombstone: GC will reclaim the straggler and clear the tombstone then.
maybe_clear_tombstone(StreamId, Prefix) ->
    _ = flush(),
    case rabbitmq_stream_s3_api:list(Prefix, start) of
        {ok, [], done} ->
            _ = rabbitmq_stream_s3_db:delete_tombstone(StreamId),
            ok;
        _ ->
            ok
    end.

%% Synchronous barrier: returns only after all delete casts enqueued before it
%% have been processed by handle_batch/2.
flush() ->
    gen_batch_server:call(?MODULE, flush).

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
