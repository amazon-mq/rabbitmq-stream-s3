%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_reaper).
-moduledoc """
Batched S3 object deletion.

Collects keys from replica readers, retention evaluation, and the stream
deletion lister, then issues batched DeleteObjects calls (up to 1000 keys
per request).

Keys arrive two ways:

- `delete_objects/2` casts keys fire-and-forget. The set of keys from
  retention and replica readers is bounded, so a cast cannot grow the
  mailbox without bound.
- `delete_objects_sync/2` submits keys and replies only once they have been
  deleted. `rabbitmq_stream_s3_lister` uses this to page through a stream's
  prefix under backpressure: it lists one page, hands it over synchronously,
  and only lists the next page once this reaper has drained the previous one.
""".

-behaviour(gen_batch_server).

-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([delete_objects/2, delete_objects_sync/2]).
-export([init/1, handle_batch/2, terminate/2]).
-export([init_counters/0]).

-define(MAX_BATCH, 1000).
%% The reaper has no upstream caller waiting on a deadline. Under sustained
%% S3 throttling the pool's default 5s checkout timeout can starve the reaper
%% and crash the gen_batch_server. Use a generous timeout consistent with
%% other write-side background work (see `retention_task_timeout`).
-define(DELETE_TIMEOUT_MS, 60_000).

-define(C_OBJECTS_DELETED, 1).
-define(C_OBJECTS_DELETE_FAILED, 2).
-define(COUNTERS, [
    {objects_deleted, ?C_OBJECTS_DELETED, counter,
        "Individual objects confirmed deleted via the reaper (retention or stream deletion)"},
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

-doc """
Send object keys for batched deletion and block until they are deleted.

Returns `ok` once this batch has been processed, regardless of whether every
individual object was confirmed deleted (unconfirmed ones are left for orphan
GC). This is the backpressure primitive the lister relies on: the reply does
not arrive until the reaper has drained the batch.

The call waits `infinity`. Each S3 request the reaper makes is individually
bounded by `?DELETE_TIMEOUT_MS`, so `handle_batch` always completes in bounded
time; a finite call timeout here would instead measure the caller's wait
against the whole batch (which folds in unrelated fire-and-forget casts and
splits into up to `?MAX_BATCH`-key sub-batches run sequentially) and could
crash the lister while deletion is still making progress.
""".
-spec delete_objects_sync(stream_id(), [rabbitmq_stream_s3:key()]) -> ok.
delete_objects_sync(_StreamId, []) ->
    ok;
delete_objects_sync(_StreamId, Keys) ->
    gen_batch_server:call(?MODULE, {delete, Keys}, infinity).

init([]) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_STREAM_S3}),
    {ok, #state{}}.

handle_batch(Ops, State) ->
    Keys = collect(Ops),
    _ = delete_batched(Keys),
    {ok, replies(Ops), State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

%% Keys arrive from both fire-and-forget casts and synchronous calls.
collect(Ops) ->
    lists:append([keys(Op) || Op <- Ops]).

keys({cast, {delete, K}}) -> K;
keys({call, _From, {delete, K}}) -> K;
keys(_) -> [].

%% Reply to every synchronous caller once its batch has been deleted above.
replies(Ops) ->
    [{reply, From, ok} || {call, From, {delete, _}} <- Ops].

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
