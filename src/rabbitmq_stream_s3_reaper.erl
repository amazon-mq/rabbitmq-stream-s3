%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_reaper).
-moduledoc """
Batched S3 object deletion.

Collects keys from replica readers, retention evaluation, and stream
deletion, then issues batched `DeleteObjects` calls (up to 1000 keys
per request). Failed deletions are retried with backoff; objects that
remain after retries become orphans for the periodic GC mechanism.

For stream deletion, a short-lived task pages through LIST on the
stream's S3 prefix and sends discovered keys back to this process.
""".

-behaviour(gen_batch_server).

-include("include/rabbitmq_stream_s3.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0]).
-export([delete_objects/2, delete_stream/1]).
-export([init/1, handle_batch/2, terminate/2]).

-record(state, {}).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_batch_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Send object keys for batched deletion.".
-spec delete_objects(stream_id(), [rabbitmq_stream_s3:key()]) -> ok.
delete_objects(_StreamId, []) ->
    ok;
delete_objects(StreamId, Keys) ->
    gen_batch_server:cast(?MODULE, {delete, StreamId, Keys}).

-doc "Delete all remote tier objects for a stream.".
-spec delete_stream(stream_id()) -> ok.
delete_stream(StreamId) ->
    gen_batch_server:cast(?MODULE, {delete_stream, StreamId}).

init([]) ->
    {ok, #state{}}.

handle_batch(Ops, State) ->
    %% Stub: collect keys from ops but do not issue S3 calls yet.
    %% Will be implemented when the replica reader is wired up.
    _ = collect_keys(Ops),
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

collect_keys(Ops) ->
    lists:foldl(
        fun
            ({cast, _Pid, {delete, _StreamId, Keys}}, Acc) ->
                Keys ++ Acc;
            ({cast, _Pid, {delete_stream, _StreamId}}, Acc) ->
                %% TODO: spawn LIST task
                Acc;
            (_, Acc) ->
                Acc
        end,
        [],
        Ops
    ).
