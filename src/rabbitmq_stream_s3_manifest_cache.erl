%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_manifest_cache).
-moduledoc """
Per-node manifest cache.

Stores manifest roots per stream in a public ETS table. The gen_server
owns the table and handles writes. Readers access ETS directly without
going through the gen_server.
""".

-behaviour(gen_server).

-include("include/rabbitmq_stream_s3.hrl").

-define(TABLE, rabbitmq_stream_s3_manifest_cache).

-export([start_link/0]).
-export([get_manifest/1, get_range/1, put_manifest/2, apply_edit/2]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(state, {}).

-doc "Start the manifest cache under a supervisor.".
-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Get the cached manifest for a stream. Direct ETS read.".
-spec get_manifest(stream_id()) -> #manifest{} | undefined.
get_manifest(StreamId) ->
    case ets:lookup(?TABLE, StreamId) of
        [{_, Manifest}] -> Manifest;
        [] -> undefined
    end.

-doc "Get the remote tier range for a stream. Direct ETS read.".
-spec get_range(stream_id()) -> rabbitmq_stream_s3:range().
get_range(StreamId) ->
    case ets:lookup(?TABLE, StreamId) of
        [{_, #manifest{first_offset = First, next_offset = Next}}] -> {First, Next};
        [] -> empty
    end.

-doc "Seed the cache with a resolved manifest for a stream.".
-spec put_manifest(stream_id(), #manifest{}) -> ok.
put_manifest(StreamId, Manifest) ->
    gen_server:call(?MODULE, {put_manifest, StreamId, Manifest}).

-doc "Apply an edit to a cached manifest.".
-spec apply_edit(stream_id(), #edit{}) -> ok.
apply_edit(StreamId, Edit) ->
    gen_server:call(?MODULE, {apply_edit, StreamId, Edit}).

init([]) ->
    _ = ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
    {ok, #state{}}.

handle_call({put_manifest, StreamId, Manifest}, _From, State) ->
    write_manifest(StreamId, Manifest),
    {reply, ok, State};
handle_call({apply_edit, StreamId, Edit}, _From, State) ->
    Reply =
        case ets:lookup(?TABLE, StreamId) of
            [{_, Manifest0}] ->
                Manifest = rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest0),
                write_manifest(StreamId, Manifest),
                ok;
            [] ->
                {error, not_found}
        end,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

write_manifest(StreamId, Manifest) ->
    ets:insert(?TABLE, {StreamId, Manifest}).
