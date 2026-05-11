%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_manifest_replica).
-moduledoc """
Per-node manifest store and replica-side coordinator.

Stores manifest roots per stream in a public ETS table. Readers access
ETS directly. Writes go through the gen_server.

On replica nodes, also holds osiris log context (dir, shared) per stream
so it can trigger retention evaluation when manifest edits advance the
uploaded range.
""".

-behaviour(gen_server).

-include("include/rabbitmq_stream_s3.hrl").

-define(TABLE, rabbitmq_stream_s3_manifest_cache).

-export([start_link/0]).
-export([
    get_manifest/1,
    get_range/1,
    put_manifest/2,
    put_manifest/3,
    apply_edit/2,
    apply_edit/3,
    register_replica_context/4
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(replica_ctx, {
    dir :: file:filename_all(),
    shared :: atomics:atomics_ref(),
    counter :: counters:counters_ref()
}).

-record(state, {
    %% Per-stream osiris context for replica-side retention.
    contexts = #{} :: #{stream_id() => #replica_ctx{}}
}).

%% ------------------------------------------------------------------
%% API
%% ------------------------------------------------------------------

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

-doc "Store a manifest locally (synchronous).".
-spec put_manifest(stream_id(), #manifest{}) -> ok.
put_manifest(StreamId, Manifest) ->
    gen_server:call(?MODULE, {put_manifest, StreamId, Manifest}).

-doc "Apply an edit locally (synchronous).".
-spec apply_edit(stream_id(), #edit{}) -> ok.
apply_edit(StreamId, Edit) ->
    gen_server:call(?MODULE, {apply_edit, StreamId, Edit}).

-doc "Seed the cache on a remote node. Fire-and-forget.".
-spec put_manifest(stream_id(), #manifest{}, node()) -> ok.
put_manifest(StreamId, Manifest, Node) ->
    gen_server:cast({?MODULE, Node}, {put_manifest, StreamId, Manifest}).

-doc "Apply an edit on a remote node. Fire-and-forget.".
-spec apply_edit(stream_id(), #edit{}, node()) -> ok.
apply_edit(StreamId, Edit, Node) ->
    gen_server:cast({?MODULE, Node}, {apply_edit, StreamId, Edit}).

-doc """
Register osiris log context for a stream on this node.
Called from the acceptor hook when a replica starts.
""".
-spec register_replica_context(
    stream_id(), file:filename_all(), atomics:atomics_ref(), counters:counters_ref()
) -> ok.
register_replica_context(StreamId, Dir, Shared, Counter) ->
    gen_server:call(?MODULE, {register_replica_context, StreamId, Dir, Shared, Counter}).

%% ------------------------------------------------------------------
%% gen_server callbacks
%% ------------------------------------------------------------------

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
                maybe_evaluate_retention(StreamId, Manifest0, Manifest, State),
                ok;
            [] ->
                {error, not_found}
        end,
    {reply, Reply, State};
handle_call(
    {register_replica_context, StreamId, Dir, Shared, Counter},
    _From,
    #state{contexts = Ctxs} = State
) ->
    Ctx = #replica_ctx{dir = Dir, shared = Shared, counter = Counter},
    {reply, ok, State#state{contexts = Ctxs#{StreamId => Ctx}}};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast({put_manifest, StreamId, Manifest}, State) ->
    write_manifest(StreamId, Manifest),
    {noreply, State};
handle_cast({apply_edit, StreamId, Edit}, State) ->
    case ets:lookup(?TABLE, StreamId) of
        [{_, Manifest0}] ->
            Manifest = rabbitmq_stream_s3_manifest:apply_edit(Edit, Manifest0),
            write_manifest(StreamId, Manifest),
            maybe_evaluate_retention(StreamId, Manifest0, Manifest, State);
        [] ->
            ok
    end,
    {noreply, State};
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

maybe_evaluate_retention(StreamId, OldManifest, NewManifest, #state{contexts = Ctxs}) ->
    case NewManifest#manifest.next_offset > OldManifest#manifest.next_offset of
        true ->
            case maps:get(StreamId, Ctxs, undefined) of
                #replica_ctx{dir = Dir, shared = Shared, counter = Cnt} ->
                    Spec = [{'fun', rabbitmq_stream_s3_hooks:local_retention_fun(StreamId)}],
                    EvalFun = fun
                        ({{FstOff, _}, _FstTs, NumSegLeft}) when is_integer(FstOff) ->
                            osiris_log_shared:set_first_chunk_id(Shared, FstOff),
                            counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET, FstOff),
                            counters:put(Cnt, ?C_OSIRIS_LOG_SEGMENTS, NumSegLeft);
                        (_) ->
                            ok
                    end,
                    osiris_retention:eval(StreamId, Dir, Spec, EvalFun);
                undefined ->
                    ok
            end;
        false ->
            ok
    end.
