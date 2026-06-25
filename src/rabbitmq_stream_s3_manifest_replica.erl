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

== Sequence numbers and partition recovery ==

Edits carry a monotonic sequence number and epoch. The replica applies
edits only if `seq == last_seq + 1` and the epoch matches. On gap or
epoch mismatch, the replica requests a full re-sync from the writer.

The sequence number is the writer's manifest revision (the stream's Khepri
`payload_version`), which advances by exactly one per persist in lockstep with
broadcasts. Because it is durable, a writer-side reader that crashes and is
restarted at the same epoch resumes the sequence where the previous incarnation
left off, so its sync is not rejected as stale (a same-epoch restart would
otherwise restart an in-memory counter at zero and wedge replica sync).

No heartbeat or reconnection mechanism is needed because:
- Message loss on a live connection is detected when the next edit
  arrives (gap in sequence). The persist interval (default 2s)
  bounds the window before the next edit.
- Network partitions cause the stream coordinator to restart the
  acceptor. Acceptor restart fires the `on_init(acceptor, ...)` hook
  which re-registers with the writer, triggering a fresh sync.
- An inactive stream (no writes) cannot grow disk regardless of
  whether the replica's manifest is stale.
""".

-behaviour(gen_server).

-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-define(TABLE, rabbitmq_stream_s3_manifest_cache).

-define(C_RESYNCS_REQUESTED, 1).
-define(C_SYNCS_REJECTED, 2).
-define(COUNTERS, [
    {resyncs_requested, ?C_RESYNCS_REQUESTED, counter,
        "Re-syncs a manifest replica requested after a broadcast gap or epoch mismatch"},
    {syncs_rejected, ?C_SYNCS_REJECTED, counter,
        "Syncs a manifest replica dropped because they were older than the cached epoch or sequence"}
]).
-define(COUNTER_KEY, {?MODULE, counter}).

-export([start_link/0]).
-export([init_counters/0]).
-export([
    get_manifest/1,
    get_manifest_and_epoch/1,
    get_range/1,
    put_manifest/2,
    put_manifest/3,
    apply_edit/2,
    apply_edit/3,
    apply_edits/4,
    apply_edits/5,
    sync/4,
    sync/5,
    register_replica_context/4,
    is_context_registered/1
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
    contexts = #{} :: #{stream_id() => #replica_ctx{}},
    %% Per-stream last applied {seq, epoch, writer_node} for gap detection.
    seqs = #{} :: #{stream_id() => {non_neg_integer(), non_neg_integer(), node()}}
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
        [{_, Manifest, _Epoch}] -> Manifest;
        [] -> undefined
    end.

-doc """
Get the cached manifest together with the writer epoch it was stored at. Direct
ETS read. The epoch is `undefined` when the manifest was stored without one. GC
uses this to skip a stream whose cache lags the committed epoch.
""".
-spec get_manifest_and_epoch(stream_id()) ->
    {#manifest{}, non_neg_integer() | undefined} | undefined.
get_manifest_and_epoch(StreamId) ->
    case ets:lookup(?TABLE, StreamId) of
        [{_, Manifest, Epoch}] -> {Manifest, Epoch};
        [] -> undefined
    end.

-doc "Get the remote tier range for a stream. Direct ETS read.".
-spec get_range(stream_id()) -> rabbitmq_stream_s3:range().
get_range(StreamId) ->
    case ets:lookup(?TABLE, StreamId) of
        [{_, #manifest{entries = <<>>}, _}] -> empty;
        [{_, #manifest{first_offset = First, next_offset = Next}, _}] -> {First, Next};
        [] -> empty
    end.

-doc """
Store a manifest locally (synchronous), recording the writer epoch that produced
it. The cached epoch lets a reader (notably GC) tell whether this node's cache
reflects the committed reset or still lags it. The writer node updates its own
cache through this path, so without the epoch its floor would be trusted blindly.
""".
-spec put_manifest(stream_id(), #manifest{}, non_neg_integer()) -> ok.
put_manifest(StreamId, Manifest, Epoch) ->
    gen_server:call(?MODULE, {put_manifest, StreamId, Manifest, Epoch}).

-doc """
Store a manifest locally without a known epoch (the cached epoch is left
undefined). Only for callers that do not participate in epoch-gated reads.
""".
-spec put_manifest(stream_id(), #manifest{}) -> ok.
put_manifest(StreamId, Manifest) ->
    gen_server:call(?MODULE, {put_manifest, StreamId, Manifest, undefined}).

-doc "Apply an edit locally (synchronous).".
-spec apply_edit(stream_id(), #edit{}) -> ok.
apply_edit(StreamId, Edit) ->
    gen_server:call(?MODULE, {apply_edit, StreamId, Edit}).

-doc "Apply an edit on a remote node. Fire-and-forget.".
-spec apply_edit(stream_id(), #edit{}, node()) -> ok.
apply_edit(StreamId, Edit, Node) ->
    gen_server:cast({?MODULE, Node}, {apply_edit, StreamId, Edit}).

-doc "Apply sequenced edits on a remote node. Fire-and-forget.".
-spec apply_edits(stream_id(), [#edit{}], non_neg_integer(), non_neg_integer(), node()) -> ok.
apply_edits(StreamId, Edits, Seq, Epoch, Node) ->
    gen_server:cast({?MODULE, Node}, {apply_edits, StreamId, Edits, Seq, Epoch, node()}).

-doc "Apply sequenced edits locally (synchronous).".
-spec apply_edits(stream_id(), [#edit{}], non_neg_integer(), non_neg_integer()) ->
    ok | {error, gap}.
apply_edits(StreamId, Edits, Seq, Epoch) ->
    gen_server:call(?MODULE, {apply_edits, StreamId, Edits, Seq, Epoch, node()}).

-doc "Send a sync (full manifest reset) to a remote node. Fire-and-forget.".
-spec sync(stream_id(), non_neg_integer(), non_neg_integer(), #manifest{}, node()) -> ok.
sync(StreamId, Seq, Epoch, Manifest, Node) ->
    gen_server:cast({?MODULE, Node}, {sync, StreamId, Seq, Epoch, Manifest, node()}).

-doc "Apply a sync locally (synchronous).".
-spec sync(stream_id(), non_neg_integer(), non_neg_integer(), #manifest{}) -> ok.
sync(StreamId, Seq, Epoch, Manifest) ->
    gen_server:call(?MODULE, {sync, StreamId, Seq, Epoch, Manifest, node()}).

-doc """
Register osiris log context for a stream on this node.
Called from the acceptor hook when a replica starts.
""".
-spec register_replica_context(
    stream_id(), file:filename_all(), atomics:atomics_ref(), counters:counters_ref()
) -> ok.
register_replica_context(StreamId, Dir, Shared, Counter) ->
    gen_server:call(?MODULE, {register_replica_context, StreamId, Dir, Shared, Counter}).

-doc "Whether a replica context is registered for the stream on this node.".
-spec is_context_registered(stream_id()) -> boolean().
is_context_registered(StreamId) ->
    gen_server:call(?MODULE, {is_context_registered, StreamId}).

%% ------------------------------------------------------------------
%% gen_server callbacks
%% ------------------------------------------------------------------

init([]) ->
    _ = ets:new(?TABLE, [named_table, public, set, {read_concurrency, true}]),
    {ok, #state{}}.

handle_call({put_manifest, StreamId, Manifest, Epoch}, _From, State) ->
    write_manifest(StreamId, Manifest, Epoch),
    {reply, ok, State};
handle_call(
    {sync, StreamId, Seq, Epoch, Manifest, WriterNode},
    _From,
    #state{seqs = Seqs, contexts = Ctxs} = State
) ->
    Seqs1 = maybe_apply_sync(StreamId, Seq, Epoch, Manifest, WriterNode, Seqs, Ctxs),
    {reply, ok, State#state{seqs = Seqs1}};
handle_call(
    {apply_edits, StreamId, Edits, Seq, Epoch, WriterNode}, _From, #state{seqs = Seqs} = State
) ->
    case maps:get(StreamId, Seqs, undefined) of
        {LastSeq, Epoch, _} when Seq =:= LastSeq + 1 ->
            case ets:lookup(?TABLE, StreamId) of
                [{_, Manifest0, _}] ->
                    case apply_edits_catching(StreamId, Edits, Manifest0) of
                        {ok, Manifest} ->
                            write_manifest(StreamId, Manifest, Epoch),
                            maybe_evaluate_retention(StreamId, Manifest0, Manifest, State),
                            {reply, ok, State#state{
                                seqs = Seqs#{StreamId => {Seq, Epoch, WriterNode}}
                            }};
                        {error, _} ->
                            request_resync(StreamId, WriterNode),
                            {reply, {error, apply_failed}, State}
                    end;
                [] ->
                    {reply, ok, State#state{seqs = Seqs#{StreamId => {Seq, Epoch, WriterNode}}}}
            end;
        _ ->
            request_resync(StreamId, WriterNode),
            {reply, {error, gap}, State}
    end;
handle_call({apply_edit, StreamId, Edit}, _From, State) ->
    Reply =
        case ets:lookup(?TABLE, StreamId) of
            [{_, Manifest0, Epoch0}] ->
                case apply_edits_catching(StreamId, [Edit], Manifest0) of
                    {ok, Manifest} ->
                        write_manifest(StreamId, Manifest, Epoch0),
                        maybe_evaluate_retention(StreamId, Manifest0, Manifest, State),
                        ok;
                    {error, _} = Err ->
                        Err
                end;
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
handle_call({is_context_registered, StreamId}, _From, #state{contexts = Ctxs} = State) ->
    {reply, maps:is_key(StreamId, Ctxs), State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown}, State}.

handle_cast({put_manifest, StreamId, Manifest, Epoch}, State) ->
    write_manifest(StreamId, Manifest, Epoch),
    {noreply, State};
handle_cast({apply_edit, StreamId, Edit}, State) ->
    case ets:lookup(?TABLE, StreamId) of
        [{_, Manifest0, Epoch0}] ->
            case apply_edits_catching(StreamId, [Edit], Manifest0) of
                {ok, Manifest} ->
                    write_manifest(StreamId, Manifest, Epoch0),
                    maybe_evaluate_retention(StreamId, Manifest0, Manifest, State);
                {error, _} ->
                    %% No writer node on this path to resync from; keep the last
                    %% good manifest and leave recovery to the next broadcast
                    %% gap or sync.
                    ok
            end;
        [] ->
            ok
    end,
    {noreply, State};
handle_cast(
    {sync, StreamId, Seq, Epoch, Manifest, WriterNode},
    #state{seqs = Seqs, contexts = Ctxs} = State
) ->
    Seqs1 = maybe_apply_sync(StreamId, Seq, Epoch, Manifest, WriterNode, Seqs, Ctxs),
    {noreply, State#state{seqs = Seqs1}};
handle_cast({apply_edits, StreamId, Edits, Seq, Epoch, WriterNode}, #state{seqs = Seqs} = State) ->
    case maps:get(StreamId, Seqs, undefined) of
        {LastSeq, Epoch, _} when Seq =:= LastSeq + 1 ->
            %% In sequence: apply edits.
            case ets:lookup(?TABLE, StreamId) of
                [{_, Manifest0, _}] ->
                    case apply_edits_catching(StreamId, Edits, Manifest0) of
                        {ok, Manifest} ->
                            write_manifest(StreamId, Manifest, Epoch),
                            maybe_evaluate_retention(StreamId, Manifest0, Manifest, State),
                            {noreply, State#state{
                                seqs = Seqs#{StreamId => {Seq, Epoch, WriterNode}}
                            }};
                        {error, _} ->
                            %% The edit is structurally inconsistent with this
                            %% replica's manifest: it has diverged (or the edit
                            %% is malformed). Don't apply a corrupt edit or crash
                            %% the per-node cache shared by every stream - keep
                            %% the last good manifest, leave the sequence
                            %% unadvanced, and force a full resync.
                            request_resync(StreamId, WriterNode),
                            {noreply, State}
                    end;
                [] ->
                    {noreply, State#state{seqs = Seqs#{StreamId => {Seq, Epoch, WriterNode}}}}
            end;
        _ ->
            %% Gap or epoch mismatch: request re-sync from writer.
            request_resync(StreamId, WriterNode),
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

write_manifest(StreamId, Manifest, Epoch) ->
    ets:insert(?TABLE, {StreamId, Manifest, Epoch}).

%% Apply a batch of edits, catching any failure from apply_edit/2. apply_edit/2
%% raises on a structurally inconsistent edit (a gap, a diverged replica, or a
%% malformed edit). This per-node process owns the manifest cache for every
%% stream on the node, so an uncaught crash here would destroy all of them;
%% isolate the failure to the one stream instead and let the caller request a
%% resync. The original manifest is returned untouched on failure (we never
%% write a partially-applied or corrupt manifest).
-spec apply_edits_catching(stream_id(), [#edit{}], #manifest{}) ->
    {ok, #manifest{}} | {error, term()}.
apply_edits_catching(StreamId, Edits, Manifest0) ->
    try
        {ok,
            lists:foldl(
                fun(Edit, Acc) -> rabbitmq_stream_s3_manifest:apply_edit(Edit, Acc) end,
                Manifest0,
                Edits
            )}
    catch
        Class:Reason ->
            ?LOG_WARNING(
                "Manifest replica for stream ~ts could not apply an edit "
                "(~ts:~p); keeping the last good manifest and resyncing",
                [StreamId, Class, Reason],
                #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
            ),
            {error, {Class, Reason}}
    end.

%% A sync is a full manifest reset tagged with the writer's epoch and sequence.
%% Casts from different writer nodes can be reordered, so a delayed sync from a
%% deposed writer can arrive after a newer writer's sync. Applying it would roll
%% the cache's epoch and sequence backward (an Epoch monotonicity violation) and
%% re-pin the stream to the old writer node, serving a stale manifest until the
%% next gap triggers a re-sync. Drop any sync that is not at least as new as
%% what is recorded, comparing epoch first and then sequence so a higher epoch
%% always wins regardless of where its sequence restarted.
maybe_apply_sync(StreamId, Seq, Epoch, Manifest, WriterNode, Seqs, Ctxs) ->
    Recorded = maps:get(StreamId, Seqs, undefined),
    case is_stale_sync(Epoch, Seq, Recorded) of
        true ->
            {Seq0, Epoch0, _} = Recorded,
            inc(?C_SYNCS_REJECTED, 1),
            ?LOG_INFO(
                "Manifest replica for stream ~ts dropped a stale sync "
                "(epoch ~b seq ~b from node ~p) behind the cached epoch ~b seq ~b",
                [StreamId, Epoch, Seq, WriterNode, Epoch0, Seq0],
                #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
            ),
            Seqs;
        false ->
            write_manifest(StreamId, Manifest, Epoch),
            seed_first_offset_counter(StreamId, Manifest, Ctxs),
            Seqs#{StreamId => {Seq, Epoch, WriterNode}}
    end.

%% A sync is stale only when an entry is already recorded and the incoming
%% (epoch, seq) is strictly older. Epoch dominates sequence so a higher epoch is
%% never stale even if its sequence restarted below the deposed writer's.
-spec is_stale_sync(
    non_neg_integer(), non_neg_integer(), {non_neg_integer(), non_neg_integer(), node()} | undefined
) -> boolean().
is_stale_sync(_Epoch, _Seq, undefined) ->
    false;
is_stale_sync(Epoch, Seq, {Seq0, Epoch0, _}) ->
    {Epoch, Seq} < {Epoch0, Seq0}.

request_resync(StreamId, WriterNode) ->
    inc(?C_RESYNCS_REQUESTED, 1),
    ?LOG_INFO(
        "Manifest replica for stream ~ts detected a broadcast gap or epoch "
        "mismatch; requesting a re-sync from writer node ~p",
        [StreamId, WriterNode],
        #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
    ),
    gen_server:cast(
        {via, rabbitmq_stream_s3_registry, {StreamId, WriterNode}},
        {resync, node()}
    ).

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

maybe_evaluate_retention(
    StreamId,
    #manifest{next_offset = OldManifestNextOffset},
    #manifest{
        first_offset = ManifestFirstOffset,
        first_timestamp = ManifestFirstTimestamp,
        next_offset = NewManifestNextOffset
    },
    #state{contexts = Ctxs}
) when NewManifestNextOffset > OldManifestNextOffset ->
    case maps:get(StreamId, Ctxs, undefined) of
        #replica_ctx{dir = Dir, shared = Shared, counter = Cnt} ->
            Spec = [{'fun', rabbitmq_stream_s3_hooks:local_retention_fun(StreamId)}],
            EvalFun = fun
                ({{FstOff, _}, FstTs, NumSegLeft}) when
                    is_integer(FstOff), is_integer(FstTs)
                ->
                    osiris_log_shared:set_first_chunk_id(Shared, FstOff),
                    counters:put(
                        Cnt,
                        ?C_OSIRIS_LOG_FIRST_OFFSET,
                        min(FstOff, ManifestFirstOffset)
                    ),
                    %% Correct the first timestamp alongside the first offset.
                    %% osiris sets it from the local tier's oldest surviving
                    %% segment; the remote tier holds older data here (the
                    %% manifest advanced), so the oldest message is the
                    %% manifest's first_timestamp. Without this the UI's oldest
                    %% message marches forward as local retention deletes
                    %% segments. The manifest advanced past the old next_offset,
                    %% so it is non-empty and first_timestamp is a real
                    %% timestamp, not the empty-manifest -1 sentinel.
                    counters:put(
                        Cnt,
                        ?C_OSIRIS_LOG_FIRST_TIMESTAMP,
                        min(FstTs, ManifestFirstTimestamp)
                    ),
                    counters:put(Cnt, ?C_OSIRIS_LOG_SEGMENTS, NumSegLeft);
                (_) ->
                    ok
            end,
            osiris_retention:eval(StreamId, Dir, Spec, EvalFun);
        undefined ->
            ok
    end;
maybe_evaluate_retention(
    _StreamId, _OldManifest = #manifest{}, _NewManifest = #manifest{}, #state{}
) ->
    ok.

%% Seed the osiris first-offset and first-timestamp counters from the manifest
%% on sync. Without this, the counters reflect only the local tier until the
%% first retention evaluation or edit arrives, which may never happen on an idle
%% stream.
seed_first_offset_counter(_StreamId, #manifest{entries = <<>>}, _Ctxs) ->
    ok;
seed_first_offset_counter(
    StreamId, #manifest{first_offset = ManifestFirst, first_timestamp = ManifestFirstTs}, Ctxs
) ->
    case maps:get(StreamId, Ctxs, undefined) of
        #replica_ctx{counter = Cnt} ->
            LocalFirst = counters:get(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET),
            counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_OFFSET, min(LocalFirst, ManifestFirst)),
            LocalFirstTs = counters:get(Cnt, ?C_OSIRIS_LOG_FIRST_TIMESTAMP),
            counters:put(Cnt, ?C_OSIRIS_LOG_FIRST_TIMESTAMP, min(LocalFirstTs, ManifestFirstTs));
        undefined ->
            ok
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

is_stale_sync_test() ->
    Node = node(),
    %% Nothing recorded yet: a first sync is never stale.
    ?assertNot(is_stale_sync(5, 10, undefined)),
    %% Same writer making forward progress, and an idempotent duplicate.
    ?assertNot(is_stale_sync(5, 11, {10, 5, Node})),
    ?assertNot(is_stale_sync(5, 10, {10, 5, Node})),
    %% A delayed same-epoch sync that arrived out of order is stale.
    ?assert(is_stale_sync(5, 9, {10, 5, Node})),
    %% A higher epoch always wins, even when its sequence restarted lower.
    ?assertNot(is_stale_sync(6, 1, {10, 5, Node})),
    %% A delayed sync from a deposed lower-epoch writer is stale, even with a
    %% higher sequence than the new writer has reached.
    ?assert(is_stale_sync(5, 10, {1, 6, Node})),
    ok.

-endif.
