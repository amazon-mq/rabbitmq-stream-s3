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

A sync or edit is applied only for a stream that has a registered reader
context, so neither can create or advance cached state that no member monitor
would ever reclaim. A sync or edit that races ahead of registration (or arrives
after the context is released) is dropped; registering the context then
requests a re-sync from that writer, so the drop is recovered rather than
leaving the cache empty or `seqs` pinned to a sequence with no corresponding
manifest row.
""".

-behaviour(gen_server).

-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-define(TABLE, rabbitmq_stream_s3_manifest_cache).

-define(C_RESYNCS_REQUESTED, 1).
-define(C_SYNCS_REJECTED, 2).
-define(C_SYNCS_DROPPED_NO_CONTEXT, 3).
-define(C_EDITS_DROPPED_NO_CONTEXT, 4).
-define(COUNTERS, [
    {resyncs_requested, ?C_RESYNCS_REQUESTED, counter,
        "Re-syncs a manifest replica requested after a broadcast gap or epoch mismatch"},
    {syncs_rejected, ?C_SYNCS_REJECTED, counter,
        "Syncs a manifest replica dropped because they were older than the cached epoch or sequence"},
    {syncs_dropped_no_context, ?C_SYNCS_DROPPED_NO_CONTEXT, counter,
        "Syncs a manifest replica dropped because no live reader context owned the stream on this node"},
    {edits_dropped_no_context, ?C_EDITS_DROPPED_NO_CONTEXT, counter,
        "Edits a manifest replica dropped because no live reader context owned the stream on this node"}
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
    register_replica_context/5,
    is_context_registered/1,
    evaluate_local_retention/1,
    forget/1
]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-ifdef(TEST).
%% Exercised as a pure predicate by prop_SUITE (the Erlang companion to the
%% manifest-replica-lifecycle P model's epoch-monotonicity invariant).
-export([is_stale_sync/3]).
-endif.

-record(replica_ctx, {
    dir :: file:filename_all(),
    shared :: atomics:atomics_ref(),
    counter :: counters:counters_ref(),
    %% Monitor ref for the osiris member that registered this context. When the
    %% member goes down the context is dropped; see handle_info/2.
    mref :: reference()
}).

-record(state, {
    %% Per-stream osiris context for replica-side retention.
    contexts = #{} :: #{stream_id() => #replica_ctx{}},
    %% Per-stream last applied {seq, epoch, writer_node} for gap detection.
    seqs = #{} :: #{stream_id() => {non_neg_integer(), non_neg_integer(), node()}},
    %% Reverse index from a member monitor ref to its stream, so a DOWN can find
    %% which stream to release. Kept in lockstep with the mref in each context.
    monitors = #{} :: #{reference() => stream_id()},
    %% Streams whose sync was dropped for lack of a context, mapped to the writer
    %% node the dropped sync came from. When a context later registers we request
    %% a re-sync from that node, so a sync that raced ahead of registration is
    %% recovered rather than leaving the cache empty. See maybe_apply_sync/8.
    pending_resync = #{} :: #{stream_id() => node()}
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
    ok | {error, gap} | {error, apply_failed} | {error, no_context} | {error, term()}.
apply_edits(StreamId, Edits, Seq, Epoch) ->
    %% Specced with an {error, _} contract; convert a singleton call exit
    %% (noproc mid-restart, timeout) into a named error so it cannot escape as a
    %% raw OTP exit to a caller matching the tagged failures above.
    try
        gen_server:call(?MODULE, {apply_edits, StreamId, Edits, Seq, Epoch, node()})
    catch
        exit:{timeout, _} ->
            {error, timeout};
        exit:Reason ->
            {error, {manifest_replica_down, Reason}}
    end.

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

Called from the acceptor hook when a replica starts (and from discovery and
reconciliation). `MemberPid` is the osiris member that owns the context; it is
monitored so the context, sequence, and cached row are released when the member
goes down. osiris has no terminate or delete hook, so this monitor is how
per-node replica state is reclaimed when a replica moves off the node or the
stream is deleted. Re-registering for a stream replaces any previous monitor, so
a member restart re-points the monitor at the new incarnation rather than letting
its predecessor's DOWN evict the live context.
""".
-spec register_replica_context(
    stream_id(), pid(), file:filename_all(), atomics:atomics_ref(), counters:counters_ref()
) -> ok.
register_replica_context(StreamId, MemberPid, Dir, Shared, Counter) ->
    gen_server:call(?MODULE, {register_replica_context, StreamId, MemberPid, Dir, Shared, Counter}).

-doc "Whether a replica context is registered for the stream on this node.".
-spec is_context_registered(stream_id()) -> boolean().
is_context_registered(StreamId) ->
    gen_server:call(?MODULE, {is_context_registered, StreamId}).

-doc "Evaluate local-tier retention for a stream on this node.".
-spec evaluate_local_retention(stream_id()) -> ok | {error, term()}.
evaluate_local_retention(StreamId) ->
    %% This is specced ok | {error, term()} and reached over
    %% rabbit_misc:rpc_call from the CLI. gen_server:call/2 exits the caller if
    %% the per-node singleton is mid-restart (noproc) or overloaded (timeout);
    %% convert the exit into a named error so it stays within the contract
    %% rather than escaping as a raw OTP exit.
    try
        gen_server:call(?MODULE, {evaluate_local_retention, StreamId})
    catch
        exit:{timeout, _} ->
            {error, timeout};
        exit:Reason ->
            {error, {manifest_replica_down, Reason}}
    end.

-doc """
Release all per-node state for a stream: its retention context (and member
monitor), its gap-detection sequence, and its cached manifest row.

Replica-side state is released automatically by the member monitor (see
`register_replica_context/5`). This is the explicit path for the writer node,
whose cached row is written by `put_manifest/3` and owned by the replica reader,
which calls this from its `terminate/2` when the stream is torn down.
""".
-spec forget(stream_id()) -> ok.
forget(StreamId) ->
    gen_server:call(?MODULE, {forget, StreamId}).

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
    #state{seqs = Seqs, contexts = Ctxs, pending_resync = Pending} = State
) ->
    {Seqs1, Pending1} = maybe_apply_sync(
        StreamId, Seq, Epoch, Manifest, WriterNode, Seqs, Ctxs, Pending
    ),
    {reply, ok, State#state{seqs = Seqs1, pending_resync = Pending1}};
handle_call(
    {apply_edits, StreamId, Edits, Seq, Epoch, WriterNode},
    _From,
    #state{seqs = Seqs, contexts = Ctxs, pending_resync = Pending} = State
) ->
    case maps:is_key(StreamId, Ctxs) of
        false ->
            Pending1 = drop_no_context(edits, StreamId, Epoch, Seq, WriterNode, Pending),
            {reply, {error, no_context}, State#state{pending_resync = Pending1}};
        true ->
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
                            %% A live context always has seqs and the cached row
                            %% advanced together (write_manifest runs alongside
                            %% every seqs update below), so this should be
                            %% unreachable. Recover via resync rather than trust
                            %% an inconsistent cache.
                            request_resync(StreamId, WriterNode),
                            {reply, {error, gap}, State}
                    end;
                _ ->
                    request_resync(StreamId, WriterNode),
                    {reply, {error, gap}, State}
            end
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
    {register_replica_context, StreamId, MemberPid, Dir, Shared, Counter},
    _From,
    #state{contexts = Ctxs, monitors = Mons0, pending_resync = Pending0} = State
) ->
    %% Replace any previous registration's monitor first: a member restart
    %% re-registers, and the old incarnation's DOWN must not evict the new
    %% context. demonitor with flush drops any DOWN already queued for it.
    Mons1 =
        case maps:get(StreamId, Ctxs, undefined) of
            #replica_ctx{mref = OldRef} ->
                demonitor(OldRef, [flush]),
                maps:remove(OldRef, Mons0);
            undefined ->
                Mons0
        end,
    MRef = monitor(process, MemberPid),
    Ctx = #replica_ctx{dir = Dir, shared = Shared, counter = Counter, mref = MRef},
    %% If a sync for this stream was dropped before this context existed, ask its
    %% writer to re-send it now that a monitored context can own the cached row.
    Pending1 = maybe_request_resync(StreamId, Pending0),
    {reply, ok, State#state{
        contexts = Ctxs#{StreamId => Ctx},
        monitors = Mons1#{MRef => StreamId},
        pending_resync = Pending1
    }};
handle_call({is_context_registered, StreamId}, _From, #state{contexts = Ctxs} = State) ->
    {reply, maps:is_key(StreamId, Ctxs), State};
handle_call({evaluate_local_retention, StreamId}, _From, #state{contexts = Ctxs} = State) ->
    Reply =
        case maps:get(StreamId, Ctxs, undefined) of
            #replica_ctx{} = Ctx ->
                case get_manifest(StreamId) of
                    #manifest{entries = <<>>} ->
                        %% No remote tier yet: nothing has been uploaded, so no
                        %% local segment is safe to delete. Returning early also
                        %% avoids feeding the empty manifest's -1 first_timestamp
                        %% sentinel into the counter (see run_local_retention).
                        ok;
                    #manifest{} = Manifest ->
                        run_local_retention(StreamId, Manifest, Ctx),
                        ok;
                    undefined ->
                        {error, manifest_not_resolved}
                end;
            undefined ->
                {error, {not_found, StreamId}}
        end,
    {reply, Reply, State};
handle_call({forget, StreamId}, _From, State) ->
    {reply, ok, release_stream(StreamId, State)};
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
    #state{seqs = Seqs, contexts = Ctxs, pending_resync = Pending} = State
) ->
    {Seqs1, Pending1} = maybe_apply_sync(
        StreamId, Seq, Epoch, Manifest, WriterNode, Seqs, Ctxs, Pending
    ),
    {noreply, State#state{seqs = Seqs1, pending_resync = Pending1}};
handle_cast(
    {apply_edits, StreamId, Edits, Seq, Epoch, WriterNode},
    #state{seqs = Seqs, contexts = Ctxs, pending_resync = Pending} = State
) ->
    case maps:is_key(StreamId, Ctxs) of
        false ->
            Pending1 = drop_no_context(edits, StreamId, Epoch, Seq, WriterNode, Pending),
            {noreply, State#state{pending_resync = Pending1}};
        true ->
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
                                    %% The edit is structurally inconsistent with
                                    %% this replica's manifest: it has diverged
                                    %% (or the edit is malformed). Don't apply a
                                    %% corrupt edit or crash the per-node cache
                                    %% shared by every stream - keep the last
                                    %% good manifest, leave the sequence
                                    %% unadvanced, and force a full resync.
                                    request_resync(StreamId, WriterNode),
                                    {noreply, State}
                            end;
                        [] ->
                            %% A live context always has seqs and the cached row
                            %% advanced together, so this should be unreachable.
                            %% Recover via resync rather than trust an
                            %% inconsistent cache.
                            request_resync(StreamId, WriterNode),
                            {noreply, State}
                    end;
                _ ->
                    %% Gap or epoch mismatch: request re-sync from writer.
                    request_resync(StreamId, WriterNode),
                    {noreply, State}
            end
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, _Pid, _Reason}, #state{monitors = Mons} = State) ->
    %% A registered osiris member went down. Release its stream's state. A DOWN
    %% whose ref is not in the map belongs to a registration already superseded
    %% (re-register demonitored+flushed it) and is ignored.
    case maps:get(MRef, Mons, undefined) of
        undefined ->
            {noreply, State};
        StreamId ->
            {noreply, release_stream(StreamId, State)}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

write_manifest(StreamId, Manifest, Epoch) ->
    ets:insert(?TABLE, {StreamId, Manifest, Epoch}).

%% Drop all per-node state for a stream: its context (and member monitor), its
%% gap-detection sequence, and its cached manifest row. Used by both the member
%% DOWN handler and the explicit forget/1 (writer reader teardown).
release_stream(
    StreamId,
    #state{contexts = Ctxs, seqs = Seqs, monitors = Mons, pending_resync = Pending} = State
) ->
    Mons1 =
        case maps:get(StreamId, Ctxs, undefined) of
            #replica_ctx{mref = MRef} ->
                demonitor(MRef, [flush]),
                maps:remove(MRef, Mons);
            undefined ->
                Mons
        end,
    ets:delete(?TABLE, StreamId),
    State#state{
        contexts = maps:remove(StreamId, Ctxs),
        seqs = maps:remove(StreamId, Seqs),
        monitors = Mons1,
        pending_resync = maps:remove(StreamId, Pending)
    }.

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
maybe_apply_sync(StreamId, Seq, Epoch, Manifest, WriterNode, Seqs, Ctxs, Pending) ->
    case maps:is_key(StreamId, Ctxs) of
        false ->
            %% No live reader context owns this stream on this node: the member
            %% has gone down (release_stream/2 already dropped its state) or has
            %% not registered yet. Applying the sync would re-insert an ETS row
            %% and sequence with no monitor to ever reclaim them. Drop it, and
            %% remember the writer so registering a context can request a re-sync
            %% (a sync that raced ahead of registration is then recovered rather
            %% than leaving the cache empty).
            {Seqs, drop_no_context(sync, StreamId, Epoch, Seq, WriterNode, Pending)};
        true ->
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
                    {Seqs, Pending};
                false ->
                    write_manifest(StreamId, Manifest, Epoch),
                    seed_first_offset_counter(StreamId, Manifest, Ctxs),
                    %% Evaluate local retention once against the freshly synced
                    %% manifest. On a replica, retention is otherwise only driven
                    %% by edits that advance next_offset; a replica that was
                    %% offline (an upgrade, say), receives a full sync on rejoin,
                    %% and whose stream then stops publishing would never reclaim
                    %% the local segments already durable in the remote tier. This
                    %% one-shot pass reclaims them. It is a no-op for an empty
                    %% manifest or a stream with no registered context here
                    %% (issue #75).
                    maybe_evaluate_retention_on_sync(StreamId, Manifest, Ctxs),
                    %% The sync landed, so any pending re-sync for this stream is
                    %% satisfied.
                    {Seqs#{StreamId => {Seq, Epoch, WriterNode}}, maps:remove(StreamId, Pending)}
            end
    end.

%% Run local retention against a just-synced manifest, guarding on the same
%% conditions run_local_retention requires: a registered replica context on this
%% node and a non-empty manifest (next_offset > 0, so the first_timestamp
%% sentinel never reaches the counters). Unlike the edit-driven
%% maybe_evaluate_retention/4 this does not require next_offset to have advanced,
%% because a sync establishes the manifest wholesale rather than moving it
%% forward.
maybe_evaluate_retention_on_sync(StreamId, #manifest{next_offset = Next} = Manifest, Ctxs) when
    Next > 0
->
    case maps:get(StreamId, Ctxs, undefined) of
        #replica_ctx{} = Ctx ->
            run_local_retention(StreamId, Manifest, Ctx);
        undefined ->
            ok
    end;
maybe_evaluate_retention_on_sync(_StreamId, #manifest{}, _Ctxs) ->
    ok.

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

%% Drop a sync or edit for a stream with no live reader context on this node
%% (see maybe_apply_sync/8 and the {apply_edits, ...} handlers). Remember the
%% writer node in pending_resync so registering a context later requests a
%% re-sync via maybe_request_resync/2, recovering the message that raced ahead
%% of registration instead of leaving the cache empty or the sequence pinned
%% with no corresponding manifest row.
drop_no_context(Kind, StreamId, Epoch, Seq, WriterNode, Pending) ->
    inc(
        case Kind of
            sync -> ?C_SYNCS_DROPPED_NO_CONTEXT;
            edits -> ?C_EDITS_DROPPED_NO_CONTEXT
        end,
        1
    ),
    ?LOG_INFO(
        "Manifest replica for stream ~ts dropped ~ts "
        "(epoch ~b seq ~b from node ~p) with no live reader context",
        [StreamId, Kind, Epoch, Seq, WriterNode],
        #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
    ),
    Pending#{StreamId => WriterNode}.

request_resync(StreamId, WriterNode) ->
    ?LOG_INFO(
        "Manifest replica for stream ~ts detected a broadcast gap or epoch "
        "mismatch; requesting a re-sync from writer node ~p",
        [StreamId, WriterNode],
        #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
    ),
    send_resync(StreamId, WriterNode).

%% Request a re-sync from the writer whose sync was dropped for lack of a context
%% (recorded in pending_resync), now that a context has registered to own the
%% cached row. Without this, a sync that raced ahead of registration is dropped
%% and never re-sent (the writer's register is idempotent), leaving the cache
%% empty. See maybe_apply_sync/8.
maybe_request_resync(StreamId, Pending) ->
    case maps:take(StreamId, Pending) of
        {WriterNode, Pending1} ->
            ?LOG_INFO(
                "Manifest replica for stream ~ts registered a context with a "
                "sync pending; requesting a re-sync from writer node ~p",
                [StreamId, WriterNode],
                #{domain => ?RMQLOG_DOMAIN_STREAM_S3}
            ),
            send_resync(StreamId, WriterNode),
            Pending1;
        error ->
            Pending
    end.

send_resync(StreamId, WriterNode) ->
    inc(?C_RESYNCS_REQUESTED, 1),
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
    #manifest{next_offset = NewManifestNextOffset} = Manifest,
    #state{contexts = Ctxs}
) when NewManifestNextOffset > OldManifestNextOffset ->
    case maps:get(StreamId, Ctxs, undefined) of
        #replica_ctx{} = Ctx ->
            run_local_retention(StreamId, Manifest, Ctx);
        undefined ->
            ok
    end;
maybe_evaluate_retention(
    _StreamId, _OldManifest = #manifest{}, _NewManifest = #manifest{}, #state{}
) ->
    ok.

%% Evaluate local-tier retention for a stream on this node against `Manifest`.
%% Deletes local segments whose data is already uploaded (up to the manifest's
%% next_offset, via local_retention_fun) and corrects the first offset/timestamp
%% counters. Shared by the automatic edit-driven path (maybe_evaluate_retention)
%% and the on-demand CLI path (evaluate_local_retention/1). Callers must ensure
%% the manifest is non-empty so the -1 first_timestamp sentinel never reaches the
%% counter.
run_local_retention(
    StreamId,
    #manifest{first_offset = ManifestFirstOffset, first_timestamp = ManifestFirstTimestamp},
    #replica_ctx{dir = Dir, shared = Shared, counter = Cnt}
) ->
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
            %% segment; the remote tier holds older data here, so the
            %% oldest message is the manifest's first_timestamp. Without
            %% this the UI's oldest message marches forward as local
            %% retention deletes segments.
            counters:put(
                Cnt,
                ?C_OSIRIS_LOG_FIRST_TIMESTAMP,
                min(FstTs, ManifestFirstTimestamp)
            ),
            counters:put(Cnt, ?C_OSIRIS_LOG_SEGMENTS, NumSegLeft);
        (_) ->
            ok
    end,
    osiris_retention:eval(StreamId, Dir, Spec, EvalFun).

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

%% When the singleton is not running (noproc), the synchronous entry points must
%% convert the gen_server:call exit into a named error rather than letting the
%% raw OTP exit escape their {error, _} contracts. The server is not registered
%% during a plain eunit run, so a call to ?MODULE exits with noproc.
evaluate_local_retention_converts_call_exit_test() ->
    ?assertMatch({error, {manifest_replica_down, _}}, evaluate_local_retention(<<"s">>)).

apply_edits_converts_call_exit_test() ->
    ?assertMatch({error, {manifest_replica_down, _}}, apply_edits(<<"s">>, [], 0, 0)).

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
