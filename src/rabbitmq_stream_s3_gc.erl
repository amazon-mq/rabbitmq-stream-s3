%% Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
%% SPDX-License-Identifier: Apache-2.0

-module(rabbitmq_stream_s3_gc).
-moduledoc """
Garbage-collects dangling remote tier objects.

LISTs S3 objects under `rabbitmq/stream/` and identifies orphans by comparing
object keys against current state in Khepri (epoch) and the manifest replica
ETS cache (first_offset).

Safety relies on monotonicity: epoch only moves forward, and first_offset
normally only moves forward. Eventually-consistent reads can only return stale
(lower) values, which makes the GC more conservative (false negatives), never
less safe.

The one break in that monotonicity is a remote-tier-ahead reset, which rebuilds
the manifest from the local log floor after a timeline change and can *lower*
first_offset, re-tiering live fragments (with fresh UIDs) at offsets below a
floor a concurrent sweep already snapshotted. Three guards keep the sweep safe:

  1. Each candidate deletion is re-validated against the live first_offset
     immediately before the object is deleted (see still_dangling/1) and skipped
     if the floor has dropped to at or below its offset. The reset lowers the
     cached floor before it re-uploads, so a re-tiered fragment is always at or
     above the live floor by the time it exists.
  2. The cross-stream sweep gates on a single strongly-consistent wildcard read of
     every stream's committed metadata (db:list_consistent/0, in run/1), and the
     single-stream path on a per-stream consistent read, so a partitioned or
     deposed node that cannot reach a quorum fails closed rather than deleting on a
     stale local view.
  3. That metadata read (the single wildcard quorum read in run/1) is sampled
     once, up front. A reset that commits after the snapshot, on a node whose
     manifest cache has not yet applied the sync, leaves still_dangling/1
     re-reading a stale-high floor while the committed epoch has moved on, so
     guards 1 and 2 both pass. Each offset-based deletion is therefore
     re-validated once more at the point of deletion (see
     `fresh_enough_to_delete/2`): a fresh quorum read of the committed epoch is
     compared against the cached epoch, and the delete is skipped when they
     differ. A orphan skipped here is reclaimed by a later sweep.

For a stream that is in the committed lookup, objects are classified against its
first_offset and epoch as above. For an object whose stream is NOT in the lookup
(a deleted stream, a stream that never committed a manifest, or one missing a
local manifest replica) the per-stream anchor decides: the anchor
(rabbitmq_stream_s3_db, written before the first fragment, kept alive by a
keep_while on the stream queue) is present for a live stream and absent only once
the queue is gone. A strongly-consistent read of the anchor that returns absent is
a positive "stream deleted" signal, so the object is reaped (reason no_anchor); a
present anchor, or a read that cannot reach a quorum, fails closed and skips. The
consistent read is load-bearing: a stale local read could report a live stream's
anchor absent and reap it.
""".

-include("include/rabbitmq_stream_s3.hrl").
-include("include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([run/0, run/1, run_stream/2, run_stream/3]).

-type mode() :: dry_run | delete.
-type config() :: #{mode => mode(), writer_epoch => non_neg_integer()}.
-type reason() :: below_first_offset | stale_epoch | no_anchor.
-type finding() :: #{stream_id := stream_id(), key := rabbitmq_stream_s3:key(), reason := reason()}.

%% The results of the two live reads the sweep depends on. The read-performing
%% shells (build_lookup/2, build_stream_lookup/2, fresh_enough_to_delete/2,
%% anchor_absent/1) hand these to pure decision functions, so every guard is a
%% total function of read results, testable without a live db or replica cache.
-type consistent_result() :: {ok, rabbitmq_stream_s3_db:entry()} | {error, term()}.
-type cache_result() :: {#manifest{}, osiris:epoch() | undefined} | undefined.

-export_type([mode/0, config/0, finding/0]).

-ifdef(TEST).
%% Exposed to prop_SUITE, which property-tests the pure reap decision against the
%% same invariants the P models in p/ verify. These are the functional cores of
%% the read-performing shells, taking read results as inputs.
-export([
    classify/2,
    still_dangling/1,
    lookup_entry/4,
    stream_lookup_decision/5,
    fresh_enough_decision/3,
    anchor_decision/2,
    cache_at_committed_epoch/2,
    epoch_permits_sweep/2
]).
-endif.

run() ->
    run(#{mode => dry_run}).

-doc """
Run garbage collection. In `dry_run` mode, identifies and logs orphans without
deleting. In `delete` mode, deletes identified orphans via the reaper.
""".
-spec run(config()) -> {ok, [finding()]} | {error, any()}.
run(Config) when is_map(Config) ->
    Mode = maps:get(mode, Config, dry_run),
    %% One strongly-consistent wildcard read snapshots every stream's committed
    %% metadata up front, so the whole sweep pays a single quorum round trip rather
    %% than one per stream. A quorum failure (for example on a minority partition)
    %% fails the entire sweep closed rather than deleting on a stale local view.
    case rabbitmq_stream_s3_db:list_consistent() of
        {ok, Streams} ->
            Lookup = build_lookup(Streams, Config),
            Fun = make_handler(Mode),
            Findings = list_and_classify(
                <<"rabbitmq/stream/">>, start, Lookup, Fun, fun anchor_absent/1, []
            ),
            ?LOG_INFO("GC ~ts complete: ~b dangling object(s)", [Mode, length(Findings)]),
            {ok, Findings};
        {error, Reason} ->
            ?LOG_WARNING(
                "GC ~ts aborted: could not read committed stream metadata with "
                "quorum (~p); sweeping nothing",
                [Mode, Reason]
            ),
            {error, Reason}
    end.

-doc """
Run garbage collection scoped to a single stream. Only lists objects under the
stream's S3 prefix and classifies them against the stream's current epoch and
first_offset. Used by the replica reader after a manifest reset to reclaim
orphaned fragments without a full cross-stream sweep.
""".
-spec run_stream(stream_id(), config()) -> {ok, [finding()]}.
run_stream(StreamId, Config) when is_binary(StreamId), is_map(Config) ->
    Mode = maps:get(mode, Config, dry_run),
    case build_stream_lookup(StreamId, Config) of
        {ok, Lookup} ->
            Prefix = rabbitmq_stream_s3:stream_prefix(StreamId),
            Fun = make_handler(Mode),
            Findings = list_and_classify(Prefix, start, Lookup, Fun, fun anchor_absent/1, []),
            ?LOG_INFO(
                "GC ~ts for stream ~ts complete: ~b dangling object(s)",
                [Mode, StreamId, length(Findings)]
            ),
            {ok, Findings};
        skip ->
            {ok, []}
    end.

-doc """
Run garbage collection scoped to a single stream identified by its vhost and
queue name. Resolves the stream id, then delegates to `run_stream/2`.
""".
-spec run_stream(rabbit_types:vhost(), binary(), config()) ->
    {ok, [finding()]} | {error, {not_found, binary()}}.
run_stream(VHost, QueueName, Config) when
    is_binary(VHost), is_binary(QueueName), is_map(Config)
->
    case rabbitmq_stream_s3_replica_reader:resolve_stream_id(VHost, QueueName) of
        {ok, StreamId} ->
            run_stream(StreamId, Config);
        {error, _} = Err ->
            Err
    end.

make_handler(dry_run) ->
    fun(Finding, Acc) ->
        log_finding(Finding),
        [Finding | Acc]
    end;
make_handler(delete) ->
    fun(#{stream_id := StreamId, key := Key, reason := Reason} = Finding, Acc) ->
        case still_dangling(Finding) of
            true ->
                %% still_dangling/1 passed against the live cache floor, but that
                %% floor is trustworthy only if the cache is at the committed epoch
                %% (a reset may have committed after the sweep snapshot). The
                %% quorum read here runs only for an object already judged
                %% deletable, so it is bounded by the number of orphans, not the
                %% number of listed objects.
                case fresh_enough_to_delete(Reason, StreamId) of
                    true ->
                        log_finding(Finding),
                        rabbitmq_stream_s3_reaper:delete_objects(StreamId, [Key]),
                        [Finding | Acc];
                    false ->
                        Acc
                end;
            false ->
                ?LOG_INFO(
                    "GC: not deleting ~ts (stream=~ts): it is no longer below the "
                    "live first offset; a concurrent manifest reset re-tiered into "
                    "this range",
                    [Key, StreamId]
                ),
                Acc
        end
    end.

%% Re-validate cache freshness against the committed epoch immediately before an
%% offset-based deletion. build_lookup/2 (and build_stream_lookup/2) sampled the
%% committed epoch once, at the start of the sweep; a remote-tier-ahead reset that
%% commits after that snapshot, on a node whose manifest cache has not yet applied
%% the sync, leaves the cached floor stale-high at the old epoch while the
%% committed epoch has advanced. A fresh quorum read of the committed epoch
%% compared against the cached epoch closes that window: delete only when the
%% cache is at the committed epoch, otherwise fail closed (a later sweep reclaims a
%% genuine orphan). Epoch-based (stale manifest) findings need no check: a higher
%% committed epoch only confirms the manifest is stale.
-spec fresh_enough_to_delete(reason(), stream_id()) -> boolean().
fresh_enough_to_delete(stale_epoch, _StreamId) ->
    true;
fresh_enough_to_delete(no_anchor, _StreamId) ->
    %% classify_page/3 already confirmed the anchor absent with a consistent read,
    %% and a deleted stream's anchor never returns (stream_id is unique per
    %% incarnation), so the absence is permanent.
    true;
fresh_enough_to_delete(below_first_offset, StreamId) ->
    Consistent = rabbitmq_stream_s3_db:get_consistent(StreamId),
    fresh_enough_decision(StreamId, Consistent, read_cache_after(Consistent, StreamId)).

%% Pure decision for the offset-based freshness re-check: given the
%% committed-epoch read and the cache read, delete only when the cache holds a
%% manifest at exactly the committed epoch. A quorum-read failure or a cache
%% that lags the committed epoch both fail closed.
-spec fresh_enough_decision(stream_id(), consistent_result(), cache_result()) -> boolean().
fresh_enough_decision(StreamId, {ok, #{epoch := CommittedEpoch}}, CacheResult) ->
    case cache_at_committed_epoch(CacheResult, CommittedEpoch) of
        true ->
            true;
        false ->
            ?LOG_INFO(
                "GC: not deleting under stream ~ts: local manifest cache "
                "epoch ~p does not match the committed epoch ~p; a reset "
                "committed after the sweep snapshot and this node has not "
                "applied it. A later sweep reclaims a genuine orphan.",
                [StreamId, cache_epoch(CacheResult), CommittedEpoch]
            ),
            false
    end;
fresh_enough_decision(StreamId, {error, Reason}, _CacheResult) ->
    ?LOG_INFO(
        "GC: not deleting under stream ~ts: could not re-read the committed "
        "epoch with quorum (~p); failing closed",
        [StreamId, Reason]
    ),
    false.

%% Pure decision for fresh_enough_to_delete/2: the cache must hold a manifest at
%% exactly the committed epoch. A cache behind the committed epoch (the
%% reset-after-snapshot window), a cache with no recorded epoch (a legacy
%% put_manifest/2 entry), or no manifest at all all fail closed.
-spec cache_at_committed_epoch(
    {#manifest{}, osiris:epoch() | undefined} | undefined, osiris:epoch()
) -> boolean().
cache_at_committed_epoch({#manifest{}, CommittedEpoch}, CommittedEpoch) ->
    true;
cache_at_committed_epoch(_CacheResult, _CommittedEpoch) ->
    false.

-spec cache_epoch({#manifest{}, osiris:epoch() | undefined} | undefined) ->
    osiris:epoch() | undefined.
cache_epoch({#manifest{}, Epoch}) ->
    Epoch;
cache_epoch(undefined) ->
    undefined.

%% Re-validate an offset-based finding against the live manifest immediately
%% before deleting it. The sweep's safety argument is that first_offset only
%% moves forward, so a snapshot floor is never above the live floor. A
%% remote-tier-ahead reset breaks that: it lowers first_offset and re-tiers live
%% fragments (with fresh UIDs) at offsets below the snapshot floor. The reset
%% lowers the cached floor before it re-uploads, so re-reading the live floor here
%% and skipping anything now at or above it restores the assumption; a genuine
%% orphan skipped by a transient drop is reclaimed by a later sweep. Epoch-based
%% (manifest) findings need no re-check: epoch is genuinely monotonic.
%%
%% A group finding additionally re-validates the leading-group carve-out against
%% the live manifest. classify_group/3 protected the SNAPSHOT leading group, but
%% the carve-out (referenced_group_key) is captured once at build_lookup time. A
%% reset followed by forward retention can install a NEW leading group below the
%% live floor that the stale snapshot does not recognise, so an offset-only
%% re-check here would delete the live referenced leading group. Re-deriving the
%% carve-out from the live manifest closes that.
-spec still_dangling(finding()) -> boolean().
still_dangling(#{reason := stale_epoch}) ->
    true;
still_dangling(#{reason := no_anchor}) ->
    %% The anchor was confirmed absent (consistent read) in classify_page/3 and
    %% the absence is permanent, so there is nothing to re-validate.
    true;
still_dangling(#{reason := below_first_offset, stream_id := StreamId, key := Key}) ->
    case rabbitmq_stream_s3_manifest_replica:get_manifest(StreamId) of
        #manifest{first_offset = FirstOffset} = Manifest ->
            case parse_key(Key) of
                {data, _StreamId, Offset} ->
                    Offset < FirstOffset;
                {group, _StreamId, Offset} ->
                    Offset < FirstOffset andalso
                        not live_leading_group(StreamId, Key, Manifest);
                _ ->
                    false
            end;
        Unresolved when Unresolved =:= undefined; Unresolved =:= pending ->
            %% No live manifest to compare against (missing row, or a pending
            %% marker whose manifest has not resolved yet): do not delete (a
            %% later sweep reclaims a genuine orphan once a floor is known
            %% again).
            false
    end.

%% Whether the group object Key is protected by the LIVE manifest's leading-group
%% carve-out: it is the live referenced leading group, or the live manifest is in
%% conservative skip-groups mode (a leading kilo-/mega-group). Mirrors
%% classify_group/3, but re-derived from the live manifest rather than the sweep
%% snapshot. See leading_group_info/2.
-spec live_leading_group(stream_id(), rabbitmq_stream_s3:key(), #manifest{}) -> boolean().
live_leading_group(StreamId, Key, Manifest) ->
    case leading_group_info(StreamId, Manifest) of
        {_ReferencedGroupKey, true} -> true;
        {Key, _SkipGroups} -> true;
        {_Other, _SkipGroups} -> false
    end.

%% ------------------------------------------------------------------
%% Internal
%% ------------------------------------------------------------------

-doc """
Cross-stream sweep lookup.

Consumes the committed metadata snapshot taken by the single wildcard quorum read
in run/1 (db:list_consistent/0): every stream's entry is already the committed
authority, so no per-stream quorum read is done here. For each stream it reads the
local replica cache (a direct ETS read) and runs the shared per-stream decision
(stream_lookup_decision/5) with no empty-manifest allowance (an empty manifest has
nothing in the remote tier to sweep against), which gates the cached floor against
the committed epoch and fails closed when the cache lags.
""".
build_lookup(Streams, Config) ->
    maps:fold(
        fun(StreamId, Entry, Acc) ->
            Consistent = {ok, Entry},
            CacheResult = read_cache_after(Consistent, StreamId),
            case stream_lookup_decision(StreamId, Consistent, CacheResult, Config, false) of
                {ok, LookupEntry} -> Acc#{StreamId => LookupEntry};
                skip -> Acc
            end
        end,
        #{},
        Streams
    ).

%% Read the local replica cache only when the quorum read succeeded, mirroring
%% the original control flow (a quorum failure fails closed before any cache
%% read).
-spec read_cache_after(consistent_result(), stream_id()) -> cache_result().
read_cache_after({ok, _}, StreamId) ->
    rabbitmq_stream_s3_manifest_replica:get_manifest_and_epoch(StreamId);
read_cache_after({error, _}, _StreamId) ->
    undefined.

%% Pure per-stream lookup decision shared by the cross-stream and
%% single-stream sweeps. Given the committed-epoch read and the cache read, it
%% yields the lookup entry to sweep against or skips (fails closed).
%%
%% The floor comes from the local replica cache but the committed epoch comes from
%% the quorum read: on a node that has not applied a floor-lowering reset's sync,
%% the cache still holds the pre-reset high floor at the old epoch, and
%% still_dangling/1 would re-read that same stale floor. So the entry is built only
%% when the cached manifest's epoch equals the committed epoch (the CommittedEpoch
%% reused in the matching clause of stream_lookup_from_cache/4); otherwise the
%% cache lags the committed reset and the stream is skipped.
%%
%% AllowEmpty distinguishes the callers: the cross-stream sweep skips an empty
%% manifest, while the single-stream reset path keeps it (the reset installs an
%% empty manifest whose first_offset is the floor the orphans sit below).
-spec stream_lookup_decision(
    stream_id(), consistent_result(), cache_result(), config(), boolean()
) -> {ok, map()} | skip.
stream_lookup_decision(StreamId, {ok, #{epoch := CommittedEpoch}}, CacheResult, Config, AllowEmpty) ->
    case epoch_permits_sweep(CommittedEpoch, Config) of
        true ->
            stream_lookup_from_cache(StreamId, CommittedEpoch, CacheResult, AllowEmpty);
        false ->
            ?LOG_INFO(
                "GC for stream ~ts skipped: writer epoch ~p is behind the "
                "committed epoch ~p, this writer has been deposed",
                [StreamId, maps:get(writer_epoch, Config, undefined), CommittedEpoch]
            ),
            skip
    end;
stream_lookup_decision(StreamId, {error, Reason}, _CacheResult, _Config, _AllowEmpty) ->
    ?LOG_INFO(
        "GC for stream ~ts skipped: could not read committed "
        "metadata with quorum (~p); not sweeping",
        [StreamId, Reason]
    ),
    skip.

-spec stream_lookup_from_cache(stream_id(), osiris:epoch(), cache_result(), boolean()) ->
    {ok, map()} | skip.
stream_lookup_from_cache(_StreamId, _CommittedEpoch, {#manifest{entries = <<>>}, _}, false) ->
    %% Cross-stream sweep: empty manifest, nothing in the remote tier to compare
    %% against (matches the prior get_range/1 `empty` skip).
    skip;
stream_lookup_from_cache(
    StreamId, CommittedEpoch, {#manifest{first_offset = FirstOffset} = Manifest, CommittedEpoch}, _
) ->
    {ok, lookup_entry(StreamId, CommittedEpoch, FirstOffset, Manifest)};
stream_lookup_from_cache(StreamId, CommittedEpoch, {#manifest{}, CacheEpoch}, _AllowEmpty) ->
    ?LOG_INFO(
        "GC for stream ~ts skipped: local manifest cache epoch ~p "
        "does not match the committed epoch ~p; the cache lags the "
        "committed reset, so its floor may be stale-high",
        [StreamId, CacheEpoch, CommittedEpoch]
    ),
    skip;
stream_lookup_from_cache(_StreamId, _CommittedEpoch, undefined, _AllowEmpty) ->
    skip.

%% Build a per-stream lookup entry. Besides the epoch and first_offset used for
%% the offset/epoch heuristics, it records the leading group's object key (the
%% one group below first_offset that may still be referenced) and a skip-groups
%% flag for the conservative multi-level case. See leading_group_info/2.
-spec lookup_entry(stream_id(), osiris:epoch(), osiris:offset(), #manifest{}) -> map().
lookup_entry(StreamId, Epoch, FirstOffset, Manifest) ->
    {ReferencedGroupKey, SkipGroups} = leading_group_info(StreamId, Manifest),
    #{
        epoch => Epoch,
        first_offset => FirstOffset,
        referenced_group_key => ReferencedGroupKey,
        skip_groups => SkipGroups
    }.

%% Determine which group object, if any, must be protected from offset-based GC,
%% and whether group deletion must be skipped entirely for the stream.
%%
%% Only the first root entry can be a group that sits below first_offset while
%% still being referenced: retention advances first_offset into the leading
%% group on partial expiry, and every later root entry begins at or above that
%% group's coverage end. A leading level-1 group contains only fragments, so
%% protecting that single object is sufficient (its expired fragment children
%% are reclaimed correctly by the offset heuristic). A leading kilo-/mega-group
%% contains nested groups whose own leading child can also straddle the floor as
%% a separate object; identifying those requires descending the tree, so we
%% conservatively skip all group deletion for such streams. This is rare: a
%% kilo-group needs ~1024 groups (~1M fragments).
-spec leading_group_info(stream_id(), #manifest{}) ->
    {rabbitmq_stream_s3:key() | none, boolean()}.
leading_group_info(_StreamId, #manifest{entries = <<>>}) ->
    {none, false};
leading_group_info(StreamId, #manifest{entries = Entries}) ->
    ?ENTRY(Offset, _FirstTs, _LastTs, Kind, _Size, Uid, _Rest) = Entries,
    case Kind of
        ?MANIFEST_KIND_FRAGMENT ->
            {none, false};
        ?MANIFEST_KIND_GROUP ->
            Key = rabbitmq_stream_s3:group_key(
                StreamId, #group_ref{offset = Offset, kind = Kind, uid = Uid}
            ),
            {Key, false};
        _ ->
            %% Kilo-/mega-group: nested referenced leading groups cannot be
            %% cheaply identified. Conservatively skip group deletion.
            {none, true}
    end.

-doc """
Single-stream sweep lookup.

Reads the committed epoch with a strongly consistent (quorum-requiring) read.
the sweep deletes data objects below a floor taken from the local just-reset
manifest, which sits above the committed floor, and a deposed writer that read
stale local state would delete a successor's live fragments in that gap. The
consistent read makes a deposed minority writer, which cannot reach a quorum,
fail closed and skip; when the caller pins its own writer epoch,
`epoch_permits_sweep/2` additionally makes a deposed writer that can still
reach a quorum skip.

Runs the shared decision with AllowEmpty = true: a manifest reset installs an
empty manifest whose first_offset is the local floor the orphaned fragments sit
below, and reading first_offset from that record (rather than via get_range/1,
which reports `empty`) is precisely what lets this path reclaim them. On the
reset path the writer's own cache was just updated synchronously at this epoch,
so the cache-epoch gate is a no-op there; it fails closed for any caller
reading a node whose cache lags the committed reset.
""".
build_stream_lookup(StreamId, Config) ->
    Consistent = rabbitmq_stream_s3_db:get_consistent(StreamId),
    CacheResult = read_cache_after(Consistent, StreamId),
    case stream_lookup_decision(StreamId, Consistent, CacheResult, Config, true) of
        {ok, Entry} -> {ok, #{StreamId => Entry}};
        skip -> skip
    end.

%% On the reset path the caller pins its writer epoch, and the sweep is permitted
%% only when the committed epoch exactly equals it, confirming this writer is the
%% current committed one and has not been superseded. On the operator CLI path no
%% writer epoch is pinned, and the consistent read alone is the guard.
epoch_permits_sweep(CommittedEpoch, #{writer_epoch := WriterEpoch}) ->
    CommittedEpoch =:= WriterEpoch;
epoch_permits_sweep(_CommittedEpoch, _Config) ->
    true.

list_and_classify(_Prefix, done, _Lookup, _Fun, _AnchorAbsent, Acc) ->
    lists:reverse(Acc);
list_and_classify(Prefix, Continuation, Lookup, Fun, AnchorAbsent, Acc) ->
    case rabbitmq_stream_s3_api:list(Prefix, Continuation) of
        {ok, [], done} ->
            lists:reverse(Acc);
        {ok, Keys, NextContinuation} ->
            Orphans = classify_page(Keys, Lookup, AnchorAbsent),
            NewAcc = lists:foldl(Fun, Acc, Orphans),
            list_and_classify(Prefix, NextContinuation, Lookup, Fun, AnchorAbsent, NewAcc);
        {error, Reason} ->
            ?LOG_WARNING("GC: failed to list objects under ~ts: ~p", [Prefix, Reason]),
            lists:reverse(Acc)
    end.

%% AnchorAbsent is fun((stream_id()) -> boolean()): true only on a strongly
%% consistent read that confirms the anchor is absent (a deleted stream). A
%% no_anchor candidate is kept only when its stream's anchor is confirmed absent;
%% each distinct stream is checked once per page.
-spec classify_page([rabbitmq_stream_s3:key()], map(), fun((stream_id()) -> boolean())) ->
    [finding()].
classify_page(Keys, Lookup, AnchorAbsent) ->
    {Definite, Candidates} = lists:foldr(
        fun(Key, {Def, Cand}) ->
            case classify(Key, Lookup) of
                {ok, #{reason := no_anchor} = Finding} -> {Def, [Finding | Cand]};
                {ok, Finding} -> {[Finding | Def], Cand};
                skip -> {Def, Cand}
            end
        end,
        {[], []},
        Keys
    ),
    Streams = lists:usort([StreamId || #{stream_id := StreamId} <- Candidates]),
    AbsentStreams = [StreamId || StreamId <- Streams, AnchorAbsent(StreamId)],
    Confirmed = [
        Finding
     || #{stream_id := StreamId} = Finding <- Candidates, lists:member(StreamId, AbsentStreams)
    ],
    Definite ++ Confirmed.

%% Strongly-consistent anchor read for the no_anchor backstop. True only when the
%% read confirms the anchor is absent; a present anchor or a read that cannot reach
%% a quorum fails closed (false), so a live or unverifiable stream is never reaped.
-spec anchor_absent(stream_id()) -> boolean().
anchor_absent(StreamId) ->
    anchor_decision(StreamId, rabbitmq_stream_s3_db:anchor_exists_consistent(StreamId)).

%% Pure decision for the no_anchor backstop: the anchor is "absent" (the stream is
%% deleted, so its prefix is reapable) only on a consistent read that positively
%% reports it gone. A present anchor, or any read that cannot reach a quorum, fails
%% closed, so a live or unverifiable stream is never reaped.
-spec anchor_decision(stream_id(), {ok, boolean()} | {error, term()}) -> boolean().
anchor_decision(_StreamId, {ok, Exists}) ->
    not Exists;
anchor_decision(StreamId, {error, Reason}) ->
    ?LOG_INFO(
        "GC: not reaping the prefix of stream ~ts: could not read its anchor "
        "with quorum (~p); failing closed",
        [StreamId, Reason]
    ),
    false.

%% A stream that is in the lookup is classified against its floor/epoch. A
%% well-formed key whose stream is NOT in the lookup becomes a no_anchor
%% CANDIDATE, resolved against the anchor in classify_page/3. Only an unrecognised
%% key format is skipped outright.
-spec classify(rabbitmq_stream_s3:key(), map()) -> {ok, finding()} | skip.
classify(Key, Lookup) ->
    case parse_key(Key) of
        {data, StreamId, Offset} ->
            case Lookup of
                #{StreamId := #{first_offset := FirstOffset}} when Offset < FirstOffset ->
                    {ok, #{stream_id => StreamId, key => Key, reason => below_first_offset}};
                #{StreamId := _} ->
                    skip;
                _ ->
                    no_anchor_candidate(StreamId, Key)
            end;
        {group, StreamId, Offset} ->
            case Lookup of
                #{StreamId := #{first_offset := FirstOffset} = Info} when Offset < FirstOffset ->
                    classify_group(StreamId, Key, Info);
                #{StreamId := _} ->
                    skip;
                _ ->
                    no_anchor_candidate(StreamId, Key)
            end;
        {manifest, StreamId, Epoch} ->
            case Lookup of
                #{StreamId := #{epoch := CurrentEpoch}} when Epoch < CurrentEpoch ->
                    {ok, #{stream_id => StreamId, key => Key, reason => stale_epoch}};
                #{StreamId := _} ->
                    skip;
                _ ->
                    no_anchor_candidate(StreamId, Key)
            end;
        unknown ->
            %% Unrecognised key format: nothing safe to do with it.
            skip
    end.

no_anchor_candidate(StreamId, Key) ->
    {ok, #{stream_id => StreamId, key => Key, reason => no_anchor}}.

%% A group object below first_offset is an orphan and safe to delete unless it
%% is the manifest's referenced leading group, or the stream is in conservative
%% skip-groups mode (a leading kilo-/mega-group whose nested referenced groups
%% cannot be cheaply identified). See leading_group_info/2.
-spec classify_group(stream_id(), rabbitmq_stream_s3:key(), map()) ->
    {ok, finding()} | skip.
classify_group(_StreamId, _Key, #{skip_groups := true}) ->
    skip;
classify_group(_StreamId, Key, #{referenced_group_key := Key}) ->
    skip;
classify_group(StreamId, Key, _Info) ->
    {ok, #{stream_id => StreamId, key => Key, reason => below_first_offset}}.

%% Parse an S3 key into its components.
%%
%% Fragment keys: rabbitmq/stream/<StreamId>/data/<offset>.<uid>.fragment
%% Group keys:   rabbitmq/stream/<StreamId>/metadata/<offset>.<uid>.<kind>
%% Manifest keys: rabbitmq/stream/<StreamId>/metadata/root.<epoch>.<uid>.manifest
-spec parse_key(rabbitmq_stream_s3:key()) ->
    {data, stream_id(), osiris:offset()}
    | {group, stream_id(), osiris:offset()}
    | {manifest, stream_id(), osiris:epoch()}
    | unknown.
parse_key(<<"rabbitmq/stream/", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>) of
        [StreamId, <<"data/", Filename/binary>>] ->
            parse_data_filename(StreamId, Filename);
        [StreamId, <<"metadata/", Filename/binary>>] ->
            parse_metadata_filename(StreamId, Filename);
        _ ->
            unknown
    end;
parse_key(_) ->
    unknown.

parse_data_filename(StreamId, Filename) ->
    %% <offset>.<uid>.fragment
    case binary:split(Filename, <<".">>, [global]) of
        [OffsetBin, _Uid, <<"fragment">>] ->
            {data, StreamId, binary_to_integer(OffsetBin)};
        _ ->
            unknown
    end.

parse_metadata_filename(StreamId, Filename) ->
    case binary:split(Filename, <<".">>, [global]) of
        [<<"root">>, EpochBin, _Uid, <<"manifest">>] ->
            {manifest, StreamId, binary_to_integer(EpochBin)};
        [OffsetBin, _Uid, Kind] when
            Kind =:= <<"group">> orelse Kind =:= <<"kgroup">> orelse Kind =:= <<"mgroup">>
        ->
            %% Unlike fragments, a group object spans a multi-fragment range, so
            %% offset-below-first_offset alone does not prove it is dead (a
            %% partially-expired leading group is still referenced). classify/2
            %% checks it against the referenced leading group; see
            %% leading_group_info/2.
            {group, StreamId, binary_to_integer(OffsetBin)};
        _ ->
            unknown
    end.

log_finding(#{stream_id := StreamId, key := Key, reason := Reason}) ->
    ?LOG_INFO("GC: dangling object ~ts (stream=~ts, reason=~p)", [Key, StreamId, Reason]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_key_fragment_test() ->
    Key = <<"rabbitmq/stream/my_stream/data/00000000000000000100.deadbeef.fragment">>,
    ?assertEqual({data, <<"my_stream">>, 100}, parse_key(Key)).

parse_key_fragment_zero_test() ->
    Key = <<"rabbitmq/stream/s/data/00000000000000000000.00000001.fragment">>,
    ?assertEqual({data, <<"s">>, 0}, parse_key(Key)).

parse_key_group_test() ->
    Key = <<"rabbitmq/stream/s/metadata/00000000000000000500.aabbccdd.group">>,
    ?assertEqual({group, <<"s">>, 500}, parse_key(Key)).

parse_key_kilo_group_test() ->
    Key = <<"rabbitmq/stream/s/metadata/00000000000000001000.aabbccdd.kgroup">>,
    ?assertEqual({group, <<"s">>, 1000}, parse_key(Key)).

%% The operator CLI path supplies no writer epoch, so the consistent read is the
%% only guard and the sweep is permitted.
epoch_permits_sweep_no_writer_epoch_test() ->
    ?assert(epoch_permits_sweep(7, #{mode => delete})).

%% The reset path pins the writer epoch. The sweep proceeds only when the
%% committed epoch matches, i.e. this writer is still the committed authority.
epoch_permits_sweep_matching_epoch_test() ->
    ?assert(epoch_permits_sweep(7, #{mode => delete, writer_epoch => 7})).

%% A successor has committed a higher epoch: this writer is deposed, skip.
epoch_permits_sweep_deposed_writer_test() ->
    ?assertNot(epoch_permits_sweep(8, #{mode => delete, writer_epoch => 7})).

%% A writer that has not yet committed at its own epoch (committed epoch is the
%% predecessor's) also skips, since below its raised floor could be the
%% predecessor's committed data.
epoch_permits_sweep_uncommitted_writer_test() ->
    ?assertNot(epoch_permits_sweep(6, #{mode => delete, writer_epoch => 7})).

parse_key_mega_group_test() ->
    Key = <<"rabbitmq/stream/s/metadata/00000000000000005000.aabbccdd.mgroup">>,
    ?assertEqual({group, <<"s">>, 5000}, parse_key(Key)).

parse_key_manifest_test() ->
    Key = <<"rabbitmq/stream/my_stream/metadata/root.3.aabb0011.manifest">>,
    ?assertEqual({manifest, <<"my_stream">>, 3}, parse_key(Key)).

%% Helpers for the classify/leading-group tests.
group_entry(Offset, Kind, Uid) ->
    ?ENTRY(Offset, 0, 0, Kind, 0, Uid).

fragment_entry(Offset, Uid) ->
    ?ENTRY(Offset, 0, 0, ?MANIFEST_KIND_FRAGMENT, 0, Uid).

%% A fragment below first_offset is always dead and deletable (unchanged
%% behaviour).
classify_fragment_below_floor_deletes_test() ->
    Lookup = #{<<"s">> => #{first_offset => 100, epoch => 1}},
    Key = <<"rabbitmq/stream/s/data/00000000000000000050.0000002a.fragment">>,
    ?assertMatch({ok, #{reason := below_first_offset}}, classify(Key, Lookup)).

%% The leading group recorded as referenced must be protected even though its
%% offset is below first_offset (the partial-expiry case).
classify_referenced_leading_group_skipped_test() ->
    StreamId = <<"s">>,
    Uid = 16#aabbccdd,
    LeadingKey = rabbitmq_stream_s3:group_key(
        StreamId, #group_ref{offset = 50, kind = ?MANIFEST_KIND_GROUP, uid = Uid}
    ),
    Lookup = #{
        StreamId => #{
            first_offset => 100,
            epoch => 1,
            referenced_group_key => LeadingKey,
            skip_groups => false
        }
    },
    ?assertEqual(skip, classify(LeadingKey, Lookup)).

%% A different group below first_offset (not the referenced leading one) is an
%% orphan and deletable.
classify_other_below_floor_group_deletes_test() ->
    StreamId = <<"s">>,
    LeadingKey = rabbitmq_stream_s3:group_key(
        StreamId, #group_ref{offset = 50, kind = ?MANIFEST_KIND_GROUP, uid = 16#aabbccdd}
    ),
    Lookup = #{
        StreamId => #{
            first_offset => 100,
            epoch => 1,
            referenced_group_key => LeadingKey,
            skip_groups => false
        }
    },
    OtherKey = <<"rabbitmq/stream/s/metadata/00000000000000000010.00000099.group">>,
    ?assertMatch({ok, #{reason := below_first_offset}}, classify(OtherKey, Lookup)).

%% In conservative skip-groups mode (leading kilo-/mega-group) no group is
%% deleted, but fragments still are.
classify_skip_groups_mode_skips_all_groups_test() ->
    StreamId = <<"s">>,
    Lookup = #{
        StreamId => #{
            first_offset => 100,
            epoch => 1,
            referenced_group_key => none,
            skip_groups => true
        }
    },
    GroupKey = <<"rabbitmq/stream/s/metadata/00000000000000000010.00000099.group">>,
    ?assertEqual(skip, classify(GroupKey, Lookup)),
    FragKey = <<"rabbitmq/stream/s/data/00000000000000000010.00000099.fragment">>,
    ?assertMatch({ok, #{reason := below_first_offset}}, classify(FragKey, Lookup)).

%% leading_group_info: empty manifest and a leading fragment protect nothing.
leading_group_info_none_test() ->
    ?assertEqual({none, false}, leading_group_info(<<"s">>, #manifest{entries = <<>>})),
    Frag = fragment_entry(0, 1),
    ?assertEqual(
        {none, false}, leading_group_info(<<"s">>, #manifest{entries = Frag})
    ).

%% leading_group_info: a leading level-1 group is protected by key.
leading_group_info_group_test() ->
    StreamId = <<"s">>,
    Entries = group_entry(50, ?MANIFEST_KIND_GROUP, 16#aabbccdd),
    Expected = rabbitmq_stream_s3:group_key(
        StreamId, #group_ref{offset = 50, kind = ?MANIFEST_KIND_GROUP, uid = 16#aabbccdd}
    ),
    ?assertEqual({Expected, false}, leading_group_info(StreamId, #manifest{entries = Entries})).

%% leading_group_info: a leading kilo-group triggers conservative skip.
leading_group_info_kilo_group_skips_test() ->
    Entries = group_entry(50, ?MANIFEST_KIND_KILO_GROUP, 16#aabbccdd),
    ?assertEqual({none, true}, leading_group_info(<<"s">>, #manifest{entries = Entries})).

parse_key_manifest_epoch_zero_test() ->
    Key = <<"rabbitmq/stream/s/metadata/root.0.aabb0011.manifest">>,
    ?assertEqual({manifest, <<"s">>, 0}, parse_key(Key)).

parse_key_unknown_test() ->
    ?assertEqual(unknown, parse_key(<<"some/other/key">>)),
    ?assertEqual(unknown, parse_key(<<"rabbitmq/stream/s/other/file">>)).

%% Round-trip: keys generated by rabbitmq_stream_s3 are parseable.
parse_key_roundtrip_fragment_test() ->
    StreamId = <<"__vhost_stream_123">>,
    Key = rabbitmq_stream_s3:fragment_key(StreamId, 42, 16#deadbeef),
    ?assertEqual({data, StreamId, 42}, parse_key(Key)).

parse_key_roundtrip_manifest_test() ->
    StreamId = <<"__vhost_stream_123">>,
    Ref = #manifest_ref{epoch = 7, uid = 16#aabbccdd},
    Key = rabbitmq_stream_s3:manifest_key(StreamId, Ref),
    ?assertEqual({manifest, StreamId, 7}, parse_key(Key)).

parse_key_roundtrip_group_test() ->
    StreamId = <<"__vhost_stream_123">>,
    Ref = #group_ref{offset = 999, kind = ?MANIFEST_KIND_GROUP, uid = 16#11223344},
    Key = rabbitmq_stream_s3:group_key(StreamId, Ref),
    ?assertEqual({group, StreamId, 999}, parse_key(Key)).

classify_fragment_below_first_offset_test() ->
    Lookup = #{<<"s">> => #{epoch => 5, first_offset => 200}},
    Key = <<"rabbitmq/stream/s/data/00000000000000000100.deadbeef.fragment">>,
    ?assertEqual(
        {ok, #{stream_id => <<"s">>, key => Key, reason => below_first_offset}},
        classify(Key, Lookup)
    ).

classify_fragment_at_first_offset_test() ->
    %% At or above first_offset is NOT garbage.
    Lookup = #{<<"s">> => #{epoch => 5, first_offset => 100}},
    Key = <<"rabbitmq/stream/s/data/00000000000000000100.deadbeef.fragment">>,
    ?assertEqual(skip, classify(Key, Lookup)).

classify_manifest_stale_epoch_test() ->
    Lookup = #{<<"s">> => #{epoch => 5, first_offset => 0}},
    Key = <<"rabbitmq/stream/s/metadata/root.3.aabb0011.manifest">>,
    ?assertEqual(
        {ok, #{stream_id => <<"s">>, key => Key, reason => stale_epoch}},
        classify(Key, Lookup)
    ).

classify_manifest_current_epoch_test() ->
    %% Current epoch is NOT garbage.
    Lookup = #{<<"s">> => #{epoch => 3, first_offset => 0}},
    Key = <<"rabbitmq/stream/s/metadata/root.3.aabb0011.manifest">>,
    ?assertEqual(skip, classify(Key, Lookup)).

%% An unrecognised key format is skipped outright.
classify_unrecognised_key_skipped_test() ->
    ?assertEqual(skip, classify(<<"rabbitmq/stream/s/other/file">>, #{})),
    ?assertEqual(skip, classify(<<"some/other/key">>, #{})).

%% A well-formed key whose stream is not in the lookup becomes a no_anchor
%% candidate, resolved against the anchor in classify_page/3.
classify_unknown_stream_is_no_anchor_candidate_test() ->
    Lookup = #{},
    DataKey = <<"rabbitmq/stream/gone/data/00000000000000000100.deadbeef.fragment">>,
    ?assertEqual(
        {ok, #{stream_id => <<"gone">>, key => DataKey, reason => no_anchor}},
        classify(DataKey, Lookup)
    ),
    GroupKey = <<"rabbitmq/stream/gone/metadata/00000000000000000500.aabbccdd.group">>,
    ?assertMatch({ok, #{reason := no_anchor}}, classify(GroupKey, Lookup)),
    ManifestKey = <<"rabbitmq/stream/gone/metadata/root.3.aabb0011.manifest">>,
    ?assertMatch({ok, #{reason := no_anchor}}, classify(ManifestKey, Lookup)).

%% An in-lookup object at or above the floor is live and skipped, not a candidate.
classify_in_lookup_live_object_skipped_test() ->
    Lookup = #{<<"s">> => #{epoch => 5, first_offset => 100}},
    Key = <<"rabbitmq/stream/s/data/00000000000000000150.deadbeef.fragment">>,
    ?assertEqual(skip, classify(Key, Lookup)).

%% classify_page keeps a no_anchor candidate only when its stream's anchor is
%% confirmed absent, and checks each distinct stream exactly once.
classify_page_no_anchor_resolution_test() ->
    GoneKey1 = <<"rabbitmq/stream/gone/data/00000000000000000100.deadbeef.fragment">>,
    GoneKey2 = <<"rabbitmq/stream/gone/data/00000000000000000200.deadbeef.fragment">>,
    LiveKey = <<"rabbitmq/stream/live/data/00000000000000000100.deadbeef.fragment">>,
    Self = self(),
    AnchorAbsent = fun(StreamId) ->
        Self ! {checked, StreamId},
        StreamId =:= <<"gone">>
    end,
    Findings = classify_page([GoneKey1, GoneKey2, LiveKey], #{}, AnchorAbsent),
    Keys = lists:sort([K || #{key := K} <- Findings]),
    %% Both objects of the deleted stream are reaped; the live stream's object is kept.
    ?assertEqual(lists:sort([GoneKey1, GoneKey2]), Keys),
    %% Each distinct stream is checked once.
    ?assertEqual(lists:sort([<<"gone">>, <<"live">>]), lists:sort(drain_checks([]))).

drain_checks(Acc) ->
    receive
        {checked, StreamId} -> drain_checks([StreamId | Acc])
    after 0 -> Acc
    end.

%% A candidate deletion is re-validated against the live first_offset just before
%% deleting. A remote-tier-ahead reset lowers first_offset and re-tiers live
%% fragments below a sweep's snapshot floor; the re-validation must skip them
%% while still reclaiming genuine orphans below the live floor. This is the
%% durability hole that an offset-only snapshot check would open.
still_dangling_respects_live_first_offset_test_() ->
    {setup,
        fun() ->
            {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
            unlink(Pid),
            Pid
        end,
        fun(Pid) -> gen_server:stop(Pid) end, fun(_) ->
            StreamId = <<"gc-revalidate-stream">>,
            %% Live, post-reset floor: first_offset lowered to 800.
            ok = rabbitmq_stream_s3_manifest_replica:put_manifest(
                StreamId, #manifest{first_offset = 800, next_offset = 800}
            ),
            Orphan = #{
                stream_id => StreamId,
                key =>
                    <<"rabbitmq/stream/gc-revalidate-stream/data/00000000000000000700.0000002a.fragment">>,
                reason => below_first_offset
            },
            ReTiered = #{
                stream_id => StreamId,
                key =>
                    <<"rabbitmq/stream/gc-revalidate-stream/data/00000000000000000850.0000002b.fragment">>,
                reason => below_first_offset
            },
            StaleManifest = #{
                stream_id => StreamId,
                key =>
                    <<"rabbitmq/stream/gc-revalidate-stream/metadata/root.1.0000aabb.manifest">>,
                reason => stale_epoch
            },
            [
                %% 700 < live floor 800: a genuine orphan, still deletable.
                ?_assert(still_dangling(Orphan)),
                %% 850 >= live floor 800: a reset re-tiered into this range. A
                %% sweep that snapshotted the old floor (e.g. 1000) would have
                %% marked it; the live floor protects it.
                ?_assertNot(still_dangling(ReTiered)),
                %% Epoch-based findings are not re-checked (epoch is monotonic).
                ?_assert(still_dangling(StaleManifest))
            ]
        end}.

%% A group finding re-validates the leading-group carve-out against the LIVE
%% manifest, not just the live floor. A reset followed by forward retention can
%% install a new leading group below the live floor; the sweep's snapshot carve-out
%% predates it, so an offset-only re-check would delete the live referenced
%% leading group. The live re-derivation must protect it while still reclaiming a
%% genuine orphan group at the same range (distinguished by uid).
still_dangling_group_respects_live_leading_group_test_() ->
    {setup,
        fun() ->
            {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
            unlink(Pid),
            Pid
        end,
        fun(Pid) -> gen_server:stop(Pid) end, fun(_) ->
            StreamId = <<"gc-leading-revalidate-stream">>,
            LeadingUid = 16#0000002b,
            %% Live manifest: floor 870, with a leading group at 850 (straddling
            %% the floor after a reset + forward retention) still referenced.
            ok = rabbitmq_stream_s3_manifest_replica:put_manifest(
                StreamId,
                #manifest{
                    first_offset = 870,
                    next_offset = 2000,
                    entries = group_entry(850, ?MANIFEST_KIND_GROUP, LeadingUid)
                }
            ),
            LiveLeadingKey = rabbitmq_stream_s3:group_key(
                StreamId, #group_ref{offset = 850, kind = ?MANIFEST_KIND_GROUP, uid = LeadingUid}
            ),
            LiveLeading = #{
                stream_id => StreamId, key => LiveLeadingKey, reason => below_first_offset
            },
            %% Same offset, different uid: a stale group object, not the live
            %% leading one, so a genuine orphan.
            StaleAtLeadingOffset = #{
                stream_id => StreamId,
                key =>
                    <<"rabbitmq/stream/gc-leading-revalidate-stream/metadata/00000000000000000850.0000002a.group">>,
                reason => below_first_offset
            },
            %% A deep orphan group well below the floor.
            DeepOrphanGroup = #{
                stream_id => StreamId,
                key =>
                    <<"rabbitmq/stream/gc-leading-revalidate-stream/metadata/00000000000000000500.00000099.group">>,
                reason => below_first_offset
            },
            [
                %% The live leading group is below the floor but referenced: keep it.
                ?_assertNot(still_dangling(LiveLeading)),
                %% A stale object at the same offset (different uid) is a real
                %% orphan: delete it.
                ?_assert(still_dangling(StaleAtLeadingOffset)),
                %% A deep orphan group below the floor: delete it.
                ?_assert(still_dangling(DeepOrphanGroup))
            ]
        end}.

%% With no live manifest (cache miss), a candidate is not deleted: there is no
%% floor to confirm it is still an orphan.
still_dangling_without_manifest_keeps_object_test_() ->
    {setup,
        fun() ->
            {ok, Pid} = rabbitmq_stream_s3_manifest_replica:start_link(),
            unlink(Pid),
            Pid
        end,
        fun(Pid) -> gen_server:stop(Pid) end, fun(_) ->
            Finding = #{
                stream_id => <<"gc-no-manifest-stream">>,
                key =>
                    <<"rabbitmq/stream/gc-no-manifest-stream/data/00000000000000000100.0000002a.fragment">>,
                reason => below_first_offset
            },
            [?_assertNot(still_dangling(Finding))]
        end}.

%% fresh_enough_to_delete/2 (see the moduledoc, guard 3): an offset-based finding
%% is deletable only when the local manifest cache is at the committed epoch. A
%% cache behind the committed epoch is the reset-after-snapshot window and must
%% fail closed.
cache_at_committed_epoch_matches_test() ->
    ?assert(cache_at_committed_epoch({#manifest{first_offset = 100}, 7}, 7)).

%% A cache behind the committed epoch (a reset committed after the sweep snapshot,
%% sync not yet applied) fails closed.
cache_at_committed_epoch_stale_test() ->
    ?assertNot(cache_at_committed_epoch({#manifest{first_offset = 100}, 6}, 7)).

%% A legacy put_manifest/2 entry has no recorded epoch: fail closed.
cache_at_committed_epoch_undefined_epoch_test() ->
    ?assertNot(cache_at_committed_epoch({#manifest{first_offset = 100}, undefined}, 7)).

%% No manifest at all: fail closed.
cache_at_committed_epoch_no_manifest_test() ->
    ?assertNot(cache_at_committed_epoch(undefined, 7)).

%% Stale-manifest (epoch-based) findings are not floor-based: a higher committed
%% epoch only confirms staleness, so the freshness re-check is skipped and they
%% remain deletable.
fresh_enough_to_delete_stale_epoch_test() ->
    ?assert(fresh_enough_to_delete(stale_epoch, <<"any-stream">>)).

-endif.
