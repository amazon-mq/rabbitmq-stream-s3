# Manifest-replica lifecycle: registration and cleanup-on-reader-exit

A model of the per-node manifest replica (`rabbitmq_stream_s3_manifest_replica`) and the cleanup-on-reader-exit landed in `cc50092`. It shares the multi-node stale-cache substrate of [`../gc-reset-multinode`](../gc-reset-multinode) (fire-and-forget, droppable, reorderable syncs gated by `is_stale_sync`) but targets the lifecycle of the replica's per-node state, not a GC sweep that reads it.

It complements the [`tla/manifest-replication`](../../tla/manifest-replication) TLA+ model without overlapping it. TLA+ verifies data-prefix consistency of sequenced edits under loss and resync (`SeqBounded`, `ReplicaConsistent`) over full edit histories; this model verifies resource-lifecycle correctness, that every cache row is owned by a live monitor and no sync races context registration, over a manifest abstracted to a floor/epoch/seq. The strand and startup-race gates below are invisible to TLA+ (it has no monitor/context coupling), and prefix-consistency is inexpressible here (there are no edit histories).

## The seam

The replica holds three per-stream maps in lockstep plus a reverse monitor index:

- `contexts`, the osiris log context, registered by the acceptor hook when a member starts; the member pid is monitored
- `seqs`, the last-applied `{seq, epoch, writer_node}` for gap detection
- the ETS cache row `{manifest, epoch}` that GC and consumers read directly
- `monitors`, an `mref -> stream` index so a member `DOWN` finds the stream to release

osiris has no terminate or delete hook, so a member `DOWN` (via `monitor/2`) is the only thing that reclaims this state: it runs `release_stream/2` to drop all four. Before `cc50092` there was no cleanup and the metadata accreted forever. The writer node's own row (written by `put_manifest/3`, with no member to monitor) is released explicitly by `forget/1` from the reader's `terminate/2`.

The model abstracts a member to its monitor reference: the replica's only handle on a member is the monitor it holds. The driver starts and kills members; a kill is `eMemberDown` with that mref (the modeled `'DOWN'`).

There are two write-path events into this state, both fire-and-forget casts from the writer: `eMRSync` (`sync/5`, a full-manifest reset checked against `is_stale_sync`) and `eMREdit` (`apply_edits/5`, a sequenced edit that requires `sn == last_seq + 1` against a recorded `seqs` entry, else a gap dropped without touching cache/seqs; a stream with no recorded entry yet accepts, the same "unrecorded is never rejected" convention `eMRSync` uses, since resolving a first-arrival gap is a data-prefix concern the TLA+ companion model owns, not this one). They are deliberately distinct events rather than one, because their acceptance checks differ, but the resource-lifecycle question this model asks - is the write gated on a live context, so it can never write a row no monitor will ever reclaim - is identical for both, and `syncRequiresContextEnabled` (A2) gates both with one check.

## The gap: a contextless sync strands the cache

With every shipped guard on, cleanup is not airtight. A sync arriving after a member `DOWN`, when cleanup has already released the stream, falls through `maybe_apply_sync` with `Recorded == undefined`, so `is_stale_sync` returns `false` and `write_manifest` re-inserts the ETS row and `seqs` with no context and no monitor: nothing will ever `'DOWN'` to reclaim it. It lingers rather than being a narrow window because the writer keeps broadcasting to a departed node (decoupling 2 below). It re-introduces exactly the accretion `cc50092` killed. `tcSyncAfterExitStrands` reproduces it (NOLEAK).

The same gap exists on the `apply_edits` path: before the fix below, only `maybe_apply_sync` checked for a live context, so a straggler edit delivered after the `DOWN` re-inserted the row exactly as a straggler sync did. `tcEditAfterExitStrands` reproduces the identical `NOLEAK` counterexample by driving `eMREdit` instead of `eMRSync` after the member exit; `tcEditAfterExitFixed` shows the same `syncRequiresContext` guard closes it.

## The fix: A2 + A1', and why the guard alone is unsafe

Dropping a contextless sync (A2, `syncRequiresContext`) closes the strand, but three decoupled lifecycles make it unsafe on its own:

1. Attach ordering. The acceptor hook (`hooks.erl:83-95`) registers with the writer first (`{register_acceptor}`, which makes the writer reply with a sync, `replica_reader.erl:435,:771`) and registers the local context second, so a sync can beat the context.
2. Writer targeting monitors the remote node's `manifest_replica` singleton (`replica_reader.erl:435,:542-549`), not the osiris member, so after a member moves off the writer keeps syncing that node.
3. Reconcile (`hooks.erl:234-256`, the 60s tick) re-registers a context only for a node that still hosts a member, and the writer's `register_acceptor` and `register_replica` are idempotent, so neither side self-heals a dropped sync.

So with A2 on, a startup sync that beats registration is dropped and never re-sent, leaving the cache empty forever, a `ReplicaConverges` liveness violation even though nothing leaks. The guard alone is a hollow fix.

There are two writer-side sync triggers, and they need different treatment. The acceptor reply (the node's own attach) is closable by reordering the attach to register the context first, the A1 fix. The reconcile path (`replica_reader.erl:467-487`, member-visible-driven, independent of `register_acceptor`) is not: A1 cannot reach it, because the trigger fires on the writer side, unordered with the node's registration.

A1' (resync-on-register) covers both: on registering a context the replica requests a resync (the existing `request_resync/2`; the writer re-sends its persisted floor), so a premature drop from either trigger is recovered. The model proves A1' is load-bearing for the reconcile trigger that A1 cannot reach: `tcReconcileRaceNoResync` starves without it and `tcReconcileRaceResync` converges with it. Because A1' fires on registration regardless of which premature sync was dropped, the same mechanism recovers the acceptor-reply drop, so A1 (attach reorder) is not required and the minimal sound fix is A2 + A1'. The acceptor starve itself is shown by `tcStartupRaceWriterFirst`, and A1 fixing it by `tcStartupRaceContextFirst`; the reconcile pair is what shows why A1 alone is insufficient and A1' is the general fix. A possible A3 (the writer prunes its target on member departure rather than singleton death, removing the orphan-feeding at the source) is noted but not modeled.

### Machines (`PSrc/`)

- `ManifestReplica`, the system under test: the three maps and reverse index, with a toggle per guard so each gate can remove one (`cleanupEnabled`, `staleSyncGuardEnabled`, `repointEnabled`, `syncRequiresContextEnabled`, `resyncOnRegisterEnabled`), and two write-path events sharing those gates, `eMRSync` (`sync/5`) and `eMREdit` (`apply_edits/5`)
- `Writer`, the writer-side broadcaster (`rabbitmq_stream_s3_replica_reader`): the authoritative source of committed floors and the emitter of fire-and-forget syncs and edits, answering two triggers from its persisted floor, `eReconcile` (member-visible-driven, independent of `register_acceptor`) and `eResync` (a replica's `request_resync`)
- `AttachNode` and `ContextRegistrar`, which drive the attach order (the `contextFirst` axis) and the concurrent reconcile race, so a startup sync can be scheduled before or after context registration

### Monitors (`PSpec/`)

- `ReplicaStateMatchesReaders` (safety): the streams the replica holds state for must equal the streams with a live reader. The held-implies-live direction (NOLEAK) proves the `cc50092` cleanup and surfaces the strand; the live-implies-held direction (RETAIN) proves the re-registration monitor repoint.
- `NoStaleFloorServed` (safety): the cache may only hold a committed floor, and its `(epoch, seq)` never regresses, so a replica never serves a floor a newer write already superseded. This is the read-side analogue of the `../gc-reset-multinode` stale-floor finding (it proves `is_stale_sync/3`).
- `ReplicaConverges` (liveness, `hot`): a replica fed syncs must eventually reach the latest committed floor; the empty-cache strand trips it.

## Tests (`PTst/`) and the validation gate

| Test case | Expectation |
| --- | --- |
| `tcCleanupGuarded` | holds: reader registers, syncs, exits; cleanup releases everything |
| `tcCleanupUnguarded` | fails: cleanup off, so the exited reader's state is stranded: `NOLEAK violated: replica holds per-node state for stream 0 with no live reader` |
| `tcStaleSyncGuarded` | holds: a delayed lower-epoch sync is dropped; the cache stays at the reset floor |
| `tcStaleSyncUnguarded` | fails: guard off; `STALEFLOOR violated: cache for stream 0 regressed to (epoch=1, sn=1) below already-applied (epoch=2, sn=2)` |
| `tcReregisterGuarded` | holds: a member restart repoints the monitor; the old `DOWN` is ignored, the new context survives |
| `tcReregisterUnguarded` | fails: repoint off; `RETAIN violated: replica dropped per-node state for stream 0 that still has a live reader (a superseded member DOWN evicted the live context)` |
| `tcConvergenceGuarded` | holds: every sync delivered; the replica reaches the latest committed floor |
| `tcConvergenceStuck` | fails: syncs dropped forever; `ReplicaConverges detected liveness bug in hot state 'Lagging' at the end of program execution` |
| `tcForgetReleasesWriterRow` | holds: the writer-node `put_manifest` row is released by `forget/1` |
| `tcSyncAfterExitStrands` | fails: all shipped guards on, and a sync after the `DOWN` re-strands the row: the same `NOLEAK` counterexample, the gap |
| `tcSyncAfterExitFixed` | holds: the `syncRequiresContext` guard (A2) ignores the post-exit sync |
| `tcEditAfterExitStrands` | fails: the same setup, but the write-path event is `eMREdit` (`apply_edits`) instead of `eMRSync`; the identical `NOLEAK` counterexample, proving `apply_edits` needed the same gate |
| `tcEditAfterExitFixed` | holds: the same `syncRequiresContext` guard (A2) ignores the post-exit edit |
| `tcStartupRaceWriterFirst` | fails: A2 on plus the shipped writer-first attach; a startup sync beats context registration, is dropped, and is never re-sent: `ReplicaConverges detected liveness bug in hot state 'Lagging' at the end of program execution` (the cache stays empty and NOLEAK stays green, so the guard alone is unsafe) |
| `tcStartupRaceContextFirst` | holds: A1 attach-ordering (context before writer) plus A2; the startup sync always lands and the post-`DOWN` straggler is dropped (convergence and NOLEAK) |
| `tcReconcileRaceNoResync` | fails: A1-consistent registration plus A2 with A1' off; the writer-driven reconcile sync races ahead of the context, is dropped, and is never re-sent: `ReplicaConverges detected liveness bug in hot state 'Lagging' at the end of program execution` (A1 cannot cover the writer-side trigger) |
| `tcReconcileRaceResync` | holds: A2 plus A1'; registering the context requests a resync that refills the cache (convergence, NOLEAK, and stale-floor) |
| `tcExplore` | holds: register, sync, restart, and exit raced across two streams with every guard and the fix on |

The four unguarded gates, the strand test, and the two startup/reconcile must-fails (`tcStartupRaceWriterFirst`, `tcReconcileRaceNoResync`) are expected to fail: those counterexamples are the proof the monitors have teeth, so the guarded results are meaningful. CI inverts them, so a passing buggy run means the model rotted.

```bash
p compile
p check -tc tcCleanupGuarded        -i 2000   # 0 bugs
p check -tc tcStaleSyncGuarded      -i 2000   # 0 bugs
p check -tc tcReregisterGuarded     -i 2000   # 0 bugs
p check -tc tcConvergenceGuarded    -i 2000   # 0 bugs
p check -tc tcForgetReleasesWriterRow -i 2000 # 0 bugs
p check -tc tcSyncAfterExitFixed    -i 2000   # 0 bugs (A2 closes the strand)
p check -tc tcEditAfterExitFixed    -i 2000   # 0 bugs (A2 closes the edit strand too)
p check -tc tcStartupRaceContextFirst -i 2000 # 0 bugs (A1 + A2)
p check -tc tcReconcileRaceResync   -i 2000   # 0 bugs (A2 + A1' covers reconcile)
p check -tc tcExplore               -i 5000   # 0 bugs
p check -tc tcExplore --sch-pct 10  -i 5000   # 0 bugs (PCT)
p check -tc tcCleanupUnguarded      -i 2000   # 1 bug: NOLEAK
p check -tc tcStaleSyncUnguarded    -i 2000   # 1 bug: STALEFLOOR
p check -tc tcReregisterUnguarded   -i 2000   # 1 bug: RETAIN
p check -tc tcSyncAfterExitStrands  -i 2000   # 1 bug: NOLEAK (the gap)
p check -tc tcEditAfterExitStrands  -i 2000   # 1 bug: NOLEAK (the same gap, via apply_edits)
p check -tc tcConvergenceStuck      -i 2000   # 1 bug: liveness (hot Lagging)
p check -tc tcStartupRaceWriterFirst -i 2000  # 1 bug: liveness (guard alone unsafe)
p check -tc tcReconcileRaceNoResync -i 2000   # 1 bug: liveness (A1 misses reconcile)
```

## Corresponding code fix

A2 and A1' are implemented in `rabbitmq_stream_s3_manifest_replica`. `maybe_apply_sync/8` drops a sync for a stream with no registered context (counted as `syncs_dropped_no_context`) and records the writer node the sync carried in a new `pending_resync` map; `register_replica_context` consumes that entry to `request_resync/2` from the writer once a monitored context can own the row. Sourcing the writer node from the dropped sync, which carries it, covers both the acceptor-reply and reconcile-path syncs without threading a leader lookup into registration. A1 (attach reorder) and A3 are not implemented: A1' is trigger-agnostic (it fires on registration regardless of which premature sync was dropped), so it recovers the acceptor-reply drop that A1 would reorder around as well as the reconcile drop A1 cannot reach, and A3 is unnecessary once the receiver refuses the orphan row. The suite's tests that drove `sync/4` without a context now register one first; `gmake ct-manifest_replica`, `gmake xref`, and `gmake dialyze` are green.

The same A2 gate has since landed on `apply_edits`'s two `handle_call`/`handle_cast` clauses, which previously applied a sequenced edit regardless of whether a context was registered. Both now check `maps:is_key(StreamId, Ctxs)` first and, if absent, drop through a shared `drop_no_context/6` helper (counted as `edits_dropped_no_context`) that records the writer node in the same `pending_resync` map `maybe_apply_sync/8` uses, so `apply_edits` and `sync` recover identically once a context registers.
