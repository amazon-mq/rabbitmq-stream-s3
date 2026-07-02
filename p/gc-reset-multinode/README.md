# GC × reset durability seam, across nodes

The cross-node companion to [`../gc-reset`](../gc-reset). That model collapses every node into one synchronous `manifest_replica`, so GC and the writer always read the same live floor: it cannot express a sweep that reads a different, lagging node's cache. This model splits them apart and shows that the shipped `still_dangling/1` guard is insufficient when the floor it re-reads comes from a stale replica.

## The seam

`rabbitmq_stream_s3_gc:build_lookup` reads two things from two different places:

- the committed epoch, via a strongly-consistent quorum read (`rabbitmq_stream_s3_db:get_consistent`)
- `first_offset` (the floor), via a direct read of the local per-node replica ETS cache (`rabbitmq_stream_s3_manifest_replica:get_manifest`)

Nothing ties the cached floor's freshness to the committed epoch. The replica cache is updated from the writer by a fire-and-forget sync that can be dropped, delayed, or reordered (`is_stale_sync`). So a node that has not yet applied a reset's sync still holds the pre-reset, high floor.

An operator CLI delete sweep (`stream_s3_gc --mode delete`, the `run/1` path) can target such a lagging node:

1. A remote-tier-ahead reset commits: epoch `1` to `2`, floor `1000` to `850`, and a live fragment is re-tiered as `(850, uid 2)`
2. The reset's sync to the target node is dropped, so its cache still says floor `1000`, epoch `1`
3. The sweep reads committed epoch `2` (quorum) but floor `1000` (stale cache), classifies `(850, uid 2)` as `below_first_offset`, and, because `still_dangling/1` re-reads the same stale cache, confirms and deletes a live object

The reset-triggered GC path (`gc_stream_async`) is safe because its `put_manifest` is synchronous on the writer node; the exposure is strictly the cross-node operator-CLI path.

## The guard this model proves

Skip the stream unless the cache's epoch equals the committed epoch. A cache behind the committed epoch has a floor that predates the committed reset, so the sweep must fail closed rather than trust it. This requires exposing the epoch the cached manifest was synced at (the replica already tracks it per stream as `{seq, epoch, writer_node}`; `get_manifest/1` currently drops it).

Crucially, `still_dangling/1` (the offset guard) stays on in every test: the point is that it does not help here, so a second, epoch-based guard is needed.

### Machines (`PSrc/`)

- `Writer`: commits the reset (bumps the committed epoch, lowers the authoritative floor, re-tiers the live object) and announces the authoritative manifest state. It does not update the target cache; that propagation is the modeled uncertainty
- `ManifestReplica`: one lagging per-node cache. Learns of the reset only via `eMRSync` (driver-controlled: dropped, delivered, or raced), applied only if the incoming epoch is not behind (`is_stale_sync`). Never announces to the monitors, since it is a possibly-stale view, not the source of truth
- `KhepriDB`: the committed epoch (quorum truth), bumped by the reset
- `S3Store`: the object store; the committed reset has already put `(850, 2)`
- `GC`: the CLI sweep, with two independent guards: `stillDanglingEnabled` (the shipped offset re-read, always on) and `epochGuardEnabled` (the fix)

### Monitors (`PSpec/`)

Same as `gc-reset`: `NoDanglingReference` (INV#2, headline), `NoLostAckedData` (INV#1), `MonotonicFrontier` (INV#5), driven by the authoritative writer and driver.

## Tests

| Test case | Expectation |
| --- | --- |
| `tcMultiNodeStaleUnguarded` | fails: epoch guard off, sync dropped; `still_dangling/1` is on but re-reads the stale cache, so `INV#2 violated: GC deleted live object (offset=850, uid=2)`. The gate |
| `tcMultiNodeStaleGuarded` | holds: epoch guard on, sync dropped; the cache epoch is behind the committed epoch, so the sweep fails closed and deletes nothing |
| `tcMultiNodeSyncedGuarded` | holds: epoch guard on, sync delivered; the caught-up cache lets the sweep proceed, preserve the live `(850, 2)`, and reclaim the deep orphan `(500, 0)` (anti-vacuity) |
| `tcMultiNodeExplore` | holds: epoch guard on, sync raced against the sweep across all interleavings |

`tcMultiNodeStaleUnguarded` is expected to fail: that failing counterexample is the proof the model reproduces the cross-node bug, so the guarded result is meaningful. The CI job inverts it, so a passing buggy run is a CI failure, meaning the model rotted.

```bash
p compile
p check -tc tcMultiNodeStaleGuarded   -i 2000   # 0 bugs
p check -tc tcMultiNodeSyncedGuarded   -i 2000   # 0 bugs (anti-vacuity)
p check -tc tcMultiNodeStaleUnguarded  -i 2000   # 1 bug: INV#2 on (850, uid 2)
p check -tc tcMultiNodeExplore         -i 5000   # 0 bugs
p check -tc tcMultiNodeExplore --sch-pct 10 -i 5000  # 0 bugs (PCT)
```

## Corresponding code fix

The model's `epochGuardEnabled` guard is implemented in `rabbitmq_stream_s3_gc:build_lookup` / `build_stream_lookup`: after the quorum `get_consistent` read, they read the cached manifest with `rabbitmq_stream_s3_manifest_replica:get_manifest_and_epoch/1` and sweep the stream only when the cached epoch equals the committed epoch, skipping otherwise. Skipping is always safe (GC is idempotent and operator-driven), so failing closed costs only a deferred sweep.

The cached epoch is recorded in the replica's ETS row, stamped on every write, including the writer's own local `put_manifest`, which previously dropped it, so the check is correct on the writer node as well as on lagging replicas.
