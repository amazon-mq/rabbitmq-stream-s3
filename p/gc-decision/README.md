# Integrated GC reap-decision

A model of the whole rabbitmq_stream_s3_gc decision in one state space. The other GC models each prove a single guard in isolation and collapse the remaining axes into a constant: gc-reset proves still_dangling re-reads the live floor, gc-reset-multinode proves build_lookup gates on the cache epoch, gc-leading-group and gc-reset-leading-group prove the leading-group carve-out. This model composes all three classify reasons and all the guards together so they can interact, with two stores that lag independently and a sweep a reset can interleave into. The point is the interactions, not re-proving each guard.

## The decision, as the code runs it

The sweep is three phases, each reading a different store, mirrored from the module:

1. build_lookup reads the committed epoch with a quorum read (db:get_consistent) and reads the floor and the leading-group carve-out from the local manifest_replica cache (get_manifest_and_epoch). It proceeds only when the cache epoch equals the committed epoch, and snapshots the committed epoch together with the cached floor and carve-out.
2. classify flags an object against that snapshot: a data fragment below the snapshot floor, a group below the floor that is neither the referenced leading group nor in skip-groups mode, or a manifest below the committed epoch.
3. still_dangling re-validates each candidate immediately before deleting it. A stale-epoch candidate is deleted with no re-check because the epoch is monotonic. A below-floor candidate re-reads the live cache: it compares the offset to the live floor (get_manifest) and re-derives the leading-group carve-out from the live manifest.

The committed manifest truth is carried only by the authoritative referenced and unreferenced announcements, exactly as in gc-reset-multinode. The cache is the single materialized copy the sweep reads, and it may lag. A reap is a bug only when it deletes a currently referenced object.

## The four guards

Three are shipped. Each gate turns one off and reproduces a live deletion, so the model proves it load-bearing in the composed decision rather than in isolation.

| Guard | Where | If removed | Gate |
| --- | --- | --- | --- |
| A epoch gate | build_lookup: skip unless cache epoch equals committed epoch | a node that has not applied a reset sweeps against a stale-high floor and the live re-read confirms it | `tcGcCrossNodeStaleUnguarded` |
| B live-floor re-read | still_dangling: compare to the live floor, not the snapshot floor | a reset that lowers the floor between snapshot and execute lets the stale-high snapshot reap a re-tiered live fragment | `tcGcNoReread` |
| C live carve-out | still_dangling: re-derive the leading group from the live manifest | a reset plus forward retention installs a new referenced leading group below the live floor that the stale snapshot carve-out deletes | `tcGcNoLeadingReread` |

The fourth guard is proposed, not shipped, and is the result the model exists to surface.

## The gap the integration surfaces

With all three shipped guards on, the model still reaches a live deletion when a reset commits after the sweep has snapshotted. build_lookup samples the committed epoch once, at the start. still_dangling re-reads only the floor (get_manifest drops the epoch), so it never notices that the committed epoch has moved. On a node whose cache has not applied the reset sync, the sweep snapshots epoch N with a matching cache and an honest floor, a reset to N+1 then commits and re-tiers a live fragment below the old floor, and the live re-read returns the same stale-high floor. The epoch gate has already passed and the floor re-read defends nothing. The sweep deletes the re-tiered live fragment.

gc-reset-multinode closed the mirror image of this, where the reset commits before the snapshot so get_consistent returns the new epoch and the gate skips. Its single scenario fixed the reset-before-snapshot ordering and did not exercise reset-after-snapshot. The dangerous path is the operator CLI full sweep (run/1 to build_lookup), which pins no writer epoch and relies on the consistent read and the cache-epoch gate, both sampled once.

The proposed guard D re-validates freshness at the point of deletion: still_dangling re-reads the committed epoch and the cache epoch and fails closed when they differ. This requires still_dangling to use the epoch-aware read (get_manifest_and_epoch) plus a fresh get_consistent. Where that re-check lands in real code (per candidate, per list page, or once per stream before execute) is an implementation choice; the model proves only that the logical re-validation closes the window. With guard D on the model holds across the reset timing and sync delivery; with it off the shipped behavior reaps the live fragment.

## Tests

| Test case | Expectation |
| --- | --- |
| `tcGcBasicReclaim` | holds: data and stale-manifest orphans reclaimed, live fragment and current manifest preserved |
| `tcGcCrossNodeStaleGuarded` | holds: guard A fails closed when the cache epoch lags |
| `tcGcCrossNodeSynced` | holds: caught-up cache reclaims the deep orphan and preserves the live re-tier |
| `tcGcResetRereadHolds` | holds: guard B preserves the re-tier and still reclaims a genuine orphan |
| `tcGcLeadingGroupHolds` | holds: guard C preserves the newly promoted leading group and reclaims a deep orphan group |
| `tcGcResetAfterSnapshotGuarded` | holds: guard D preserves the re-tier when the reset commits after the snapshot |
| `tcGcExploreGuarded` | holds under PCT over reset timing and sync delivery, guard D on |
| `tcGcCrossNodeStaleUnguarded` | fails: guard A off reaps the live re-tier |
| `tcGcNoReread` | fails: guard B off reaps the live re-tier |
| `tcGcNoLeadingReread` | fails: guard C off reaps the live leading group |
| `tcGcResetAfterSnapshotStale` | fails: shipped guards only, reset after snapshot reaps the live re-tier |
| `tcGcExploreShipped` | fails: PCT with shipped guards only reaches the same deletion |

`NoDanglingReference` is the single monitor: it observes the authoritative committed manifest and asserts GC never deletes a currently referenced object, across data, manifest, and leading-group objects at once.

```bash
p compile
p check -tc tcGcBasicReclaim              -i 2000   # 0 bugs
p check -tc tcGcResetAfterSnapshotGuarded -i 2000   # 0 bugs (guard D closes the gap)
p check -tc tcGcExploreGuarded            -i 6000 --sch-pct 20   # 0 bugs
p check -tc tcGcResetAfterSnapshotStale   -i 2000   # 1 bug (the shipped gap)
p check -tc tcGcNoReread                  -i 2000   # 1 bug
p check -tc tcGcNoLeadingReread           -i 2000   # 1 bug
p check -tc tcGcCrossNodeStaleUnguarded   -i 2000   # 1 bug
```

## What the model does not cover

The stale-epoch reap path deletes with no re-check, and its safety rests on epoch monotonicity, which is the subject of the writer-fencing model rather than this one. The model is one stream and one sweeping node, with the rest of the cluster represented by the committed-truth announcements and the lagging cache. The fail-closed-on-quorum-error path (a partitioned node that cannot reach a quorum) is covered by gc-reset-multinode and is not repeated here.
