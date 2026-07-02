# GC × reset durability seam

Models the decide-then-act window in `rabbitmq_stream_s3_gc` between an orphan sweep and a remote-tier-ahead manifest reset.

## The race

The sweep is safe under one assumption: `first_offset` only moves forward, so a snapshot floor is never above the live floor. A remote-tier-ahead reset (`restart_at_local_floor`) breaks that assumption. It lowers `first_offset` and re-tiers live fragments, with fresh UIDs, at offsets below the snapshot floor:

1. The sweep captures a snapshot floor (`build_lookup`), e.g. `1000`
2. A reset lowers the live floor to `850` and re-tiers `(850, uid2)` as live
3. The sweep's LIST (which runs after the snapshot) now sees `(850, uid2)`, classifies it against the stale snapshot floor `1000` as `below_first_offset`, and, without re-validation, deletes a live object

The mitigation is `still_dangling/1`: it re-reads the live floor immediately before each `below_first_offset` deletion and skips anything now at or above it. Epoch-based (`stale_epoch`) findings need no re-check because the epoch is monotonic.

## What the model captures

- S3 objects are keyed by `(offset, uid)`, so a stale UID and a freshly re-tiered UID can coexist at the same offset. The headline monitor keys protection on `(offset, uid)`, not offset alone, otherwise it cannot distinguish a legal stale-UID delete from the illegal fresh-UID delete
- The reset lowers the floor and commits the new entry before the re-tiered object is put into S3 (handler ordering), modeling the atomic prefix that keeps the "object present while floor still high" state unreachable
- Faults are deliberately off: this is an ordering bug, not a fault-driven one

### Machines (`PSrc/`)

- `Writer`: the reset orchestrator
- `GC`: the sweep, split into snapshot / list-classify / execute steps
- `ManifestReplica`: serialized owner of the live floor and live entries
- `KhepriDB`: committed epoch (monotonic)
- `S3Store`: the object store

### Monitors (`PSpec/`)

- `NoDanglingReference` (INV#2, headline): GC never deletes a live `(offset, uid)`
- `NoLostAckedData` (INV#1): GC never deletes the live cover of an in-range offset
- `MonotonicFrontier` (INV#5): `first_offset` only decreases across a labeled reset

## Tests

| Test case | Expectation |
| --- | --- |
| `tcGcResetGuarded` | holds: guard on; also asserts the genuine deep orphan `(500,0)` is reclaimed and the live `(850,2)` preserved (anti-vacuity) |
| `tcGcResetUnguarded` | fails: guard off; `INV#2 violated: GC deleted live object (offset=850, uid=2)` |
| `tcGcResetExplore` | holds: guard on, reset raced against the sweep across all interleavings |
| `tcGcResetFloorLast` | fails: guard on, but the reset lowers the floor after re-tiering the object; the same INV#2 violation. Proves the atomic-prefix ordering is load-bearing, so the guard alone is insufficient |
| `tcEpochAxisSafe` | holds: a `stale_epoch` object is deleted with no re-check; safe because the epoch is monotonic (the asymmetry that proves the guard is only needed on the offset axis) |

`tcGcResetFloorLast` is a rare interleaving (the guard's live-floor re-read must land in the window after the object is referenced but before the floor is lowered). The random strategy misses it even at 50k schedules; use the PCT strategy:

```bash
p check -tc tcGcResetFloorLast --sch-pct 10 -i 5000   # 1 bug: INV#2 on (850, uid 2)
```

`tcGcResetUnguarded` is expected to fail: that failing counterexample is the proof the model reproduces the real bug, so the guarded result is meaningful.

```bash
p compile
p check -tc tcGcResetGuarded  -i 2000   # 0 bugs
p check -tc tcGcResetUnguarded -i 2000  # 1 bug: INV#2 on (850, uid 2)
p check -tc tcGcResetExplore  -i 5000   # 0 bugs, multiple timelines
p check -tc tcEpochAxisSafe   -i 2000   # 0 bugs
```
