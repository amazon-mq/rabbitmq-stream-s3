# P Models

This directory contains [P](https://p-org.github.io/P/) formal models of the
plugin's concurrent protocols. P is an actor-model verification language: each
component is a state machine communicating by asynchronous events, and the
checker explores the interleavings of event delivery. This maps cleanly onto the
plugin's `gen_server` / message-passing architecture, which is why P complements
the [`tla/`](../tla) models (those cover replication; the P models target the
decide-then-act races between independently-scheduled components).

Models are built one seam at a time. A model is only trusted once it reproduces a
known bug when its guard is removed (the *validation gate*), so each model ships
both a guarded test that must hold and an unguarded test that must fail with a
specific counterexample.

## Invariant coverage

The models target five global invariants of the tiered-storage design:

| Invariant | Property | Model |
| --- | --- | --- |
| INV#1 | no lost acked data | `gc-reset/` |
| INV#2 | no dangling reference (GC never deletes a live object) | `gc-reset/`, `gc-leading-group/`, `gc-decision/` |
| INV#3 | no unbounded orphan leak (liveness) | `orphan-leak/`, `manifest-replica-lifecycle/` |
| INV#4 | tier resolution total / gap-free | `read-resolution/`, `tier-routing/` |
| INV#5 | monotonic frontier except a labeled reset | `gc-reset/`, `trimmed-segment/` |
| (lifecycle) | per-node replica state is released when a reader exits | `manifest-replica-lifecycle/` |
| (foundation) | epoch monotonicity / split-brain safety | `writer-fencing/` |

The last row is the mechanism the others assume: `writer-fencing/` proves the
committed epoch is monotonic, which is what gc-reset's epoch axis and the
durability guards rely on.

## Models

### `gc-reset/`

The durability seam: orphan GC versus a remote-tier-ahead manifest reset. Verifies
that the `still_dangling/1` guard in `rabbitmq_stream_s3_gc` prevents the sweep
from deleting a live fragment that a concurrent reset re-tiered below the sweep's
snapshot floor. See [`gc-reset/README.md`](gc-reset/README.md).

### `gc-leading-group/`

A second, independent live-deletion guard in the GC seam: `classify_group`
protects the one leading group that straddles `first_offset` (retention advanced
the floor into it on partial expiry) while deleting other groups below the floor.
Verifies that removing the `referenced_group_key` carve-out deletes a live group.
See [`gc-leading-group/README.md`](gc-leading-group/README.md).

### `gc-decision/`

The whole `rabbitmq_stream_s3_gc` reap decision in one state space: build_lookup,
classify, and still_dangling composed, with all three classify reasons (data and
group below the floor, stale-epoch manifest) and the guards interacting rather
than isolated. Re-proves each shipped guard load-bearing in the composed decision,
and surfaces a gap the single-axis models miss: a reset that commits after the
sweep snapshots, on a node whose cache has not applied the sync, reaps a live
re-tier even with every shipped guard on (the epoch gate is sampled once and
still_dangling re-reads only the floor). Models a proposed execute-time epoch
re-validation that closes it. See [`gc-decision/README.md`](gc-decision/README.md).

### `read-resolution/`

The read / tier-resolution seam: resolving the `first` offset spec when the
leading manifest group fetch fails transiently. Verifies that
`log_reader:resolve_first_lookup/1` surfaces a `group_fetch_failed` as a retry
rather than silently falling back to the local tier (the pre-`e3f931b` catch-all
bug). See [`read-resolution/README.md`](read-resolution/README.md).

### `tier-routing/`

The offset -> tier routing branch of `resolve_remote_location`: when the local
log is empty (`first_chunk_id == -1`), every offset must fall through to the
remote tier. Verifies that dropping the `=/= -1` guard routes a remote-only
offset to the local tail (a silent remote skip). A second INV#4 guard, distinct
from the group-fetch catch-all. See [`tier-routing/README.md`](tier-routing/README.md).

### `trimmed-segment/`

The upload-pipeline seam (issue #225): a head fragment whose local segment is
trimmed by retention before the upload reads it. Verifies that
`handle_transfer_failure` recovers via `restart_at_local_floor` (the
`local_log_ahead` branch) rather than resubmitting forever. This is a *liveness*
property, checked with a `hot`-state monitor. See
[`trimmed-segment/README.md`](trimmed-segment/README.md).

### `writer-fencing/`

The manifest-commit fencing seam (`db.erl` optimistic-lock CAS): a commit
succeeds only if the revision matches and the new epoch is `>=` the stored epoch.
Verifies that removing the epoch fence lets a deposed lower-epoch writer overwrite
a newer one (split-brain / epoch regression). This proves the epoch monotonicity
the other models assume. See [`writer-fencing/README.md`](writer-fencing/README.md).

### `orphan-leak/`

The GC reclamation seam: the reaper tolerates partial-batch DeleteObjects
failures by leaving unconfirmed objects for orphan GC. Verifies that GC's
re-sweep eventually reclaims a transiently-failed delete rather than leaking it
forever. A *liveness* property (`hot`-state monitor). See
[`orphan-leak/README.md`](orphan-leak/README.md).

### `manifest-replica-lifecycle/`

The per-node manifest replica's lifecycle: registration and cleanup when a reader
(osiris member) exits, verifying the design behind commit `cc50092`. osiris has no
terminate hook, so the replica monitors the registering member and releases its
context, gap sequence, and cached row on `'DOWN'`. Proves three shipped guards
load-bearing — the member-`DOWN` cleanup (no metadata leak), `is_stale_sync` (no
stale floor served), and the re-registration monitor repoint (no eviction of a
live reader) — plus a `hot`-state convergence property. Surfaces a gap the
single-axis view misses: with every shipped guard on, a sync that arrives after
the `DOWN` re-strands a cache row with no monitor to reclaim it, and models a
proposed `syncRequiresContext` guard (A2) that closes it. Adds an attach-ordering
axis that proves the guard is **unsafe in isolation**: with the shipped writer-first
attach order a startup sync that beats the local context registration is dropped
and never re-sent (the cache stays empty — a convergence violation). A1
(register-context-before-writer) closes that for the acceptor-reply sync, but a
second axis models the **writer-driven reconcile path** — a member-visible sync
independent of `register_acceptor` that A1 cannot reach — and a proposed
**A1′ resync-on-register** toggle. The gates show A1′ recovers *both* startup
triggers (acceptor reply and reconcile) and **subsumes A1**, so the minimal sound
fix is **A2 + A1′**, not A2 + A1 + A1′.
See [`manifest-replica-lifecycle/README.md`](manifest-replica-lifecycle/README.md).

## Running

Requires the `p` CLI. From a model directory:

```bash
cd gc-reset/
p compile
p check -tc <testCase> -i 2000
```

`p compile` generates C# and builds it; `p check` runs the explicit-state
checker. `-i N` sets the number of schedules to explore; `-tc` selects a test
case. Counterexample traces are written under `PCheckerOutput/BugFinding/`.

The default random strategy can miss rare interleavings (the gc-reset
atomic-prefix bug eludes it even at 50k schedules). Add `--sch-pct N` to use the
PCT strategy, which is designed to surface low-probability concurrency bugs; CI
runs the exploration tests under both strategies.

Generated code (`PGenerated/`) and checker output (`PCheckerOutput/`) are build
artifacts and are gitignored.
