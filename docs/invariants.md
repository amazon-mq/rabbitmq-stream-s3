# Invariants

This document states the invariants the plugin relies on, precisely enough to check a proposed change against them. It assumes the design is understood (see [architecture.md](./architecture.md), [manifest.md](./manifest.md), [upload-path.md](./upload-path.md), and [read-path.md](./read-path.md)) and states only the properties those mechanisms must preserve, in the language of offsets, intervals, and sets of objects. Each invariant is named; the tables below group them by subsystem and point to the design doc that explains the mechanism.

## Notation

Offsets are non-negative integers. A stream's committed log occupies the half-open interval `[0, T)`, where `T` is the committed tail.

A manifest `M` describes the remote tier:

- `f(M)` and `n(M)` are its `first_offset` and `next_offset`, with `0 ≤ f(M) ≤ n(M) ≤ T`.
- `Obj(M)` is the set of S3 objects `M` references: fragments (the leaves of the manifest tree) and groups (its interior nodes).
- A fragment `x` carries an offset interval `I(x) = [lo(x), hi(x))`, a byte size `size(x) ≥ 0`, and a timestamp span `[t₀(x), t₁(x)]`. A group's interval is the union of its descendants'.
- `Frag(M) ⊆ Obj(M)` is the set of fragments, and `Cov(M) = ⋃_{x ∈ Frag(M)} I(x)` is the covered offset set.

States evolve as `M₀, M₁, …` under five operations: append (upload), rebalance, retention, replication, and reset (local-log-ahead recovery, see [Recovery](#recovery)). Unsubscripted symbols denote the current state.

`f_local` denotes the first offset of the local log, the floor below which the local log has been trimmed. The local segments cover `[f_local, T)`.

## Durability

| Invariant | Statement | Where |
|---|---|---|
| Coverage | `Cov(M) = [f, n)` and the fragment intervals are pairwise disjoint: every offset in `[f, n)` belongs to exactly one fragment, so the interior has no gap. | [upload-path.md](./upload-path.md) (Durability boundary) |
| Durability | Every offset in `[f, n)` is durable in S3, and durability is promised for `[f, n)` **only**, not for the un-tiered tail `[n, T)`. Local segments are reclaimed by two independent drivers: the tiering fun (offsets below `n`, made safe by Coverage and Durability) and the user's retention bound (`max-age`/`max-bytes`), which is authoritative over upload progress and may trim un-tiered segments at or above `n` once they pass the bound. When that lifts `f_local` above `n`, the un-uploaded range `[n, f_local)` is lost from both tiers; see Reset safety. | [upload-path.md](./upload-path.md) (Drain loop), [architecture.md](./architecture.md) (Durability versus the local retention bound) |
| Contiguous advance | `n` is non-decreasing. An append adds one fragment `x` with `lo(x) = n` and sets `n := hi(x)`. No operation advances `n` over an offset outside `Cov`. | [upload-path.md](./upload-path.md) (Drain loop) |

> **Non-guarantee.** The plugin does not guarantee that every acked offset reaches S3. Only `[f, n)` is durable. The un-uploaded tail past the user's retention bound is lost exactly as a non-tiered stream with the same policy would lose it: the bound is a stream-retention limit, not a local-disk limit, and local trimming wins over upload progress (see [architecture.md](./architecture.md), "Durability versus the local retention bound", and issues #206/#225/#227). A formal model must therefore scope durability to `[f, n)` and model the retention-trim-past-seam + reset; it must not assert that every appended offset is eventually durable.

## Retention

Retention is the only operation that removes data, and it removes only a prefix.

| Invariant | Statement | Where |
|---|---|---|
| Prefix monotonicity | A step sends `f` to some `f′` with `f ≤ f′ ≤ n` and leaves `n` fixed; afterwards `Cov(M) = [f′, n)`. So `f` is non-decreasing and no offset `≥ f′` is removed. | [manifest.md](./manifest.md) (Retention within groups) |
| Deletion set | The objects deleted are exactly `{ x ∈ Obj(M) : I(x) ⊆ [f, f′) }`: the fragments wholly below `f′` and the groups all of whose descendants are. Every object with `hi > f′` survives. | [manifest.md](./manifest.md) (Retention within groups) |
| Idempotence | For fixed parameters a second evaluation deletes `∅`. Group objects are immutable, so a re-evaluation re-reads the same children; idempotence requires skipping the already-removed prefix below `f`. | [manifest.md](./manifest.md) (Retention within groups) |
| Accounting | `total_size(M) = Σ_{x ∈ Frag(M)} size(x)`, and `f(M) = min_{x ∈ Frag(M)} lo(x)` (or `n(M)` when `Frag(M) = ∅`). | [manifest.md](./manifest.md) (Retention within groups) |

## Manifest structure

| Invariant | Statement | Where |
|---|---|---|
| Ordering | Fragment intervals are pairwise disjoint and totally ordered by offset, so an offset resolves to at most one fragment; a group's children are stored in increasing offset order. | [manifest.md](./manifest.md) (Data structure) |
| Immutability | Group objects are immutable. `Obj(M)` changes only by creation (rebalance adds a group and removes nothing) and by deletion (Deletion set). No object's bytes are rewritten. | [manifest.md](./manifest.md) (Factoring out groups) |
| Edit ordering | Within the edit sequence a persist emits, the append that creates an entry precedes any rebalance edit that replaces it. | [manifest.md](./manifest.md) (Edit ordering when rebalancing) |
| Single mutator | Persist, rebalance, and retention all rewrite the leading entries, and at most one is in flight, so none observes a front another has concurrently changed. | [upload-path.md](./upload-path.md) (Functional core) |

## Consistency and concurrency

| Invariant | Statement | Where |
|---|---|---|
| Local authority | The local log is authoritative: if the local timeline diverges from the remote tier, `M` is truncated so that `n(M) ≤ T_local`. Write availability is never blocked on the remote tier. | [manifest.md](./manifest.md) (Resolving the manifest) |
| Key disjointness | Every object key carries a random UID, so writers contending across a leader change produce disjoint subsets of `Obj` and cannot overwrite one another. | [manifest.md](./manifest.md) (Concurrency control) |
| Epoch monotonicity | The committed-manifest pointer in Khepri carries a non-decreasing epoch; an update tagged with epoch `e` is rejected once an update with epoch `> e` has committed, so a deposed writer cannot advance the committed manifest past its successor. | [manifest.md](./manifest.md) (Concurrency control) |

## Replication

| Invariant | Statement | Where |
|---|---|---|
| Replica prefix | A replica's applied edits are always a prefix `(ε₁, …, ε_j)` of the writer's edit sequence, and its manifest equals `fold(apply, M₀, (ε₁, …, ε_j))`. A sequence gap forces a re-sync rather than an out-of-order apply. Modeled in [`tla/manifest-replication/`](../tla/manifest-replication/). | [manifest.md](./manifest.md) (Manifest edit replication) |

## Recovery

Append, rebalance, and retention keep the remote tier a contiguous extension below the local log. None of them handles the local log being trimmed past the tier: when user retention deletes segments faster than they upload, the local floor `f_local` rises above the seam `n`, and the offsets `[n, f_local)` are durable in neither tier and can never be uploaded. The reset operation recovers from this. The local retention bound is authoritative and is never extended to wait for upload, so reset accepts the loss rather than stalling (see [architecture.md](./architecture.md), Durability versus the local retention bound).

| Invariant | Statement | Where |
|---|---|---|
| Reset safety | When `f_local > n`, the writer discards `M` and installs the empty manifest at the floor: `f := n := f_local` and `Frag := ∅`. Coverage and Durability hold vacuously, since `Cov = ∅ = [f, n)`; the step advances `n` over `[n, f_local)` only while emptying coverage, so it never claims durability it lacks. This is the inverse of the #206 hole, which advanced `n` while claiming coverage. `f` and `n` are non-decreasing, and the seam is restored at `f_local`, so Tier overlap holds again with the remote tier empty and the local segments covering `[f_local, T)`. The discarded range `[f, f_local)` is lost by design: it is the un-uploaded tail together with the now-disconnected durable prefix, which cannot be kept without a hole in `M`. | [architecture.md](./architecture.md) (Durability versus the local retention bound) |

## Garbage collection

GC reaps an S3 object only when it can prove the object is not live. For an object whose stream is still in the committed lookup that proof is the monotonic offset/epoch barrier. For an object whose stream is absent from the lookup (deleted, never committed, or missing a local replica) the proof is the per-stream anchor.

| Invariant | Statement | Where |
|---|---|---|
| Anchor ordering | A stream's anchor is committed to Khepri before its first S3 object is written, so no object can exist under a prefix whose anchor is absent. The anchor's presence therefore witnesses a live stream, and its absence witnesses a prefix that is safe to reap. | [operations.md](./operations.md#safety-guarantee-the-anchor) |
| Anchor removal | The anchor is kept alive by a `keep_while` on the stream queue, so it is removed in the same transaction that deletes the queue. The "stream deleted" signal is permanent and cannot be lost to a crash. | [operations.md](./operations.md#safety-guarantee-the-anchor) |
| Anchor read | An object whose stream is absent from the lookup is reaped only when a strongly-consistent read reports its anchor absent; a present anchor or a non-quorum read fails closed. A stale local read could report a live stream's anchor absent, so the consistent read is load-bearing. Modeled in [PR #311](https://github.com/amazon-mq/rabbitmq-stream-s3/pull/311). | [operations.md](./operations.md#safety-guarantee-the-anchor) |

## Read path

| Invariant | Statement | Where |
|---|---|---|
| Exactly-once | A read started at spec `s` delivers each offset of `[max(s, f), T)` exactly once, in strictly increasing order, across both tiers. A seek into a group resolves to the unique fragment containing the target (Ordering), not the group's first child. | [read-path.md](./read-path.md) |
| Tier overlap | The local segments cover `[n, T)` while the remote tier covers `[f, n)`; the ranges overlap and the reader hands off at a shared offset, so no offset is skipped or repeated at the seam. | [read-path.md](./read-path.md) |
