# Invariants

This document states the invariants the plugin relies on, precisely enough to check a proposed change against them. It assumes the design is understood (see [architecture.md](./architecture.md), [manifest.md](./manifest.md), [upload-path.md](./upload-path.md), and [read-path.md](./read-path.md)) and states only the properties those mechanisms must preserve, in the language of offsets, intervals, and sets of objects. Each invariant is named; the tables below group them by subsystem and point to the design doc that explains the mechanism.

## Notation

Offsets are non-negative integers. A stream's committed log occupies the half-open interval `[0, T)`, where `T` is the committed tail.

A manifest `M` describes the remote tier:

- `f(M)` and `n(M)` are its `first_offset` and `next_offset`, with `0 ≤ f(M) ≤ n(M) ≤ T`.
- `Obj(M)` is the set of S3 objects `M` references: fragments (the leaves of the manifest tree) and groups (its interior nodes).
- A fragment `x` carries an offset interval `I(x) = [lo(x), hi(x))`, a byte size `size(x) ≥ 0`, and a timestamp span `[t₀(x), t₁(x)]`. A group's interval is the union of its descendants'.
- `Frag(M) ⊆ Obj(M)` is the set of fragments, and `Cov(M) = ⋃_{x ∈ Frag(M)} I(x)` is the covered offset set.

States evolve as `M₀, M₁, …` under four operations: append (upload), rebalance, retention, and replication. Unsubscripted symbols denote the current state.

## Durability

| Invariant | Statement | Where |
|---|---|---|
| Coverage | `Cov(M) = [f, n)` and the fragment intervals are pairwise disjoint: every offset in `[f, n)` belongs to exactly one fragment, so the interior has no gap. | [upload-path.md](./upload-path.md) (Durability boundary) |
| Durability | Every offset in `[f, n)` is durable in S3. A local segment may be reclaimed once its offsets fall below `n`; Coverage and Durability are what make that safe. | [upload-path.md](./upload-path.md) (Drain loop) |
| Contiguous advance | `n` is non-decreasing. An append adds one fragment `x` with `lo(x) = n` and sets `n := hi(x)`. No operation advances `n` over an offset outside `Cov`. | [upload-path.md](./upload-path.md) (Drain loop) |

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

## Read path

| Invariant | Statement | Where |
|---|---|---|
| Exactly-once | A read started at spec `s` delivers each offset of `[max(s, f), T)` exactly once, in strictly increasing order, across both tiers. A seek into a group resolves to the unique fragment containing the target (Ordering), not the group's first child. | [read-path.md](./read-path.md) |
| Tier overlap | The local segments cover `[n, T)` while the remote tier covers `[f, n)`; the ranges overlap and the reader hands off at a shared offset, so no offset is skipped or repeated at the seam. | [read-path.md](./read-path.md) |
