# Anchor-before-first-fragment cleanup

A model of a correct-by-construction design for reclaiming a stream's remote-tier objects when its queue is deleted. There is no orphan-GC fallback for a deleted stream: GC skips any prefix it has no live manifest for, so once the stream is gone its leftover objects can never be classified. The deletion path therefore has to reclaim everything, and it must never reap a stream that is still live.

## The design

Before the very first fragment is written for a stream, the replica reader writes a dedicated anchor node in Khepri, kept alive by a keep_while condition on the queue, as a blocking step whose write commits strictly before the first S3 PUT. A sweep classifies a prefix as junk when objects are present and the anchor is absent. The keep_while removes the anchor in the same transaction that deletes the queue, so the "anchor absent" signal is produced atomically and is permanent: there is no record written at deletion time that a crash could lose.

This is correct by construction off one ordering invariant. An object exists only after the anchor committed, and the anchor disappears only when the queue does, so there is no live state with objects but no anchor. Two properties fall out for free:

- a stream that never committed a manifest still has an anchor, because the anchor predates the first fragment
- a crash anywhere in the deletion path cannot strand the objects, because the signal is the anchor's permanent absence rather than a write that could be dropped

The model deliberately has no manifest. Objects are bare fragments and the anchor is the only Khepri record, so every scenario is the never-committed case.

## The two load-bearing requirements

Each is proven necessary by a gate that must fail when the requirement is dropped.

| Requirement | If violated | Test |
| --- | --- | --- |
| Consistent reads: the sweep reads the anchor with a strongly consistent (committed) read, not a stale local replica | a stale read reports the anchor absent for a stream whose anchor just committed while S3 already shows its first fragment, and the sweep reaps live data | `tcAnchorStaleReadReapsLive` (must fail) |
| Ordering: the anchor commits before the first fragment PUT | a fragment exists before the anchor commits, so a sweep in that window reaps a live stream even with a consistent read | `tcAnchorOrderingViolated` (must fail) |

The stale-read gate is the same shape as the `../gc-reset-multinode` stale-floor finding: a local replica cache that lags committed Khepri defeats an otherwise sound guard.

## Tests

| Test case | Expectation |
| --- | --- |
| `tcAnchorReclaimsAcrossCrash` | holds: consistent reads and anchor-before-fragment reclaim the junk even when a sweep crashes mid-reap, because the absence signal is permanent |
| `tcAnchorExplore` | holds across nondeterministic replication timing and a crash mid-sweep |
| `tcAnchorStaleReadReapsLive` | fails with `INV NOREAPLIVE violated: GC reaped object ... while the stream's queue is still live` |
| `tcAnchorOrderingViolated` | fails with the same counterexample, from the ordering violation |

`NoReapLive` is the headline monitor: a reap may happen only after the queue is deleted, and both gates trip it. `EventuallyEmpty` is the reclaim property: once the stream is deleted and sweeps quiesce, nothing remains.

```bash
p compile
p check -tc tcAnchorReclaimsAcrossCrash -i 2000   # 0 bugs (reclaims across a crash)
p check -tc tcAnchorExplore             -i 5000   # 0 bugs, crash + replication explored
p check -tc tcAnchorStaleReadReapsLive  -i 2000   # 1 bug: stale read reaps live data
p check -tc tcAnchorOrderingViolated    -i 2000   # 1 bug: fragment before anchor reaps live data
```

## What the model does not cover

The anchor's absence is a correct classifier but not a work-list. Finding the orphan prefixes still needs the deletion trigger for the prompt path (hung on the anchor node it fires for never-committed streams too) or a periodic S3-prefix scan as a backstop, which is now safe because the decision is by construction. Those provide discovery latency, not correctness.
