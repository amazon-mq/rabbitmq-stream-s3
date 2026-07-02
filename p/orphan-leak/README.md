# Orphan-leak GC seam

Models orphan reclamation: the reaper tolerates partial-batch `DeleteObjects` failures, so eventual reclamation depends on GC re-sweeping.

## The bug

`rabbitmq_stream_s3_reaper:delete_batch/1` treats a per-key delete failure as a routine transient and leaves the unconfirmed object for orphan GC, without retrying within the batch. Reclamation liveness therefore rests entirely on GC re-sweeping: a later sweep re-lists the object and re-issues the delete. If GC runs only once (or never re-sweeps), a transiently-failed delete leaks forever, and orphaned objects accumulate unbounded in the bucket.

## Why this is a liveness model

The property is INV#3, that every orphan is eventually reclaimed. The failure is the absence of progress, so the monitor uses a P `hot` state (`Dirty`, entered while any orphan is outstanding). An execution that ends, or loops, with an orphan still outstanding is a liveness violation.

## What the model captures

- An `S3Store` holding orphans, whose delete fails transiently while a bounded fault budget remains (modeling the reaper's routine partial failures) and then succeeds once the transient clears
- A `GC` sweep that lists and deletes, leaving failed deletes behind; `reSweep` is the liveness mechanism (re-list and re-delete until clean)
- `OrphanEventuallyReclaimed` (INV#3, liveness)

## Tests

| Test case | Expectation |
| --- | --- |
| `tcOrphanGuarded` | holds: GC re-sweeps, so the transiently-failed delete is reclaimed on a later pass |
| `tcOrphanBuggy` | fails: a single pass leaves the failed delete leaked; the `Dirty` hot state never clears |
| `tcOrphanExplore` | holds: guard on, multiple orphans and a larger fault budget |

`tcOrphanBuggy` is expected to fail: that liveness counterexample is the proof the model reproduces the unbounded leak.

```bash
p compile
p check -tc tcOrphanGuarded -i 5000   # 0 bugs
p check -tc tcOrphanBuggy   -i 2000   # 1 bug: OrphanEventuallyReclaimed never discharged
p check -tc tcOrphanExplore -i 5000   # 0 bugs
```
