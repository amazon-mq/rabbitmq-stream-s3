# Trimmed-segment upload seam (issue #225)

Models the TOCTOU between submitting a fragment for transfer and the upload
worker reading the local segment, when retention trims that segment in between.

## The bug class

A head fragment (its first offset equals the manifest `next_offset`) is submitted
for transfer. The upload worker `pread`s the local segment. If local retention
trims the log past `next_offset` first, the segment is permanently gone and the
`pread` fails with `enoent`.

`handle_transfer_failure` must distinguish this from a transient error. It checks
`local_log_ahead` (`LocalFirst > NextOffset`): if the local floor has advanced
past the stalled fragment, no retry can ever make that range durable, so it
recovers via `restart_at_local_floor` (reset the frontier to the live local
floor and re-tier from there). Without that check every failure is treated as
retriable and resubmitted - the upload loops forever, tiering wedges, and local
disk grows unbounded (local retention only reclaims offsets below the pinned
`next_offset`).

## Why this is a liveness model

The bug is not a bad state the system reaches; it is the *absence* of progress -
the transfer never resolves. That is a liveness property, so the monitor uses a
P `hot` state (`AwaitingResolution`): an execution that stays in it forever (the
resubmit loop) is a liveness violation, which the checker reports when the hot
state persists past the max-steps temperature bound.

## What the model captures

- A `Reader` driving a transfer, a `Worker` that preads, and a `LocalLog` whose
  retention trim removes the segment and advances the local floor
- The `checkLocalLogAhead` toggle selects the `local_log_ahead` recovery versus
  the #225 bug (always resubmit)
- `TransferEventuallyResolves` (INV#3, liveness): a submitted transfer must
  eventually complete or recover
- `ResetTargetsLocalFloor` (safety): a recovery reset targets the live local floor

## Tests and the validation gate

| Test case | Expectation |
| --- | --- |
| `tcTrimGuarded` | **holds** - a trimmed transfer recovers via reset |
| `tcTrimBuggy` | **fails** - the recovery check removed; the transfer resubmits forever and the `AwaitingResolution` hot state never clears |
| `tcTrimExplore` | **holds** - guard on, the trim raced against the transfer at the log's queue |

`tcTrimBuggy` is *expected to fail*; that liveness counterexample is the proof
the model reproduces the #225 wedge.

```bash
p compile
p check -tc tcTrimGuarded -i 5000   # 0 bugs
p check -tc tcTrimBuggy   -i 2000   # 1 bug: TransferEventuallyResolves never discharged
p check -tc tcTrimExplore -i 5000   # 0 bugs
```
