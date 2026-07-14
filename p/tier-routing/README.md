# Offset-to-tier routing

Models the integer-offset branch of `log_reader:resolve_remote_location/2`: the decision to serve an offset from the local log or the remote tier, including the lifecycle of the manifest cache the decision reads.

## The bugs

Two shipped defects, both INV#4 silent remote skips, both gated on below.

The floor-guard bug. The local-tier check is:

```erlang
first_chunk_id =/= -1 andalso Offset >= first_chunk_id
```

`first_chunk_id = -1` means the local log is empty (fully trimmed or not yet populated). Without the `=/= -1` guard, `Offset >= -1` is always true, so an empty local log routes every offset to the local tail and silently skips the remote tier.

The boot-window bug. The reader consults the node's manifest CACHE, not the manifest. Before the cache had a `pending` state, a missing row meant "no remote tier", so on a freshly restarted node (empty cache, no publish to re-seed it) an offset below the local floor fell back to `{local, first}` and silently skipped the whole remote range for up to one reconciliation period. The fix distinguishes three cache states: `RESOLVED` (route by extent), `PENDING` (attached but unresolved: fail closed, the consumer retries), `ABSENT` (un-tiered stream: local is the whole stream), and places the pending marker in member init, before any reader can attach.

## What the model captures

- A `ManifestStore` as a lifecycle state machine (`Cold -> Pending -> Serving`), not a static oracle: the true remote extent is fixed at construction but only reported once resolved, exactly like the real cache
- A `Reader` deciding from the cache reply alone, never from the ground truth
- `bugNoMinusOneGuard` drops the `=/= -1` guard; `bugMissFallsLocal` restores the pre-fix miss collapse (`PENDING -> local`)
- `TierRoutingCorrect` (INV#4): an offset held only by the remote tier must not route `LOCAL` (`RETRY` is acceptable, it fails closed). Coverage is re-derived by the monitor from the ground truth the driver announces (`eGroundTruth`), never from the cache's reply. The previous version of this spec used the cache's own answer as the oracle, which made the boot-window bug unrepresentable: a reader that believed a cold cache could not be flagged. The oracle must stay independent of the component under test.

## Tests

| Test case | Expectation |
| --- | --- |
| `tcTierRoutingGuardedWarm` | holds: full lifecycle, then a deterministic probe grid over every local floor (`-1`, `0`, `30`) and offset (`0`..`175`) |
| `tcTierRoutingGuardedColdStart` | holds: readers attach while the resolution races them; pending probes `RETRY`, never `LOCAL`, and a probe after the acknowledged resolution is served remotely |
| `tcTierRoutingBuggyNoMinusOneGuard` | fails: the `=/= -1` guard removed; an empty-local read of a remote-only offset routes local |
| `tcTierRoutingBuggyMissFallsLocal` | fails: the pre-fix miss collapse; a below-floor read on an unresolved cache routes local (the boot-window bug) |
| `tcTierRoutingBuggyNoMarker` | fails: the environment weakened instead of the code; no pending marker, so a reader attaching on a tiered stream with an absent row routes local. Proves the marker's member-init placement is load-bearing, not just the reader's `PENDING` branch |

The three failing runs are the validation gate: each proves the spec reproduces one specific shipped (or nearly shipped) defect.

```bash
p compile
p check -tc tcTierRoutingGuardedWarm      -s 2000   # 0 bugs
p check -tc tcTierRoutingGuardedColdStart -s 2000   # 0 bugs
p check -tc tcTierRoutingBuggyNoMinusOneGuard -s 500   # 1 bug: INV#4 misrouting
p check -tc tcTierRoutingBuggyMissFallsLocal  -s 500   # 1 bug: INV#4 misrouting
p check -tc tcTierRoutingBuggyNoMarker        -s 500   # 1 bug: INV#4 misrouting
```

## Scope note

The cache-availability axis lives in this model. The sibling `read-resolution` model covers the group-fetch classification inside `resolve_first`; the `pending` clause added to `resolve_first` itself is exercised directly by `read_resolution_SUITE` (Erlang property and eunit cases), which drives the real module rather than a model of it.
