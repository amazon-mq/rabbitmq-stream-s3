# Offset to tier routing

Models the integer-offset branch of `log_reader:resolve_remote_location/2`: the
decision to serve an offset from the local log or the remote tier.

## The bug class

The local-tier check is:

```erlang
first_chunk_id =/= -1 andalso Offset >= first_chunk_id
```

`first_chunk_id = -1` means the local log is empty (fully trimmed or not yet
populated). Without the `=/= -1` guard, `Offset >= -1` is always true, so an
empty local log routes *every* offset to the local tail and silently skips the
remote tier - the consumer sees no data (or waits at the tail) for offsets the
remote tier actually holds. The guard makes an empty local log fall through to
remote resolution.

This is a second INV#4 guard, distinct from the `read-resolution/`
group-fetch catch-all: that one is about a transient failure while descending
into a group; this one is about routing the offset to the correct tier in the
first place.

## What the model captures

- A `Reader` routing an offset given `first_chunk_id` (the local floor, `-1` when
  empty) and the remote extent `[remoteFirst, remoteNext)`
- The full branch lattice: local-covered, no-remote, empty-local-beyond-tail, and
  remote
- `bugNoMinusOneGuard` drops the `=/= -1` guard
- `TierRoutingCorrect` (INV#4): an offset held only by the remote tier (covered
  by the remote extent, not by the local log) must route to the remote tier. An
  offset the local tier also holds may route locally (osiris prefers the local
  reader), so the obligation is only on offsets the local tier does not cover

## Tests and the validation gate

| Test case | Expectation |
| --- | --- |
| `tcTierRoutingGuarded` | **holds** - the guarded driver deterministically probes every combination of local floor (`-1`, `0`, `30`) and offset (`0`..`175`), so totality does not rely on random sampling |
| `tcTierRoutingBuggy` | **fails** - the `=/= -1` guard removed; an empty-local read of a remote-only offset routes local; `INV#4 violated: an offset held only by the remote tier routed to the local tier` |

`tcTierRoutingBuggy` is *expected to fail*; that counterexample is the proof the
model reproduces the empty-local silent remote skip.

```bash
p compile
p check -tc tcTierRoutingGuarded -i 5000   # 0 bugs (full floor x offset grid)
p check -tc tcTierRoutingBuggy   -i 2000   # 1 bug: INV#4 misrouting
```
