# Read / tier-resolution seam

Models `log_reader:resolve_first/2` and `resolve_first_lookup/1`: resolving the
`first` offset spec when descending into the leading manifest group requires
fetching that group from S3, and the fetch can fail transiently.

## The bug class

Resolving `first` descends into the leading group (`fragment_iterator:next` ->
`GetGroup`). The fetch has three outcomes, mapped by `resolve_first_lookup/1`:

- `{ok, FragRef, _}` -> `{remote, _}`
- `end_of_manifest` -> `{local, first}`
- `{error, {group_fetch_failed, _}}` -> `{retry, _}` (surfaced as `{error, _}`,
  so the consumer retries)

The pre-[`e3f931b`](https://github.com/amazon-mq/rabbitmq-stream-s3/commit/e3f931b)
code had a catch-all that collapsed `group_fetch_failed` into `{local, first}`.
A transient S3 error while fetching a live group then silently attached the
reader at the local first offset, skipping the entire remote range below the
local floor. (A sibling fix, `649a5fc`, addressed the same collapse in
`remote_reader_core:try_fragment_transition` during streaming; not yet modeled.)

A retry is always the safe answer for `group_fetch_failed`: even if the group was
deleted by a concurrent retention, the retry re-reads a fresh manifest (with the
advanced `first_offset`) and resolves correctly. A local fallback cannot
self-correct.

## What the model captures

- A `Reader` resolving `first`, a `ManifestStore` (remote-tier metadata plus a
  retention advance that empties the remote tier), and a `GroupStore` whose fetch
  can fail transiently or because retention deleted the group
- The `bugCatchAll` toggle selects the buggy mapping (`group_fetch_failed` ->
  `LOCAL_FIRST`) versus the correct one (`-> RETRY`)
- INV#4 (`NoSilentRemoteSkip`): resolving to the local tier is allowed only when
  the remote tier is genuinely empty

## Tests and the validation gate

| Test case | Expectation |
| --- | --- |
| `tcReadResolveGuarded` | **holds** - a transient fetch surfaces as `RETRY` (asserted, not merely "not local") |
| `tcReadResolveBuggy` | **fails** - the catch-all collapses to local; `INV#4 violated: resolved 'first' to the local tier while the remote tier is non-empty` |
| `tcReadResolveExplore` | **holds** - guard on, nondeterministic transient fetch raced against retention |

`tcReadResolveBuggy` is *expected to fail*; that counterexample is the proof the
model reproduces the real bug.

```bash
p compile
p check -tc tcReadResolveGuarded -i 5000   # 0 bugs
p check -tc tcReadResolveBuggy   -i 2000   # 1 bug: INV#4 silent remote skip
p check -tc tcReadResolveExplore -i 5000   # 0 bugs
```
