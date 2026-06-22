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

## Models

### `gc-reset/`

The durability seam: orphan GC versus a remote-tier-ahead manifest reset. Verifies
that the `still_dangling/1` guard in `rabbitmq_stream_s3_gc` prevents the sweep
from deleting a live fragment that a concurrent reset re-tiered below the sweep's
snapshot floor. See [`gc-reset/README.md`](gc-reset/README.md).

### `read-resolution/`

The read / tier-resolution seam: resolving the `first` offset spec when the
leading manifest group fetch fails transiently. Verifies that
`log_reader:resolve_first_lookup/1` surfaces a `group_fetch_failed` as a retry
rather than silently falling back to the local tier (the pre-`e3f931b` catch-all
bug). See [`read-resolution/README.md`](read-resolution/README.md).

### `trimmed-segment/`

The upload-pipeline seam (issue #225): a head fragment whose local segment is
trimmed by retention before the upload reads it. Verifies that
`handle_transfer_failure` recovers via `restart_at_local_floor` (the
`local_log_ahead` branch) rather than resubmitting forever. This is a *liveness*
property, checked with a `hot`-state monitor. See
[`trimmed-segment/README.md`](trimmed-segment/README.md).

## Running

Requires the `p` CLI (provided by the repository Nix dev shell). From a model
directory:

```bash
cd gc-reset/
p compile
p check -tc <testCase> -i 2000
```

`p compile` generates C# and builds it; `p check` runs the explicit-state
checker. `-i N` sets the number of schedules to explore; `-tc` selects a test
case. Counterexample traces are written under `PCheckerOutput/BugFinding/`.

Generated code (`PGenerated/`) and checker output (`PCheckerOutput/`) are build
artifacts and are gitignored.
