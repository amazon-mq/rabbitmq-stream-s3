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
