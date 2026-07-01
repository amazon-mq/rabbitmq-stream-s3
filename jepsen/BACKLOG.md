# Jepsen harness backlog

Follow-up work for the `rabbitmq_stream_s3` Jepsen harness, in rough priority order.

## Bounded-durability checker for aggressive retention

A `force-trim` nemesis that drops a stream's local retention past the upload seam `n`, paired with a checker scoped to the `[f, n)` range, so loss is only flagged inside the durable range the plugin actually promises (see `../docs/invariants.md`, "Non-guarantee"). The current durability checker assumes retention is wide enough that nothing legitimately drops, which is why retention is configured generously today.

## Hard-kill / stream-churn nemesis to reproduce the manifest-replica leak

The `:replica` manifest-replica consistency checker's `:leaked-replica-row` assertion (a cache row on a non-leader node with no registered replica context) only *fires* when an osiris member departs a node permanently mid-sync: a sync arriving after the member's `DOWN` re-creates a cache row with no monitor to reclaim it. Graceful `leader-move` (which transfers leadership, leaving the member set intact) does not cause this, so the assertion ships as a cheap always-on guard rather than something the current faults reproduce.

Reliably exercising it needs a nemesis that makes members genuinely leave a node mid-sync, for example:

- a hard kill (SIGKILL) of a broker while uploads/syncs are in flight, then a restart that relocates the member, or
- stream churn (delete and re-create streams, or add/remove replicas) under the S3-outage fault so syncs race the member teardown

Until then the convergence and stale-floor assertions carry the meaningful verdict for this scenario.

## Run this scenario as a regression check once the manifest-replica fix lands

The manifest-replica sync-context fix (branch `manifest-replica-lifecycle`: drop a sync for a stream with no reader context, and request a resync when a context registers) changes the replica cache's write path. Once it lands on `main`, run this scenario against a cluster carrying it and confirm the convergence, stale-floor, and durability assertions still hold, with the run's `:divergence-exercised?` telemetry and the `syncs_dropped_no_context` and `resyncs_requested` counters confirming the gated and recovery paths were actually taken. This is regression coverage only: it does not positively prove the fix closes the leak, because `:leaked-replica-row` does not fire without the hard-kill or stream-churn nemesis above. The fix itself is validated by the `p/manifest-replica-lifecycle` model's gates and the module's unit tests.

## Super-streams (multi-partition)

Extend the workload to super-streams so partitioned producers and consumers are exercised, not just single streams.
