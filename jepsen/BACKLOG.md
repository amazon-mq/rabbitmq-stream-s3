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

## Super-streams (multi-partition)

Extend the workload to super-streams so partitioned producers and consumers are exercised, not just single streams.
