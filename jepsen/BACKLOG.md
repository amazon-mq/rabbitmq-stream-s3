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

## Regression check for the manifest-replica fix (validated)

The manifest-replica sync-context fix (drop a sync for a stream with no reader context, and request a resync when a context registers) landed on `main` and changed the replica cache's write path. This scenario was run against a cluster carrying it under `s3-outage,leader-move` and confirmed the convergence, stale-floor, and durability assertions all still hold (`:diverged`, `:stale-floors`, `:leaked-rows` and durability violations all empty, 49/49 keys). The gated and recovery paths were genuinely exercised, not merely present: `syncs_dropped_no_context` and `resyncs_requested` both ticked 240 times over the run, so `:divergence-exercised?` was true and `:convergence-hollow?` false. The matched counts are the fix end to end, every contextless sync the A2 guard dropped was recovered by an A1' resync once a context registered.

This is regression coverage only: it does not positively prove the fix closes the leak, because `:leaked-replica-row` still does not fire without the hard-kill or stream-churn nemesis above (it stayed empty here, as expected under graceful leader-move). The fix itself is validated by the `p/manifest-replica-lifecycle` model's gates and the module's unit tests.

## Super-streams (multi-partition)

Extend the workload to super-streams so partitioned producers and consumers are exercised, not just single streams.
