# Jepsen harness backlog

Follow-up work for the `rabbitmq_stream_s3` Jepsen harness, in rough priority order.

## Bounded-durability checker for aggressive retention

A `force-trim` nemesis that drops a stream's local retention past the upload seam `n`, paired with a checker scoped to the `[f, n)` range, so loss is only flagged inside the durable range the plugin actually promises (see `../docs/invariants.md`, "Non-guarantee"). The current durability checker assumes retention is wide enough that nothing legitimately drops, which is why retention is configured generously today.

## member-churn nemesis to reproduce the manifest-replica leak

The `:replica` manifest-replica consistency checker's `:leaked-replica-row` assertion (a cache row on a non-leader node with no registered replica context) only *fires* when an osiris member departs a node **permanently** mid-sync: a sync arriving after the member's `DOWN` re-creates a cache row with no monitor to reclaim it.

`leader-move` does not reproduce this, but not for the reason first assumed. `transfer_leadership` calls `rabbit_stream_coordinator:restart_stream/2`, which sets `target = stopped` on *every* member (writer and all replicas) and restarts them - so the osiris members genuinely go `DOWN`, and the DOWN-then-racing-sync window is exercised (this is what the `syncs_dropped_no_context` counter records under `leader-move`). What `leader-move` leaves intact is the **node set**: every member restarts *in place* on the same node, so each departed member re-registers a context that both drops the racing sync (post-fix) and would reclaim an orphan row (even pre-fix). Nothing stays orphaned at quiesce.

Reproducing a *persistent* leak therefore needs a member to leave a node and not come back. Since a stream spans all cluster nodes by default (`initial_cluster_size(undefined) -> length(rabbit_nodes:list_members())`, five members in the five-node cluster), `delete_replica` is the clean way to do this: it permanently removes a member from a node, so a sync racing the teardown has no re-registration to reclaim it.

Implemented as the **member-churn** nemesis (`nemesis.clj`, `db.clj`): each tick it `delete_replica`s a random replica from a random stream, bounded to keep every stream at leader + >=1 replica so durability holds for the final read. It is delete-only by design - re-adding the member would re-register a context that reclaims (masks) an orphan before the checker's end-of-run snapshot. Pair it with `s3-outage`/`s3-latency` to widen the window in which a sync races the teardown:

```sh
FAULTS=member-churn,s3-latency ./run.sh
```

Validated green on a fresh five-node cluster (`member-churn,s3-latency`, 150s): overall `:valid? true`, `:replica` with `:diverged`, `:stale-floors` and `:leaked-rows` all empty, `:durability` clean, and `syncs-dropped-no-context` at 395 - so `delete_replica` genuinely drove the sync-context guard end to end, not merely present.

One implementation note the run surfaced: `delete_replica` restarts the stream's writer, so it drifts the client's send offsets exactly like `leader-move` (the coordinator sets `target = stopped` on every running member, not just the removed one - see `rabbit_stream_coordinator:update_stream0/3`). The kafka workload checker therefore has to be downgraded to advisory under `member-churn` too, with durability carrying the safety verdict (`core.clj`). Also mind cluster reuse: because the nemesis is delete-only, replicas do not come back, so re-running against the same cluster churns already-thinned streams - run it on a fresh cluster, as CI does.

Not in CI until the coordinator writer-liveness fix lands. Under `delete_replica` churn a stream can be left leaderless - every member stranded in `current = {starting, _}` with no running writer - and never re-elect. This is the stream coordinator writer-liveness bug [#16822](https://github.com/rabbitmq/rabbitmq-server/issues/16822), fixed by [#16881](https://github.com/rabbitmq/rabbitmq-server/pull/16881), which adds a `maybe_reelect_leader` backstop in `evaluate_stream` and clears stale `starting` actions on epoch transition, gated at coordinator machine version 8. Our broker is still at machine version 7 (`rabbit_stream_coordinator:version/0`), so it carries the bug and member-churn can intermittently wedge the cluster (a writer-less stream hangs its consumers, which hangs the poll). Bounded graceful churn usually self-heals in seconds, so short runs mostly pass, but the hazard is real. Keep `member-churn` out of the `jepsen.yaml` CI matrix until the fix is in the broker we build against (machine version 8); at that point member-churn doubles as a regression test for #16822.

Two things still open here:

- The assertion can only be made to positively *fire* (proving it has teeth) against a build with the fix removed - a negative control - since with the fix in place even the departing-member race is handled and the row is never created.
- A hard kill (SIGKILL) of a broker that then relocates the member is a stronger, more abrupt variant still worth adding; `delete_replica` is a graceful, coordinator-driven departure.

Observation surfaced while designing this: post-delete, a sync that races the teardown records a `pending_resync[stream] = writer` entry on the departed node that is never consumed (no context ever re-registers there without a re-add). Post-fix this is a small, permanent map entry rather than a leaked ETS row - harmless for correctness, but unbounded under heavy churn; worth having the delete/forget path clear it.

## Regression check for the manifest-replica fix (validated)

The manifest-replica sync-context fix (drop a sync for a stream with no reader context, and request a resync when a context registers) landed on `main` and changed the replica cache's write path. This scenario was run against a cluster carrying it under `s3-outage,leader-move` and confirmed the convergence, stale-floor, and durability assertions all still hold (`:diverged`, `:stale-floors`, `:leaked-rows` and durability violations all empty, 49/49 keys). The gated and recovery paths were genuinely exercised, not merely present: `syncs_dropped_no_context` and `resyncs_requested` both ticked 240 times over the run, so `:divergence-exercised?` was true and `:convergence-hollow?` false. The matched counts are the fix end to end, every contextless sync the A2 guard dropped was recovered by an A1' resync once a context registered.

This is regression coverage only: it does not positively prove the fix closes the leak, because `:leaked-replica-row` still does not fire without the hard-kill or stream-churn nemesis above (it stayed empty here, as expected under graceful leader-move). The fix itself is validated by the `p/manifest-replica-lifecycle` model's gates and the module's unit tests.

## Super-streams (multi-partition)

Extend the workload to super-streams so partitioned producers and consumers are exercised, not just single streams.
