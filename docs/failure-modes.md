# Failure Modes

Each failure mode is described with: trigger, impact assessment, detection, mitigation, and resolution. Most failure modes are self-healing.

For the concepts behind these scenarios, see [concepts.md](./concepts.md). For the metric definitions, see [operations.md](./operations.md#monitoring).

---

## S3 outage

**Trigger.** S3 becomes unreachable or returns errors for an extended period.

**Impact assessment.** Publishing and local consumption are unaffected. Uploads stall but the writer is not blocked. Customer impact is limited to consumers reading old data from S3 (those reads stall) and to durability of recent data (which has not yet reached S3). If `rabbitmq_stream_s3_local_log_ahead_recoveries` stays at zero throughout the outage, no remote-tier gap will form once S3 recovers.

**Detection.**

- `rate(rabbitmq_stream_s3_response_500[5m])` and `rate(rabbitmq_stream_s3_response_503[5m])` rise.
- `rate(rabbitmq_stream_s3_transfers_failed[5m])` rises.
- `rabbitmq_stream_s3_transfers_in_flight` grows as new fragments queue.
- `rabbitmq_stream_s3_governor_pending_submissions` grows when the rate limit is configured.
- `rabbitmq_stream_s3_request_duration_seconds` histogram tail extends.
- Logs include warnings from `rabbitmq_stream_s3_api_aws` about HTTP errors and `gun` connection issues.

**Mitigation.** None for the upload path: the system retries automatically. Consumers reading from S3 cannot make progress until S3 recovers. This is inherent to tiered storage.

**Resolution.** Automatic. When S3 recovers, the replica reader resumes uploading and consumers resume reading. Any gaps formed during the outage (because local retention deleted un-uploaded data) are permanent but consumers are repositioned past them.

---

## Upload retry strategy

A failed fragment upload is always retried; the fragment is never abandoned, because dropping it would advance the manifest over a range that is not durable in S3 and leave a silent hole (issue #206). How the retry is paced depends on the error class.

**Transient errors** (throttling, 5xx, timeouts, connection errors) are expected to clear quickly. The first retry is immediate, preserving responsiveness for a one-off blip. Successive consecutive failures of the same fragment back off exponentially from `stream_s3.task_retry_delay_constant` (10ms) by a factor of `stream_s3.task_retry_delay_exponent` (2), capped at `stream_s3.task_retry_delay_max_ms` (5s).

**Non-transient errors** (a confirmed checksum mismatch, an unexpected 4xx) are unlikely to clear on a tight retry, so the pipeline stalls at the failed offset and retries with a backoff starting at `upload_retry_delay_ms` (1000ms), growing to `upload_retry_delay_max_ms` (30s). Local-tier cleanup also stalls at that offset, so the only durable copy is retained until the upload succeeds.

Both profiles apply equal jitter (the delay is uniformly distributed in `[delay/2, delay]`) so that many streams stalled by a shared incident do not retry against S3 in lockstep. The per-fragment attempt counter resets once the fragment is durable.

**Detection.** `rate(rabbitmq_stream_s3_transfer_retries[5m])` rising means uploads are not succeeding on the first try. `rate(rabbitmq_stream_s3_nontransient_transfer_retries[5m]) > 0` or `rabbitmq_stream_s3_upload_stalled_offset > 0` means a fragment is failing with a confirmed-fatal error and the pipeline is wedged at that offset; the accompanying WARNING log names the error and offset.

---

## Leader election

**Trigger.** Writer node fails or is shut down. A new writer is elected.

**Impact assessment.** Brief pause in delivery for all consumers on this stream. Upload progress pauses while the new replica reader starts. If the deposed replica reader was mid-upload, its fragment becomes an orphan in S3. No data loss for committed records.

**Detection.**

- `rabbitmq_stream_s3_persist_conflicts` increments by one if the deposed and new replica readers race on a manifest write.
- `rabbitmq_stream_s3_manifests_resolved` increments on the new writer node.
- Logs on the new writer node:
  - `Remote replica reader starting for stream <stream_id>`
  - On the deposed node: `Writer down, stopping remote replica reader for stream <stream_id>`

**Mitigation.** None. The system handles this automatically.

**Resolution.** The new replica reader resolves the manifest from S3, determines where to resume, and begins uploading. Normal operation resumes within seconds. Any orphaned fragment is cleaned up by orphan detection (planned; see "Orphaned S3 objects" below).

---

## Remote tier ahead of local (epoch change)

**Trigger.** The manifest references data from a previous timeline that no longer exists locally. Causes:

- A leader election truncated uncommitted data that the previous replica reader had already uploaded. See [epochs and elections](./concepts.md#epochs-and-elections).
- A node's data directory was wiped and restarted.

**Impact assessment.** The remote tier temporarily contains stale data. Consumers reading from the stale range may see data that is about to be removed. The plugin treats this as a manifest-side error to resolve, not a data-loss event for the local cluster.

**Detection.** Logs on the writer node when the replica reader starts:

```
~ts remote tier ahead of local log (manifest_next=M, local_first=N).
Discarding remote manifest and restarting from the local log.
```

`rabbitmq_stream_s3_remote_tier_ahead_recoveries` increments. The manifest's `rabbitmq_stream_s3_manifest_next_offset` resets to the local log's first offset. (This is the inverse of the "Segment deleted before upload" case below, which fires `rabbitmq_stream_s3_local_log_ahead_recoveries`: there the local log's first offset is *ahead of* the manifest; here the manifest's next offset is *beyond* the local log's last offset.)

**Mitigation.** None. The replica reader handles this automatically on startup.

**Resolution.** The replica reader discards the remote manifest, deletes orphaned fragment objects in the background (an async `rabbitmq_stream_s3_gc` sweep that deletes only objects the new manifest no longer references, routed through the reaper), and resumes normal operation from the local log's first offset. The local log is always authoritative. One sub-case is intentionally left to the ordinary resolution retry rather than triggering a reset: a *completely empty* local log paired with a non-empty manifest, which is more often a transient writer-recovery state than a genuine timeline divergence, and resetting then would prematurely discard recoverable remote data.

---

## Segment deleted before upload

**Trigger.** Local retention deletes a segment the replica reader has not yet uploaded. Most likely during an S3 outage or when local retention is aggressive relative to throughput.

**Impact assessment.** The deleted offsets will never exist in the remote tier. A gap forms. Consumers requesting an offset inside the gap are repositioned to the next available offset (same behavior as a vanilla stream past its retention horizon).

**Detection.**

- `rabbitmq_stream_s3_local_log_ahead_recoveries` increments when the replica reader discovers the gap on its next startup or read attempt.
- Logs:
  - `Segment file missing for stream <stream_id>, retrying`
  - `~ts local log ahead of manifest ... Discarding remote manifest and restarting.`

**Mitigation.** If gaps are unacceptable, increase the segment size (via policy or `x-args`) so retention keeps more data locally. Retention always keeps at least the active segment, and a larger segment retains more headroom against upload lag. See [operations.md](./operations.md#local-retention-and-segment-size).

**Resolution.** Automatic. The replica reader jumps to the oldest available segment and continues. The gap is permanent.

---

## Slow replica reader (upload lag)

**Trigger.** The replica reader cannot keep up with the write rate. Causes: S3 throttling, network issues, sustained throughput exceeding upload capacity, governor rate limit set too low.

**Impact assessment.** Upload lag grows. The remote tier falls further behind the local log. If the lag persists long enough for local retention to delete un-uploaded segments, this becomes the "Segment deleted before upload" scenario.

**Detection.**

- `rabbitmq_stream_s3_transfers_in_flight` stays elevated.
- `rabbitmq_stream_s3_governor_pending_submissions` stays elevated when a rate limit is configured.
- `rate(rabbitmq_stream_s3_transfers_completed[5m])` is below the publish rate divided by the fragment target size.
- `histogram_quantile(0.99, rabbitmq_stream_s3_request_duration_seconds_bucket{kind="write"})` rises.
- `rate(rabbitmq_stream_s3_response_503[5m])` non-zero indicates S3 throttling specifically.

**Mitigation.**

- For S3 throttling: the system retries automatically. S3 scales up under sustained traffic.
- For network or capacity issues: review instance bandwidth and `stream_s3.max_transfer_bytes_per_sec` if it is set. A sustained upload rate that holds then cliffs to a flat floor is usually host network burst-credit exhaustion, not the plugin; see [troubleshooting.md](./troubleshooting.md#upload-throughput-cliffs-after-a-sustained-burst).
- For sustained over-throughput: ensure local retention has enough headroom that uploads can catch up after transient slowdowns.

**Resolution.** Transient lag is self-correcting. Persistent lag indicates a capacity mismatch between write throughput and upload throughput; treat this as a capacity-planning issue.

---

## Lost transfer result (stalled upload pipeline)

**Trigger.** A fragment transfer is submitted to the governor but no `{transfer_result, ...}` ever comes back. Three ways this happens:

- The governor process crashes while the submission is still in its in-memory `pending` queue. The governor is supervised on its own and restarts with an empty `pending` queue, so the submission is lost. The `pending` queue is only non-empty when a finite `stream_s3.max_transfer_bytes_per_sec` is throttling, so this trigger requires a rate limit. In-flight tasks run in unlinked processes that outlive a governor restart and still deliver their results.
- The spawned upload task is killed externally (for example by the OOM killer) before it replies. The task's exception handling only converts errors raised inside the upload function; an external kill produces no result.
- The result message is otherwise lost.

**Impact assessment.** Without recovery, the affected reader's in-flight queue head never drains, so `next_offset` is pinned, await-offset waiters never return, and local retention cannot reclaim the affected segments. The stream's remote tier stops advancing while the local log keeps growing. If local retention then trims past the pinned `next_offset`, the reader takes the local-log-ahead recovery path and the un-uploaded range is lost from both tiers. The failure is otherwise silent.

**Detection.**

- `rabbitmq_stream_s3_transfers_in_flight` stays elevated with no matching `rate(rabbitmq_stream_s3_transfers_completed[5m])`.
- `transfer_deadlines_armed` in the replica reader's `format_state` shows armed deadlines that do not clear.
- Logs: `~ts no result for an in-flight fragment transfer within the transfer deadline ... Resubmitting to keep the upload pipeline live.`

**Mitigation.** None required: recovery is automatic.

**Resolution.** Each submitted transfer arms a reader-side deadline (`stream_s3.transfer_deadline_ms`, default four times `segment_upload_timeout`). On expiry the reader resubmits the transfer under the same reference through the normal retry path, recovering from a dropped `pending` item, an externally killed task, or a lost message with one mechanism. The deadline is generous so a healthy but slow upload (including time queued behind the token bucket) is not resubmitted spuriously; a spurious resubmit is harmless because the reference is reused, the first result to arrive is accounted and the duplicate is discarded, and the losing upload's object becomes an orphan that GC reclaims. If local retention has already trimmed past the stalled offset by the time the deadline fires, the reader instead takes the local-log-ahead recovery path (see "Segment deleted before upload").

---

## Persist conflict (deposed writer races new writer)

**Trigger.** Two replica readers attempt to update the same stream's manifest in Khepri. Possible during partition-induced leader elections.

**Impact assessment.** No data corruption: Khepri's optimistic lock rejects the deposed writer's update. The deposed writer's fragment becomes an orphan in S3. The new writer's manifest is authoritative.

**Detection.**

- `rabbitmq_stream_s3_persist_conflicts` increments.
- `rabbitmq_stream_s3_put_conflicts` (Khepri-side counter) increments in lockstep.
- Logs on the deposed node: `~ts reinitializing after commit conflict`.

**Mitigation.** None. The system handles this automatically.

**Resolution.** The losing replica reader re-resolves the manifest from S3 and reinitializes its core state. If the deposed writer's process is still running on the partitioned side, it will continue to lose conflicts until the partition heals or it is stopped.

---

## Data loss from crash and partition

**Trigger.** A node crashes (losing unflushed page cache data) and the node holding the surviving copy is partitioned from the remaining cluster.

**Example.** Three nodes A, B, C all at committed offset 100. Node B crashes and restarts with only offset 75 on disk (offsets 76-100 were in page cache). Node A is then partitioned from B and C. Nodes B and C elect B as writer in a new epoch. B begins writing from offset 76, overwriting the old 76-100. Node A has the original data but is partitioned and will truncate when it rejoins.

Committed data (offsets 76-100, confirmed to publishers) is lost. This requires only a single node crash plus a partition, not simultaneous power loss on multiple nodes.

**Impact assessment.** Confirmed messages are lost. The stream is shorter than expected from the publisher's perspective. Publishers received confirms for messages that no longer exist anywhere in the cluster. If the lost range had been uploaded to S3 before the crash, the data is recoverable from the remote tier.

**Detection.**

- After the partition heals, node A's log truncates to match the new epoch.
- Stream offsets may appear to go backwards from a monitoring perspective.
- The remote tier may be ahead of the local log, triggering "Remote tier ahead of local" handling.

**Mitigation.** This cannot be mitigated after the fact. The data is gone from the local cluster.

**Prevention.**

- Deploy across availability zones with independent failure domains.
- Data uploaded to S3 before the crash is safe. The remote tier is the durability floor for uploaded data.
- Streams trade durability against power loss for throughput (no fsync). This is a property of the streaming subsystem, not specific to this plugin.

**Resolution.** The cluster converges on the new timeline. The replica reader handles any "Remote tier ahead of local" scenario. Normal operation resumes.

---

## Orphaned S3 objects

**Trigger.** S3 objects exist that are not referenced by any manifest. Causes:

- Deposed replica reader uploaded a fragment that was never applied to the manifest.
- Manifest truncation removed entries but object deletion failed or is pending.
- Stream deletion cleanup was interrupted.
- A node's data directory was wiped (e.g. during node replacement or disaster recovery) and RabbitMQ restarted. The Khepri database is recreated from scratch and any streams that existed before the wipe are gone from Khepri, but not via a delete operation, so the keep-while condition that triggers S3 cleanup never evaluates and the S3 objects remain. The per-stream anchor is also gone with the wiped database, so a GC run reclaims these objects via the no-anchor path (see Resolution below).

**Impact assessment.** Wasted S3 storage. No correctness impact. No impact on consumers or publishers.

**Detection.**

- `rate(rabbitmq_stream_s3_persist_conflicts[1h])` greater than zero indicates orphans are likely accumulating from leadership elections.
- Listing the bucket prefix and comparing against Khepri reveals concrete orphans:

```bash
aws s3 ls "s3://${BUCKET}/rabbitmq/stream/${STREAM_ID}/data/" --recursive
```

**Mitigation.** None needed urgently. Orphans do not affect operation.

**Resolution.** The `rabbitmq_stream_s3_gc` module identifies and optionally deletes orphaned objects. Run it via the CLI:

```bash
rabbitmq-streams stream_s3_gc
rabbitmq-streams stream_s3_gc --formatter json
rabbitmq-streams stream_s3_gc --mode delete
```

Objects whose stream is still live are classified against its first_offset and epoch. Objects whose stream is gone from Khepri (a deleted stream, or one whose metadata was wiped) are classified against the per-stream anchor: with the stream gone the anchor is absent, so a GC run reaps them via the no-anchor path rather than requiring manual deletion. See [operations.md](./operations.md#garbage-collection) for details on the GC mechanism and its safety guarantees.
