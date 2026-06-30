# Troubleshooting

This guide covers symptoms that look like plugin problems but originate outside the plugin: host network limits, instance sizing, and other environmental constraints. For the plugin's own self-healing scenarios (S3 outage, leader election, upload lag, persist conflicts), see [failure-modes.md](./failure-modes.md). For metric definitions, see [operations.md](./operations.md#monitoring).

The diagnostic principle throughout: before concluding the plugin is at fault, confirm the upload path actually has the resources it needs. The governor, the connection pool, and the request path can all be healthy while the host starves them of bandwidth.

---

## Upload throughput cliffs after a sustained burst

**Symptom.** A high-throughput stream uploads to S3 at a high rate for several minutes, then S3 upload bandwidth drops sharply to a lower, dead-flat floor and stays there. Publishing slows in lockstep, leader memory climbs, and consumers that were keeping up begin reading from the remote tier. The drop is abrupt (one or two scrape intervals), not gradual, and the post-drop rate is suspiciously constant.

**Trigger.** The writer node's network egress allowance is exhausted. On burstable-network EC2 instances (for example `m7g.large`: up to ~12.5 Gbps burst, but only ~0.78 Gbps sustained baseline), the instance sustains a high egress rate only until its network credit bucket drains, then is shaped down to the sustained baseline.

This bites the upload path specifically because **all S3 uploads for a stream happen on the leader** (the writer is the only node that tiers to S3; replicas upload nothing). The leader's NIC egress is shared three ways under load:

- ingest from producers,
- replication to the replicas (roughly `2 x` ingress for a three-node cluster),
- S3 upload.

At a high publish rate the replication traffic alone can dominate egress, so the leader hits its network ceiling well before any single consumer of that bandwidth looks saturated. When the credit bucket drains, every consumer of egress drops together and S3 upload falls to its proportional share of the baseline.

**Why it cascades.** Once upload bandwidth drops below the publish rate, uploads can no longer drain the local tier. The local log grows, leader memory climbs, and the stream backpressures publishers. As publishers slow and consumers fall behind the shrinking local window, consumers start reading the **remote tier** (S3), so read traffic now contends with upload traffic on the same saturated NIC. The result is a self-reinforcing slowdown that can look like a plugin throughput regression.

**Detection.** The plugin metrics rule the plugin out rather than in. Across the cliff, all of the following hold, which is inconsistent with a plugin-side bottleneck:

- `rabbitmq_stream_s3_response_503`, `_response_500`, and `_request_timeouts` stay at zero (so this is not S3-side throttling, which would return `503 SlowDown`).
- `rabbitmq_stream_s3_governor_pending_submissions` and `_governor_oversized_admissions` stay at zero, and the governor is `unlimited` (so the rate limiter is not pacing transfers).
- `rabbitmq_stream_s3_transfers_in_flight`, `_governor_tasks_in_flight`, and `_active_requests` are unchanged across the drop (so upload concurrency did not collapse).
- the connection pool has idle capacity (checkouts roughly equal checkins, and `_checkout_queued` is not climbing).

Meanwhile the host-level signals confirm the network ceiling. On the leader node:

```bash
# ENA driver's own tally of egress shaping. A large and climbing value during
# the cliff, with the other allowances at zero, means a pure bandwidth ceiling.
ethtool -S ens5 | grep -E 'allowance_exceeded'
#   bw_out_allowance_exceeded:   <large, climbing while throttled>
#   bw_in_allowance_exceeded:    <smaller>
#   pps_allowance_exceeded:      0      <- not packet-rate limited
#   conntrack_allowance_exceeded:0      <- not connection-tracking limited
```

If the run is driven through Prometheus with `node_exporter`, the total NIC egress makes the cliff and its floor obvious:

```promql
rate(node_network_transmit_bytes_total{device="ens5"}[1m])
```

On the leader this shows a high, flat plateau followed by a cliff to a dead-flat floor equal to the instance's sustained network baseline. The replica nodes show near-zero S3 upload egress throughout, confirming that uploads are leader-only.

**Resolution.** This is a host capacity constraint, not a plugin fault. Options, depending on whether the goal is production sizing or a clean plugin benchmark:

- **Size the instance for sustained egress.** Use a non-burstable or network-optimized instance type (for example `m7gn.*`, or a larger size such as `.4xlarge` and up) whose sustained network baseline holds the required egress indefinitely. Account for replication: the leader's egress is roughly `ingress x (replica_count + 1)` plus upload, so size for the leader's total, not the publish rate alone.
- **Pace the upload path.** Set `stream_s3.max_transfer_bytes_per_sec` to a fraction of the instance's sustained bandwidth so uploads coexist with replication and client traffic without driving the NIC into shaping. This trades peak upload rate for predictability.
- **Stay within baseline when benchmarking the plugin.** Cap the producer rate below the egress baseline so the measurement reflects plugin behavior rather than the NIC.

**Prevention.** When capacity planning, treat the writer node's network egress as the shared, contended resource it is, and confirm the chosen instance type can sustain (not just burst to) the required leader egress. See [operations.md](./operations.md#capacity-planning) for request-rate and disk planning, and the "Slow replica reader (upload lag)" entry in [failure-modes.md](./failure-modes.md) for the plugin-side view of upload lag.

## Nothing is tiered to the remote tier

**Symptom.** Streams work normally but no data appears in the bucket. The upload-path metrics (`rabbitmq_stream_s3_transfers_completed`, `rabbitmq_stream_s3_bytes_transferred`) stay flat, or `rabbitmq_stream_s3_transfers_failed` climbs steadily.

**Trigger.** The configured bucket does not exist, or the node's credentials cannot access it. A wrong or unreachable bucket does not stop a stream: it keeps running on local disk, and uploads fail and retry indefinitely. The condition is therefore nearly silent without monitoring.

**Diagnosis.** Check the bucket accessibility signal:

- The `rabbitmq_stream_s3_bucket_accessible` metric is `0`.
- An `ERROR` log states whether the bucket does not exist or access was denied.
- `rabbitmq-streams stream_s3_status <stream>` reports `Accessible: no (<reason>)` in its "Remote tier bucket" section.

A `403` (access denied) also increments `rabbitmq_stream_s3_response_403`. A nonexistent bucket can itself report `access denied` rather than `does not exist` when the credentials lack `s3:ListBucket` on it, so treat either reason as "the configured bucket is not usable" and check both the bucket name and the IAM permissions.

**Resolution.** Correct `stream_s3.bucket` (and `stream_s3.region`) in `rabbitmq.conf`, or grant the node's IAM role the required S3 permissions on the bucket. The monitor re-probes on its interval (`stream_s3.bucket_check.interval`, default 5 minutes) and clears the signal with an `INFO` log once access is restored; tiering resumes automatically from where it stalled.
