# Operations

This document covers configuration, monitoring, and debugging of `rabbitmq_stream_s3` for operators and SREs. For the design of the system see [architecture.md](./architecture.md). For what to do when things go wrong see [failure-modes.md](./failure-modes.md).

## Configuration

All configuration goes in `rabbitmq.conf`. Application-environment style configuration is also supported but not documented here.

### Required: bucket and region

```ini
stream_s3.bucket = my-rabbitmq-streams-bucket
stream_s3.region = us-east-1
```

If `stream_s3.region` is not set, the plugin attempts to determine the region automatically via the EC2 instance metadata service.

### Endpoint overrides

Useful for S3-compatible storage or VPC endpoints.

```ini
stream_s3.region_endpoints.us-east-1 = stream_s3.us-east-1.amazonaws.com
```

### Credentials

The plugin resolves AWS credentials in this order:

1. Static credentials from `rabbitmq.conf` (`stream_s3.access_key_id` and `stream_s3.secret_key`).
2. Container credentials endpoint (when `AWS_CONTAINER_CREDENTIALS_FULL_URI` is set).
3. EC2 instance metadata service (IMDSv2).

```ini
# Not recommended for production. Prefer IAM roles.
stream_s3.access_key_id = AKIAIOSFODNN7EXAMPLE
stream_s3.secret_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Continuous Membership Reconciliation (CMR)

Mirrors [Quorum Queue CMR](https://www.rabbitmq.com/docs/quorum-queues#member-reconciliation) for streams.

```ini
# Whether membership should be periodically evaluated.
# Type: boolean. Default: false.
stream_s3.continuous_membership_reconciliation.enabled = true

# The desired size to which stream clusters should grow.
# Type: positive integer. Default: none.
stream_s3.continuous_membership_reconciliation.target_group_size = 3

# Whether to remove 'dangling' members which are no longer
# part of the RabbitMQ cluster.
# Type: boolean. Default: false.
stream_s3.continuous_membership_reconciliation.auto_remove = true

# Interval at which membership is evaluated, in milliseconds.
# Type: positive integer. Default: 360000 (60 minutes).
stream_s3.continuous_membership_reconciliation.interval = 360000

# Delay in milliseconds after which membership is evaluated
# following any trigger event, for example when a node joins the
# RabbitMQ cluster.
# Type: positive integer. Default: 10000 (10 seconds).
stream_s3.continuous_membership_reconciliation.trigger_interval = 10000
```

### Tuning the upload path

```ini
# Target byte size at which the replica reader cuts a fragment.
# Smaller values produce more S3 PUTs and more manifest entries.
# Larger values reduce request rate but slow time-to-S3 for low-throughput
# streams. Default: 64 MiB.
stream_s3.fragment_target_size = 67108864

# Number of applied fragments after which a manifest persist is triggered.
# Lower values shorten the durability window; higher values reduce
# manifest PUT rate. Default: 5.
stream_s3.persist_threshold = 5

# Maximum time between manifest persists, in milliseconds.
# Bounds the persist window when fragments arrive slowly. Default: 2000.
stream_s3.persist_interval_ms = 2000

# Byte-rate ceiling applied by the per-node governor across all streams.
# When unlimited, the governor does not pace transfers. Set to a fraction
# of instance bandwidth on managed deployments to avoid contention with
# other traffic. Default: unlimited.
#
# The ceiling is a long-run average. A single transfer larger than the
# internal burst allowance (a fragment can exceed its target size when a
# large chunk lands at the end) is still admitted, on credit, and the
# governor throttles subsequent transfers to repay it; it never stalls.
# The `governor_oversized_admissions` metric counts such transfers: a
# persistently climbing value means the configured rate is low relative
# to fragment sizes.
stream_s3.max_transfer_bytes_per_sec = unlimited
```

### Read-path integrity

```ini
# Whether to validate CRC32 checksums when reading chunk data from the
# remote tier (S3). When enabled, each chunk read by a consumer from S3
# is verified against the CRC in its header before delivery. This catches
# corruption introduced after upload (bit rot in S3, HTTP response
# corruption not caught by TCP checksums). The cost is one erlang:crc32/1
# call per chunk on the consumer read path.
# Type: boolean. Default: false.
stream_s3.verify_crc_on_read = false
```

Osiris does not validate CRC when sending chunk data to consumers via `send_file` or the chunk iterator. The CRC in each chunk header is validated on the write path (when replicas accept chunks) and during `read_chunk/1`, but not on the streaming read paths used for consumer delivery. This setting adds validation on the remote read path where data has traversed an additional network hop (S3) beyond the original replication.

### Server-side encryption

The plugin sends an `x-amz-server-side-encryption: AES256` header on every PUT request. This is a no-op on default S3 buckets (S3 encrypts all objects with SSE-S3 automatically) but satisfies bucket policies or SCPs that deny uploads without an explicit encryption header.

When the bucket requires SSE-KMS, configure the key ARN:

```ini
# KMS key ARN for server-side encryption. When set, the plugin uses
# SSE-KMS (aws:kms) instead of SSE-S3 (AES256) for all uploads.
# Type: binary. Default: not set (uses AES256).
stream_s3.kms_key_id = arn:aws:kms:us-east-1:123456789012:key/example-key-id
```

For more information on S3 server-side encryption options, see [Protecting data with server-side encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/serv-side-encryption.html).

## Monitoring

The plugin exposes its metrics via Prometheus through the existing `rabbitmq_prometheus` plugin. Enabling `rabbitmq_stream_s3` implicitly enables `rabbitmq_prometheus`, which opens the Prometheus metrics endpoint on port 15692 (TCP) or 15691 (TLS).

### Endpoints

| Endpoint                   | Use it for                                                                         |
|----------------------------|------------------------------------------------------------------------------------|
| `/metrics`                 | Default scrape target. Per-stream gauges are summed into per-node aggregates. Per-stream counters are not summed (this would violate Prometheus monotonicity when streams are deleted); their cluster-wide value is published as a node-level shadow counter with a `module=` label. |
| `/metrics/per-object`      | Per-stream metrics with `queue=` and `vhost=` labels. Cardinality scales with stream count, so scrape less often. |

The plugin's metrics follow the same convention as core RabbitMQ: aggregate metrics on the default endpoint, per-object metrics on the per-object endpoint. Dashboards built against the default endpoint do not break when the cluster grows to many streams.

### Metric prefix and labels

All metrics use the `rabbitmq_stream_s3_` prefix. Per-module metrics carry a `module` label identifying the source. Per-stream metrics on `/metrics/per-object` add `vhost=` and `queue=` labels. The aggregate values on `/metrics` carry no per-stream label.

### Replica reader (per-stream, also aggregated on default endpoint)

Owned by `rabbitmq_stream_s3_replica_reader`. Each writer-node replica reader owns one counter set per stream.

#### Upload counters

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_transfers_completed`               | counter | Fragment uploads that succeeded                                   |
| `rabbitmq_stream_s3_transfers_failed`                  | counter | Fragment uploads that failed for any reason                       |
| `rabbitmq_stream_s3_bytes_transferred`                 | counter | Total payload bytes uploaded                                      |
| `rabbitmq_stream_s3_groups_created`                    | counter | Group manifest objects uploaded                                   |
| `rabbitmq_stream_s3_kilo_groups_created`               | counter | Kilo-group manifest objects uploaded                              |
| `rabbitmq_stream_s3_mega_groups_created`               | counter | Mega-group manifest objects uploaded                              |
| `rabbitmq_stream_s3_roots_created`                     | counter | Root manifest objects uploaded (one per successful persist)       |
| `rabbitmq_stream_s3_transfers_in_flight`               | gauge   | Fragments cut and submitted that have not yet completed           |

#### Pipeline (drain to persist)

These metrics decompose disk-resident bytes by their position in the upload pipeline. See [Investigating disk pressure](#investigating-disk-pressure) for the operational use case.

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_bytes_drained_total`               | counter | Cumulative bytes drained from osiris by the replica reader        |
| `rabbitmq_stream_s3_bytes_persisted_total`             | counter | Cumulative bytes whose fragment manifest update has succeeded     |
| `rabbitmq_stream_s3_bytes_in_assembly`                 | gauge   | Bytes drained but not yet cut into a fragment (stage 1)           |
| `rabbitmq_stream_s3_bytes_in_transfer`                 | gauge   | Bytes in fragments at the governor or being uploaded (stage 2)    |
| `rabbitmq_stream_s3_bytes_in_persist`                  | gauge   | Bytes in uploaded fragments waiting on a manifest persist (stage 3) |

#### Persist counters

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_persists_completed`                | counter | Manifest persists (S3 + Khepri) that succeeded                    |
| `rabbitmq_stream_s3_persists_failed`                   | counter | Manifest persists that failed                                     |
| `rabbitmq_stream_s3_persist_conflicts`                 | counter | Persists rejected by Khepri's optimistic lock (competing writer)  |
| `rabbitmq_stream_s3_last_persist_timestamp_ms`         | gauge   | Wall-clock time of the last successful persist                    |

#### Manifest gauges

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_manifest_first_offset`             | gauge   | Offset of the oldest record in the remote tier                    |
| `rabbitmq_stream_s3_manifest_next_offset`              | gauge   | Next offset to upload to the remote tier                          |
| `rabbitmq_stream_s3_manifest_first_timestamp_ms`       | gauge   | Timestamp of the oldest record (ms since epoch, or 0 when empty)  |
| `rabbitmq_stream_s3_remote_bytes`                      | gauge   | Total bytes of segment data stored in the remote tier             |
| `rabbitmq_stream_s3_remote_messages`                   | gauge   | Approximate record count (next_offset - first_offset)             |

#### Resolution and recovery

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_manifests_resolved`                | counter | Times a non-empty manifest was resolved on startup                |
| `rabbitmq_stream_s3_manifests_resolved_empty`          | counter | Times a genuinely empty manifest was resolved on startup          |
| `rabbitmq_stream_s3_manifest_resolution_failures`      | counter | Resolutions that hit a transient store or object error and were retried instead of resolving empty |
| `rabbitmq_stream_s3_local_log_ahead_recoveries`        | counter | Times the replica reader discarded the remote manifest because the local log was ahead (e.g. retention deleted un-uploaded data) |

#### Retention

| Metric                                  | Type    | Description                                                       |
|-----------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_local_tier_retention_evaluations`      | counter | Local tier retention evaluations                                  |
| `rabbitmq_stream_s3_remote_tier_retention_evaluations`     | counter | Remote tier retention evaluations                                 |
| `rabbitmq_stream_s3_remote_tier_retention_failures`        | counter | Remote tier retention evaluations that failed                     |
| `rabbitmq_stream_s3_fragments_deleted`                     | counter | Fragment objects deleted by remote retention                      |
| `rabbitmq_stream_s3_groups_deleted`                        | counter | Group objects deleted by remote retention                         |
| `rabbitmq_stream_s3_kilo_groups_deleted`                   | counter | Kilo-group objects deleted by remote retention                    |
| `rabbitmq_stream_s3_mega_groups_deleted`                   | counter | Mega-group objects deleted by remote retention                    |

### Governor (per-node)

Owned by `rabbitmq_stream_s3_governor`. Single counter set per node, no per-stream variant.

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_governor_submissions_received`     | counter | Total transfer submissions received                               |
| `rabbitmq_stream_s3_governor_oversized_admissions`     | counter | Transfers admitted on credit because they exceed the burst allowance; a persistently climbing value means the configured rate is low relative to fragment sizes |
| `rabbitmq_stream_s3_governor_tasks_in_flight`          | gauge   | Transfer tasks currently executing                                |
| `rabbitmq_stream_s3_governor_pending_submissions`      | gauge   | Submissions queued waiting for token-bucket capacity              |

### Reaper (per-node)

Owned by `rabbitmq_stream_s3_reaper`.

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_objects_deleted`                   | counter | S3 objects confirmed deleted via the reaper (retention or stream deletion) |
| `rabbitmq_stream_s3_objects_delete_failed`             | counter | Objects the reaper could not confirm deleted (a DeleteObjects partial or whole-request failure); left for orphan GC |
| `rabbitmq_stream_s3_streams_deleted`                   | counter | Streams whose deletion task ran to completion                     |

### Manifest replica (per-node)

Owned by `rabbitmq_stream_s3_manifest_replica`.

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_resyncs_requested`                | counter | Re-syncs a replica requested after a manifest broadcast gap or epoch mismatch |
| `rabbitmq_stream_s3_syncs_rejected`                   | counter | Syncs a replica dropped because they were older than the cached epoch or sequence |

### S3 API (per-node)

Owned by `rabbitmq_stream_s3_api`. One counter set per node.

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_get`                               | counter | Full-object GET requests                                          |
| `rabbitmq_stream_s3_get_range`                         | counter | Range GET requests                                                |
| `rabbitmq_stream_s3_put`                               | counter | PUT requests                                                      |
| `rabbitmq_stream_s3_delete_many`                       | counter | Multi-object DELETE requests                                      |
| `rabbitmq_stream_s3_delete_one`                        | counter | Single-object DELETE requests                                     |
| `rabbitmq_stream_s3_list`                              | counter | LIST requests                                                     |
| `rabbitmq_stream_s3_bytes_received`                    | counter | Total bytes received from S3                                      |
| `rabbitmq_stream_s3_bytes_sent`                        | counter | Total bytes sent to S3                                            |

### S3 API histogram

| Metric                                       | Description                                                       |
|----------------------------------------------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_request_duration_seconds{kind="read"}`      | GET request duration (full-object and range)                      |
| `rabbitmq_stream_s3_request_duration_seconds{kind="write"}`     | PUT/POST/DELETE request duration                                  |

Buckets: 10ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s, +Inf.

### S3 transport (per-node)

Owned by `rabbitmq_stream_s3_api_aws`.

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_active_requests`                   | gauge   | Current in-flight S3 requests                                     |
| `rabbitmq_stream_s3_total_requests`                    | counter | Total S3 API requests (includes internal pagination)              |
| `rabbitmq_stream_s3_response_403`                      | counter | HTTP 403 responses                                                |
| `rabbitmq_stream_s3_response_500`                      | counter | HTTP 500 responses                                                |
| `rabbitmq_stream_s3_response_503`                      | counter | HTTP 503 responses                                                |
| `rabbitmq_stream_s3_request_timeouts`                  | counter | Request timeouts                                                  |

### HTTP connection pools (per-node, per-pool)

Owned by `rabbitmq_stream_s3_api_aws_pool`. Each metric is labeled with `pool` (the pool name).

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_checkouts`                         | counter | Connections checked out                                           |
| `rabbitmq_stream_s3_checkins`                          | counter | Connections returned                                              |
| `rabbitmq_stream_s3_checkout_queued`                   | counter | Checkouts that had to wait                                        |
| `rabbitmq_stream_s3_checkout_cancelled`                | counter | Checkouts cancelled before being satisfied                        |

### Khepri metadata store (per-node)

Owned by `rabbitmq_stream_s3_db`.

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_sproc_triggers`                    | counter | Khepri stored procedure triggers                                  |
| `rabbitmq_stream_s3_gets`                              | counter | Khepri get requests                                               |
| `rabbitmq_stream_s3_puts`                              | counter | Khepri put requests                                               |
| `rabbitmq_stream_s3_put_successes`                     | counter | Successful Khepri puts                                            |
| `rabbitmq_stream_s3_put_conflicts`                     | counter | Khepri put conflicts                                              |
| `rabbitmq_stream_s3_put_not_founds`                    | counter | Khepri put-not-found errors                                       |
| `rabbitmq_stream_s3_put_errors`                        | counter | Khepri put errors                                                 |

### Log reader (per-node)

Owned by `rabbitmq_stream_s3_log_reader`.

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_remote_init`                       | counter | Readers initialized in remote mode                                |
| `rabbitmq_stream_s3_local_init`                        | counter | Readers initialized in local mode                                 |
| `rabbitmq_stream_s3_remote_close`                      | counter | Remote readers closed                                             |
| `rabbitmq_stream_s3_local_close`                       | counter | Local readers closed                                              |
| `rabbitmq_stream_s3_resolve_remote_first`              | counter | Offset specs resolved to remote tier (first)                      |
| `rabbitmq_stream_s3_resolve_remote_next`               | counter | Offset specs resolved to remote tier (next)                       |
| `rabbitmq_stream_s3_resolve_remote_last`               | counter | Offset specs resolved to remote tier (last)                       |
| `rabbitmq_stream_s3_resolve_remote_offset`             | counter | Offset specs resolved to remote tier (absolute offset)            |
| `rabbitmq_stream_s3_resolve_remote_timestamp`          | counter | Offset specs resolved to remote tier (timestamp)                  |
| `rabbitmq_stream_s3_resolve_local`                     | counter | Offset specs resolved to local tier                               |
| `rabbitmq_stream_s3_resolve_duration_ms`               | counter | Total ms spent resolving offset specs                             |
| `rabbitmq_stream_s3_resolve`                           | counter | Number of offset spec resolutions                                 |

### Remote reader (per-node)

Owned by `rabbitmq_stream_s3_remote_reader`. One counter set per node, summed across consumers.

| Metric                              | Type    | Description                                                       |
|-------------------------------------|---------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_buffer_hit`                        | counter | Reads served from prefetch buffer                                 |
| `rabbitmq_stream_s3_buffer_miss`                       | counter | Reads that had to await async data                                |
| `rabbitmq_stream_s3_fragment_transition`               | counter | Number of transitions between fragments                           |
| `rabbitmq_stream_s3_requests_in_flight`                | gauge   | Async requests currently executing                                |
| `rabbitmq_stream_s3_read_duration_ms`                  | counter | Total ms spent in `read/4,5` calls                                |
| `rabbitmq_stream_s3_read`                              | counter | Number of reads                                                   |
| `rabbitmq_stream_s3_remote_reader_total_requests`      | counter | S3 requests initiated                                             |
| `rabbitmq_stream_s3_remote_reader_fatal_errors`        | counter | Remote readers stopped by a non-retryable S3 error                |

### Read size histogram

| Metric                              | Description                                                       |
|-------------------------------------|-------------------------------------------------------------------|
| `rabbitmq_stream_s3_read_size_bytes_bucket`            | Distribution of read sizes from S3                                |

## Dashboards

A reference Grafana dashboard ships with the plugin at
[`grafana/RabbitMQ-Stream-Tiered-Storage.json`](../grafana/RabbitMQ-Stream-Tiered-Storage.json).
The dashboard scrapes the default `/metrics` endpoint and uses the
folded aggregate values so it works regardless of stream count.

### Recommended alerting

The numbers below are starting points. Tune for your traffic patterns.

- **Disk capacity.** Alert externally on the node's filesystem fill ratio (typically via `node_exporter` or the equivalent). When the alert fires, [Investigating disk pressure](#investigating-disk-pressure) walks through the plugin's pipeline gauges to attribute the cause to a specific upload-path stage.
- **Persistent transfer failures.** `rate(rabbitmq_stream_s3_transfers_failed[5m]) > 0` for more than ten minutes. Sustained failures usually mean S3 connectivity, throttling, or credentials problems.
- **Persist conflicts.** `rate(rabbitmq_stream_s3_persist_conflicts[5m]) > 0`. A non-zero rate indicates competing writers, likely from a partition or a struggling leader election.
- **Stuck uploads.** `rabbitmq_stream_s3_transfers_in_flight` non-zero and `rate(rabbitmq_stream_s3_transfers_completed[5m]) == 0` together. Drain loop is producing fragments but the governor is not flushing them.
- **Local log ahead recoveries.** `rate(rabbitmq_stream_s3_local_log_ahead_recoveries[1h]) > 0` for any stream. Indicates local retention is racing ahead of uploads. See [failure-modes.md](./failure-modes.md).
- **Khepri conflicts.** `rate(rabbitmq_stream_s3_put_conflicts[5m]) > 0` matches `rabbitmq_stream_s3_persist_conflicts` above and surfaces the same problem from the metadata store side.
- **S3 errors.** `rate(rabbitmq_stream_s3_response_500[5m]) > 0` or `rate(rabbitmq_stream_s3_response_503[5m]) > 0`. The plugin retries automatically; sustained high rates indicate an S3-side incident.

The pipeline gauges (`rabbitmq_stream_s3_bytes_in_assembly`, `rabbitmq_stream_s3_bytes_in_transfer`, `rabbitmq_stream_s3_bytes_in_persist`) are diagnostic rather than alerting metrics. Their absolute values depend on stream count and fragment target size, so static thresholds do not generalise across deployments. Use them after a disk capacity or transfer-failure alert fires to identify which streams contribute most and which pipeline stage is stuck.

## Capacity planning

### S3 request rates

S3 supports at least 3,500 PUT/POST/DELETE and 5,500 GET requests per second per partitioned prefix and scales automatically under sustained load. Each fragment is one PUT, so even very high stream throughput produces few PUTs per second. The realistic concern is GETs from many consumers reading old data on the same stream simultaneously. S3's automatic scaling handles sustained load but a sudden burst on a previously idle stream may see brief throttling.

The AIMD prefetch in the remote reader naturally grows toward larger reads for fast consumers, which reduces request rate per consumer.

### Local disk and page cache

Osiris does not flush writes to disk. Recently-written data is served from page cache. The upload path reads bytes the writer just wrote, which is almost always page-cache-hot. The practical bottleneck is memory pressure, not disk throughput. Under memory pressure the OS evicts dirty pages and both the write path and the upload path slow down.

[Linux Pressure Stall Information (PSI)](https://docs.kernel.org/accounting/psi.html) for memory is the right signal here. `/proc/pressure/memory` reports when processes are stalling because the kernel is under memory pressure.

- `some > 0`: at least one task is stalling on memory; page cache is being evicted; upload and read latency may increase.
- `full > 0`: all tasks are stalling; severe degradation.

PSI is more actionable than "free memory" (which is always near zero on a healthy Linux system because the kernel uses available memory for page cache). PSI only fires when there is actual contention.

When PSI signals memory pressure, investigate what is consuming memory (BEAM heap, ETS, other processes), increase instance memory, or reduce streams per node.

### Local retention and segment size

Retention always keeps at least one segment locally. Operators who want more data kept locally should increase the segment size (via policy or `x-args`), not max-bytes. A larger segment retains more data before the plugin's `{'fun', ...}` retention spec can reclaim it.

Aggressive local eviction also enables fast replica addition: when local data is minimal, adding a new replica only requires replicating the recent window (roughly one segment) rather than the full stream history. The new replica can serve old reads from S3 immediately.

## Investigating disk pressure

Local disk filling up is the outer signal that something is wrong in tiered storage. The disk capacity alert fires from infrastructure monitoring, not from this plugin. Once it fires, the operator needs to answer "why is disk filling up?" within minutes. This section walks through the answer.

### The shape of the problem

Local disk holds three kinds of bytes for tiered streams:

1. Bytes the writer has produced but the replica reader has not yet drained.
2. Bytes drained but not yet safely in S3 plus a manifest update. These are "in flight" through the upload pipeline.
3. Bytes that exist in both tiers, uploaded and persisted, awaiting local retention.

Categories 1 and 2 are at risk if the disk fills further or the cluster fails before the bytes reach S3. Category 3 is safely uploaded data that local retention will reclaim shortly.

Healthy operation keeps category 3 small because retention runs frequently. Categories 1 and 2 are also small because the pipeline keeps up with the write rate. When disk fills up, almost always the cause is categories 1 or 2 growing.

### The metric set

Per stream:

- `rabbitmq_stream_s3_bytes_drained_total` counter: cumulative bytes the replica reader has consumed from osiris.
- `rabbitmq_stream_s3_bytes_persisted_total` counter: cumulative bytes whose fragment manifest update has succeeded.
- `rabbitmq_stream_s3_bytes_in_assembly` gauge: bytes drained but not yet cut into a fragment.
- `rabbitmq_stream_s3_bytes_in_transfer` gauge: bytes in fragments waiting at the governor or being uploaded to S3.
- `rabbitmq_stream_s3_bytes_in_persist` gauge: bytes whose fragments are uploaded but whose manifest update has not yet succeeded.

Same metric names also appear with no `queue` label on `/metrics`, summed across streams (gauges) or maintained as node-level cumulative shadows (counters).

The total bytes in pipeline at any moment equals `rabbitmq_stream_s3_bytes_in_assembly + rabbitmq_stream_s3_bytes_in_transfer + rabbitmq_stream_s3_bytes_in_persist`. These are the bytes on this node that S3 does not yet have a confirmed copy of.

### Investigation walkthrough

Step 1: identify the streams contributing most. Query `topk(10, rabbitmq_stream_s3_bytes_in_assembly + rabbitmq_stream_s3_bytes_in_transfer + rabbitmq_stream_s3_bytes_in_persist)` on `/metrics/per-object`. This ranks streams by pipeline backlog. Streams at the top are where to focus.

Step 2: identify which pipeline stage is stuck. For each of the top streams, look at the three gauges.

- `rabbitmq_stream_s3_bytes_in_assembly` elevated means chunks are being drained but fragments are not being cut. Likely causes: the replica reader process is starved or blocked, the `stream_s3.fragment_target_size` is too high relative to the stream's chunk rate, or assembly is waiting on a slow drain loop.
- `rabbitmq_stream_s3_bytes_in_transfer` elevated means fragments are sitting at the governor or in flight to S3. Cross-check `rabbitmq_stream_s3_governor_pending_submissions`, `rabbitmq_stream_s3_transfers_failed`, `rabbitmq_stream_s3_response_500`, `rabbitmq_stream_s3_response_503`, and `rabbitmq_stream_s3_request_duration_seconds` quantiles. The typical cause is S3 throttling, an S3 outage, or a too-low governor rate limit.
- `rabbitmq_stream_s3_bytes_in_persist` elevated means the upload to S3 succeeded but the manifest update did not. Cross-check `rabbitmq_stream_s3_persist_conflicts` and `rabbitmq_stream_s3_put_conflicts`. The typical cause is Khepri unavailability, partition-induced manifest contention, or a struggling leader election.

If all three gauges are small and `rate(rabbitmq_stream_s3_bytes_drained_total[5m])` is well below the writer's chunk-write rate, the replica reader is behind the writer. Unusual. Check that the replica reader process is alive and that osiris is delivering offset notifications.

If all three gauges are small and the drain rate looks healthy, retention has stopped for some reason. Check that `rabbitmq_stream_s3_local_tier_retention_evaluations` is incrementing on the affected streams.

### Why pipeline gauges are the right diagnostic

The alertable signal is disk capacity, observed externally. The plugin cannot prevent the alert, only explain it. The pipeline gauges decompose the explanation by attributing each backlogged byte to a specific stage of the upload path. Each stage has a distinct failure mode and a distinct mitigation, which is what the operator on call needs.

A single `rabbitmq_stream_s3_archival_lag_bytes` metric would not give this decomposition. Knowing "this stream has X bytes behind" does not tell you whether X is sitting in S3 retries, manifest contention, or normal drain latency. The three-stage decomposition pinpoints the failure within seconds.

## Debugging

### CLI commands

The plugin adds commands to `rabbitmq-streams` for debugging the upload pipeline.

**Stream S3 status** shows the current state of the tiered storage replica reader for a stream: manifest offsets, pipeline state, assembly progress, and whether persist/rebalance/retention operations are in flight.

```bash
rabbitmq-streams stream_s3_status my-stream --vhost /
# Status of tiered storage for stream my-stream in vhost / ...
# stream:	<<"__my-stream_1781016420026141262">>
# fragment_target_size:	67108864
# log_next_offset:	1421556
# manifest_first_offset:	0
# manifest_next_offset:	1317635
# manifest_total_size:	1339723340
# persisted_next_offset:	1317635
# transfers_in_flight:	1
# transfers_pending_order:	0
# since_persist:	0
# persist_in_flight:	false
# rebalance_in_flight:	false
# retention_in_flight:	false
# waiters:	0
# assembly_payload_size:	38043104
# assembly_target_size:	67108864
# assembly_num_chunks:	6661
# assembly_cut:	false
```

> [!TIP]
> Use `--formatter=json` for machine-readable output: `rabbitmq-streams stream_s3_status my-stream --formatter=json`

**Evaluate local retention** triggers the local retention job for a stream. Useful to determine whether retention is correctly reclaiming segments after upload.

```bash
rabbitmq-streams evaluate_local_retention my-stream --vhost /
```

**Evaluate remote retention** triggers the remote retention job for a stream. Useful when the retention policy has changed and you want immediate effect, or to verify that remote retention is not stuck.

```bash
rabbitmq-streams evaluate_remote_retention my-stream --vhost /
```

**Force fragment cut** forces the current in-progress fragment to cut and upload immediately, regardless of whether it has reached the target size. Useful to verify that the upload pipeline works end-to-end without waiting for data to accumulate.

```bash
rabbitmq-streams force_fragment_cut my-stream --vhost /
```

**Garbage collection** identifies (and optionally deletes) dangling S3 objects that are not referenced by any manifest. Defaults to `dry_run` mode which reports orphans without deleting them. Pass `--mode delete` to actually remove the objects.

```bash
rabbitmq-streams stream_s3_gc
rabbitmq-streams stream_s3_gc --formatter json
rabbitmq-streams stream_s3_gc --mode delete
```

Pass a stream name to restrict GC to a single stream, which lists only that stream's S3 prefix instead of sweeping every stream:

```bash
rabbitmq-streams stream_s3_gc my-stream --vhost /
rabbitmq-streams stream_s3_gc my-stream --mode delete
```

See [Garbage collection](#garbage-collection) below for the mechanism and safety guarantees.

### Inspect a replica reader

```erlang
%% On the writer node for a stream:
StreamId = <<"__stream-01_1745252105682952932">>,
Pid = rabbitmq_stream_s3_registry:whereis_name({StreamId, node()}),
sys:get_state(Pid).
```

`format_status` returns the public-safe view: stream id, fragment target, manifest next offset, log next offset, and the in-progress assembly state.

### Inspect counters from a node-local shell

```erlang
%% Per-node counter sets:
seshat:counters(rabbitmq_stream_s3, rabbitmq_stream_s3_governor).
seshat:counters(rabbitmq_stream_s3, rabbitmq_stream_s3_reaper).

%% All metrics with labels:
seshat:format(rabbitmq_stream_s3, #{labels => as_map}).
```

### Find the writer node for a stream

```erlang
{ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(<<"/">>, queue, <<"my-stream">>)),
maps:get(leader_node, amqqueue:get_type_state(Q)).
```

### Trace a slow upload

If `rabbitmq_stream_s3_transfers_in_flight` is non-zero on a stream and `rabbitmq_stream_s3_transfers_completed` is not advancing:

1. Check `rabbitmq_stream_s3_governor_pending_submissions` on the writer node. Non-zero indicates token-bucket starvation.
2. Check `rabbitmq_stream_s3_active_requests` and `rabbitmq_stream_s3_request_duration_seconds` histograms. Long-tailed write durations indicate S3-side latency.
3. Check `rabbitmq_stream_s3_response_503` rate. Sustained 503s indicate throttling.

### Correlate per-stream metrics

Per-stream metrics live behind `/metrics/per-object`. Scrape this endpoint for diagnostics, not for routine monitoring.

```bash
curl http://node:15692/metrics/per-object | grep 'queue="my-stream"'
```

## Garbage collection

Objects in S3 can become orphaned (not referenced by any manifest) in several scenarios: a deposed writer uploads a fragment but loses the Khepri race, a manifest persist fails or the process crashes before completion, or retention deletes entries from the manifest but the corresponding object deletion does not complete.

The GC mechanism identifies these orphans by listing S3 objects and comparing their keys against current authoritative state. It is invoked on demand via the CLI.

### Safety guarantee: monotonicity

The correctness of GC depends on a single structural property: the values used as barriers (`first_offset`, `epoch`) only ever increase. This makes false positives (deleting a live object) structurally impossible, regardless of timing or consistency:

- `first_offset` advances monotonically as retention removes data from the front of the manifest. An object whose offset is below first_offset was already removed by retention. It can never become live again.
- `epoch` advances monotonically with each new writer. A manifest root whose epoch is below the current epoch belongs to a deposed writer. The new writer's manifest is authoritative and the old root can never become active again.

Eventually-consistent reads (from Khepri or the manifest replica ETS cache) can only return stale values that are lower than or equal to the true current value. A stale barrier is more conservative: it classifies fewer objects as garbage. The GC may miss orphans on a given run (false negatives) but can never delete live objects (false positives).

### Classifying garbage

| Object type   | Key format                                                  | Condition for "garbage"
|---            |---                                                          |---
| Fragment      | `rabbitmq/stream/<id>/data/<offset>.<uid>.fragment`         | offset < manifest first_offset
| Group         | `rabbitmq/stream/<id>/metadata/<offset>.<uid>.<kind>`       | offset < manifest first_offset
| Manifest root | `rabbitmq/stream/<id>/metadata/root.<epoch>.<uid>.manifest` | epoch < current epoch according to Khepri

Note that objects belonging to an unknown stream ID are not currently considered garbage. Handling this case is planned.

### Modes

- `dry_run` (default): lists S3 objects, classifies orphans, logs each finding at info level, returns the list. No deletions.
- `delete`: same as dry_run, but also sends each orphan's key to the reaper for batched deletion as it is discovered.

### Output

With `--formatter json`, the output is a JSON array of objects:

```json
[
  {"stream_id": "...", "key": "rabbitmq/stream/.../00000000000000000042.abcd1234.fragment", "reason": "below_first_offset"},
  {"stream_id": "...", "key": "rabbitmq/stream/.../metadata/root.1.deadbeef.manifest", "reason": "stale_epoch"}
]
```

Without `--formatter json`, the output is tab-separated with a summary line.

## Cost considerations

Tiered storage trades local disk for remote object storage. The primary cost factor is how long data is retained in S3. Read-heavy workloads against old data generate additional GET request traffic. Publishing throughput determines how frequently data is uploaded.

For pricing estimates, use the official [AWS S3 pricing calculator](https://calculator.aws/) with the relevant region and access pattern. The plugin does not expose cost-estimation tooling.
