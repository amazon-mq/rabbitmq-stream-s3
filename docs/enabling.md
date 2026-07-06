# Enabling tiered storage

This document is for operators turning on `rabbitmq_stream_s3` for the first time, on a new cluster or one with existing streams. For day-to-day configuration and monitoring see [operations.md](./operations.md). For an end-user view of what tiered storage does see [user-guide.md](./user-guide.md).

## Prerequisites

- A RabbitMQ cluster running a build that includes this plugin. The plugin currently requires specific development branches of `rabbitmq-server` and `osiris`; see the project [README](../README.md) for the current branch names.
- An S3 bucket the cluster's IAM identity can access. The plugin requires four S3 actions, split across two resource scopes:
  - object-level actions on `arn:aws:s3:::<bucket>/*`: `s3:PutObject` (upload fragments and manifests), `s3:GetObject` (read them back), and `s3:DeleteObject` (trim and retention)
  - a bucket-level action on `arn:aws:s3:::<bucket>`: `s3:ListBucket`, used only by the bucket accessibility probe (a `HeadBucket` request); the upload and read paths do not need it, so a policy scoped to only the object actions works for tiering but makes the accessibility check report a false `access denied`
  - See [operations.md](./operations.md#iam-permissions) for a minimal policy example
- The Prometheus metrics endpoint is opened automatically because `rabbitmq_stream_s3` depends on `rabbitmq_prometheus`. Enabling the tiered-storage plugin **implicitly enables** `rabbitmq_prometheus`, which listens on port 15692.

## Configuration

Set the bucket and region in `rabbitmq.conf` on every node:

```ini
stream_s3.bucket = my-rabbitmq-streams-bucket
stream_s3.region = us-east-1
```

This is the minimum required configuration. The plugin attempts EC2 instance metadata when the region is omitted, but explicit configuration is more predictable.

For credentials, see [operations.md](./operations.md#credentials). On EC2 with an IAM role attached, no credentials configuration is needed.

## Enabling the plugin

```bash
rabbitmq-plugins enable rabbitmq_stream_s3
```

Run this on every node. After the plugin is enabled:

- Existing streams continue to work, with uploads beginning from their current write position (see below).
- New streams created from this point upload to S3 transparently.
- Consumers reading old data fall back to S3 for offsets that no longer exist locally.
- The Prometheus endpoint at `/metrics` exposes the new counters described in [operations.md](./operations.md#monitoring).

The plugin runs entirely in the background. The local write path is unaffected. Publish latency does not change.

## What happens to existing streams

When the plugin starts on a node that already has running streams, it discovers each stream and attaches a replica reader on the writer. The replica reader:

1. Resolves the manifest. There is none, so it starts from an empty manifest (`next_offset = 0`).
2. Opens a data reader at offset 0, the beginning of the local log.
3. Begins draining committed chunks and uploading fragments.

**Existing data on local disk is uploaded.** The replica reader walks the local log from offset 0 and produces fragments as if those chunks had just been written. After enabling on a stream that has, say, 10 GiB on disk, expect a burst of upload activity until the replica reader catches up to the writer's current position.

The only data that is not recovered is data already deleted by local retention before the plugin was enabled. If retention has reclaimed the start of the log, the replica reader detects this on its first read attempt (`{offset_out_of_range, {LocalFirst, _}}` from osiris), accepts the gap, and resumes from the oldest offset still on disk. The metric `rabbitmq_stream_s3_local_log_ahead_recoveries` increments in that case.

If you need a clean baseline with no uploaded backlog, create new streams after enabling the plugin. Existing streams will progressively migrate as the replica reader works through their local data.

## What happens on disable

```bash
rabbitmq-plugins disable rabbitmq_stream_s3
```

Disabling the plugin:

- Stops all replica readers.
- Removes the osiris hooks, so newly created writers do not attempt to upload.
- Leaves all S3 objects in place. The remote tier is preserved.
- Leaves Khepri state for the plugin in place. Re-enabling resumes from the same manifest UIDs.

After disable, streams behave as vanilla streams: only local retention applies, and consumers reading offsets that no longer exist locally see the standard `offset out of range` behavior.

Re-enabling after a disable picks up where uploading left off. The replica reader resolves the (already-existing) manifest from S3 and resumes reading from its `next_offset`. If local retention deleted data the plugin had not uploaded, the replica reader accepts the gap and jumps forward.

## Interaction with existing retention policies

The plugin extends but does not override the user's configured retention. Two retentions run independently:

- **User-configured retention (`max-age`, `max-bytes`, neither, or both).** Continues to apply. If you have `max-bytes = 5 GiB` set today, that policy keeps applying after enabling the plugin. Local segments are reclaimed at 5 GiB regardless of upload progress.
- **The plugin's `{'fun', ...}` retention spec.** Adds a second condition: segments whose data has been fully uploaded to S3 can be deleted. This is what makes local disk a sliding window over the full stream rather than a hard cap.

Effects:

- A stream with no user-configured retention previously grew unbounded. After enabling the plugin, local disk only holds recent data and the rest goes to S3. Operationally bounded without you setting any policy.
- A stream with `max-bytes = 5 GiB` and steady throughput now keeps roughly 5 GiB locally **and** the rest in S3. Existing applications that depend on the 5 GiB cap behave the same; they just have more data accessible.
- A stream with `max-bytes` set very low can race ahead of uploads during S3 outages. The remote tier may end up with gaps. See [failure-modes.md](./failure-modes.md) for details.

If you want to keep more data on local disk, increase the segment size (via policy or `x-args`) rather than `max-bytes`. The plugin's reclaim spec releases segments only after they are fully uploaded; a larger segment retains more data locally. See [operations.md](./operations.md#local-retention-and-segment-size).

## Verifying the plugin is working

After enabling and publishing some traffic:

```bash
# Scrape the metrics endpoint.
curl -s http://localhost:15692/metrics | grep '^rabbitmq_stream_s3_'
```

Expected, with non-zero values once a fragment has been uploaded:

```
rabbitmq_stream_s3_transfers_completed{...} N
rabbitmq_stream_s3_persists_completed{...} M
rabbitmq_stream_s3_remote_bytes{...} B
```

You can also list S3 keys for a specific stream prefix to confirm objects are being created:

```bash
aws s3 ls "s3://${BUCKET}/rabbitmq/stream/" --recursive | head
```

For more diagnostic recipes (per-stream metrics, inspecting a replica reader, tracing slow uploads) see [operations.md](./operations.md#debugging).

## Multi-cluster considerations

Each cluster should use its own bucket or its own bucket prefix. The plugin uses `rabbitmq/stream/<StreamId>/...` as its key prefix and does not isolate by cluster. Sharing a bucket across clusters is safe only if no two clusters can ever produce the same stream ID, which is not guaranteed.

## Disaster recovery posture

Once a fragment is referenced by a persisted manifest, it is durable in S3 independent of the local cluster. If the cluster is wiped and rebuilt, the data uploaded so far is recoverable from the bucket. Recovery tooling for reading the remote tier without the originating cluster is not yet built.

The window of vulnerability is between commit (replicated to a majority of page caches) and persist (manifest written to S3). For the design tradeoff and what "committed" actually means in streams, see [concepts.md](./concepts.md) and [failure-modes.md](./failure-modes.md).
