# Tiered Storage for RabbitMQ Streams

This page explains what tiered storage does from a user's perspective. It covers behavior and limitations without going into implementation details.

For how it works under the hood, see [concepts.md](./concepts.md).

## What it does

RabbitMQ streams store data on local disk. Retention policies delete old data to bound disk usage. Once deleted, that data is gone.

With tiered storage, old data is copied to Amazon S3 in the background before it is deleted locally. Consumers can read arbitrarily far back into the stream. Recent data is served from local disk. Older data is served from S3. The transition is transparent to clients.

No client-side changes are needed. The same consumer code works whether data comes from disk or S3.

## Consumer experience

When a consumer subscribes, it specifies where to start reading: the beginning of the stream, the end, a specific offset, or a timestamp. The broker resolves this to a position in the stream. This resolution step happens regardless of whether the data is local or remote, and is fast even over very large data sets.

Once positioned, the consumer reads forward through the stream.

**Reading from local disk** has the same performance as a stream without tiered storage.

**Reading from S3** is served by a prefetch mechanism that adapts to the consumer's read rate. When the consumer reads quickly, the broker fetches larger chunks of data ahead of time. When the consumer slows down, prefetch sizes shrink. This keeps throughput high without wasting memory or bandwidth.

**Transition from remote to local** happens automatically. As the consumer catches up to data that still exists on local disk, reading switches over without interruption.

**Slow consumers.** If a consumer reads slower than data is being published and local retention is deleting old data, the consumer's position can fall behind what is available locally. In this case, the consumer transitions from local reads to S3 reads transparently. It continues reading without interruption.

## Retention

There are two retention domains:

**Local retention** determines when data is deleted from disk. The plugin reclaims local data once it has been uploaded to S3. This protects the local disk from unbounded growth. At least one segment of data is always kept locally.

**Remote retention** controls how far back consumers can read from S3. It uses the same max-age and max-bytes rules applied to data in S3. If remote retention is not configured, data in S3 is kept indefinitely.

A consumer can read any data that exists in either tier. The oldest readable position is determined by remote retention (or the beginning of the stream if no remote retention is set).

## Gaps

In rare cases, the remote tier may have a gap: a range of data that does not exist in either tier. This can happen if local retention deletes data before it was uploaded to S3 (for example, during an extended S3 outage).

If a consumer requests a position that falls in a gap, it is repositioned at the oldest available data after the gap. This is the same behavior as requesting data that has been deleted by retention in a regular stream.

## Failover

When a leader election occurs (node failure, maintenance), there is a brief pause in delivery while the new leader starts. Committed data is preserved. The consumer resumes from where it left off.

During failover, a consumer reading from S3 may need to re-resolve its position, adding a small amount of latency to the first delivery after the election. Subsequent deliveries resume at full throughput.

## Cost considerations

Tiered storage trades local disk for remote object storage. The primary cost factor is how long data is retained in S3: longer retention means more stored data. Read-heavy workloads against old data generate additional request traffic to S3. Publishing throughput determines how frequently data is uploaded.

Remote retention is the main lever for controlling storage cost. Consumers reading from local disk do not interact with S3.

## Quick answers

**Will my consumer be slower reading old data?**
Only the initial positioning adds latency. Once streaming, throughput adapts to match your read rate via prefetch.

**What happens if S3 goes down?**
Publishing continues normally. Consumers reading recent data are unaffected. Consumers reading old data from S3 pause until connectivity returns.

**Can there be gaps in my stream?**
Rarely. Only if local retention deletes data before it was uploaded (e.g. during an extended S3 outage). Consumers are repositioned past the gap automatically.

**What happens during a failover?**
Brief pause in delivery. Committed data is preserved. The consumer resumes from where it left off.

**Does tiered storage affect publish latency?**
No. Uploads happen in a background process. The write path is entirely local.
