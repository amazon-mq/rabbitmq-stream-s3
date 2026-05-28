# Overview

`rabbitmq_stream_s3` adds a remote storage tier to RabbitMQ streams using Amazon S3. Committed stream data is uploaded to S3 in the background. Consumers read from local disk when data is available there and transparently fall back to S3 for older data.

## Why

Tiered storage makes "how much data should I keep?" purely a question of time. Keep 7 days. Keep 30 days. Keep everything. The system manages where data lives. Local disk holds recent data for low-latency reads. Older data lives in S3 at negligible cost. Consumers read the full window without knowing or caring which tier serves each byte.

Without tiered storage, that question is tangled with capacity planning. How much disk can you afford? How much throughput do you expect? A stream producing 1 MB/s during off-hours and 100 MB/s during a traffic spike needs wildly different disk to cover the same time window. Any fixed byte limit is wrong half the time. Tiered storage removes the coupling between "how far back can I read?" and "how much disk do I need?"

### How it achieves this

Osiris stores stream data in segment files on local disk. Retention policies delete old segments to bound disk usage. Once a segment is deleted, any consumer that hasn't read it loses access permanently.

This plugin makes local disk a cache over S3. Streams retain data in S3 indefinitely (or according to a remote retention policy) at low cost, while local disk only holds recent data. Consumers see a single continuous stream regardless of where the data lives.

```
  ◄──── remote tier (S3) ────►◄──── local tier (disk) ────►

  ┌──────────────────────────────┬──────────────────────────┐
  │  old data in S3              │  recent data on disk     │
  └──────────────────────────────┴──────────────────────────┘
  ▲                               ▲                         ▲
  oldest readable              overlap                 newest
```

## How it works

The plugin hooks into Osiris at two points:

**Write path.** A per-stream remote replica reader process on the writer node reads committed chunks from the local log, assembles them into fragments (typically 64 MiB), and uploads each fragment to S3 as a single PUT. A fragment is cut when it reaches the size target. The architecture supports time-based cuts for low-throughput streams but this is not implemented currently. After upload, the remote replica reader updates the manifest and notifies replica nodes.

**Read path.** When a consumer requests an offset or timestamp that no longer exists locally, the log reader resolves the manifest to find the correct fragment and byte position, then streams data from S3 using HTTP range requests. A per-consumer remote reader process prefetches data to approach local-tier throughput.

## Key properties

- The local log is authoritative. If the local timeline diverges from what is in S3 (epoch change, data directory reset), the remote tier is truncated to match. Availability of the local write path is never sacrificed for remote tier consistency.
- The write path never blocks on S3. Uploads happen asynchronously. S3 outages do not affect publishing or replication.
- Local retention runs independently. If local retention deletes data before it is uploaded, the remote tier has a gap. Consumers are repositioned at the oldest available offset, same as vanilla streaming retention.
- The manifest uses optimistic concurrency control via Khepri. Competing writers (during leader elections) cannot corrupt each other's uploads because fragment keys include random UIDs.
- No new Osiris behaviours are required on the write path. The plugin integrates via lightweight init hooks on the writer and acceptor paths. The read path uses the `osiris_log_reader` behaviour.

## Reading guide

These docs are structured for layered learning. Pick a track based on what you need.

### User track

Understand the behavior and guarantees of tiered storage without implementation details.

1. [user-guide.md](./user-guide.md): what you observe, what is guaranteed, what drives cost
2. [concepts.md](./concepts.md) (optional): how it works under the hood

### Operator track

Understand enough to configure, monitor, and troubleshoot the plugin.

1. This overview (you're here)
2. [concepts.md](./concepts.md): skim for mental model of streams and tiered storage
3. [enabling.md](./enabling.md): turning the plugin on for new and existing clusters
4. [operations.md](./operations.md): configuration, metrics, debugging
5. [failure-modes.md](./failure-modes.md): what breaks, what the system does, what you do

### Developer track

Understand enough to modify the plugin or diagnose issues in the code.

1. This overview
2. [DEVELOPMENT.md](./DEVELOPMENT.md): building, running tests, formatting
3. [concepts.md](./concepts.md): read fully, builds the mental model the code assumes
4. [architecture.md](./architecture.md): supervision tree, module map, data flow between processes
5. [conventions.md](./conventions.md): patterns, osiris conventions, test infrastructure
6. [upload-path.md](./upload-path.md): remote replica reader lifecycle (startup, drain, seal, upload, broadcast)
7. [read-path.md](./read-path.md): offset resolution, local/remote routing, fragment iterator, prefetch
8. [manifest.md](./manifest.md): tree structure, binary entry format, rebalancing, concurrency control

### Deep dive

Cover to cover. Follow the developer track, then continue with:

8. [failure-modes.md](./failure-modes.md): failure triggers, system response, operator actions
9. [log-framing.md](./log-framing.md): binary formats for fragments, index records, chunk headers
10. [scale.md](./scale.md): theoretical and practical limits of a single stream
11. [tla/README.md](../tla/README.md): TLA+ model, what it proves, how to run it

### Reference

- [GLOSSARY.md](./GLOSSARY.md): alphabetical definitions of all terms used in these docs
- [scale.md](./scale.md): how far the design scales and what limits you in practice
- [investigations/](./investigations/): one-off analyses that informed design decisions
