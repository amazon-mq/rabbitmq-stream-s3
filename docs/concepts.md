# Concepts

RabbitMQ streams are append-only logs built on [Osiris](https://github.com/rabbitmq/osiris), the library that implements the streaming log subsystem within RabbitMQ. Publishers write messages to the end. Consumers can read from any position, and reading does not remove data. Old data is deleted by retention policies, not by consumption. This plugin replicates committed stream data to S3 so that consumers can read arbitrarily far back without requiring all that data to live on local disk.

See [GLOSSARY.md](./GLOSSARY.md) for a quick-reference of all terms used here.

## If you know queues

RabbitMQ is historically a message broker so you may already be familiar with the concepts of queues. Streams share some concepts with queues but differ in important ways. If you are only familiar with streaming brokers (such as Kafka) then you may skip this section.

| Queue concept | Stream equivalent | Key difference |
|---------------|-------------------|----------------|
| Message | Record | Same idea: a unit of data published by a client. |
| Consume = remove from queue | Consume = read at an offset | Consuming does not remove data. Multiple consumers read independently. |
| Consumer acknowledgment | Offset tracking | The broker tracks where each consumer is, but the data stays. |
| Replication (Raft log in quorum queues) | Replication (epoch-based majority commit) | Both replicate for durability. Streams use a simpler protocol optimized for sequential I/O. |
| Message TTL / queue length limits | Retention (max-age, max-bytes) | Queues drop individual messages. Streams delete entire segments from the head. |
| No equivalent | Offset specs | Consumers can attach at the beginning, end, a specific offset, or a timestamp. |
| No equivalent | Chunks | The writer batches records into chunks for efficient I/O and replication. |
| No equivalent | Tiered storage | Old data lives in S3. Consumers read from S3 transparently when local data is gone. |

## The big picture

Think of a stream as a tape that only moves forward. Publishers append to the right end. Consumers place their read head anywhere on the tape. Retention trims from the left end.

```
                        ┌─────────────────────────────────────────────┐
                        │                  Stream                     │
                        │                                             │
  retention trims ───►  │  offset 0    ...    offset N    offset N+1  │  ◄─── publishers append
        here            │                                             │
                        └─────────────────────────────────────────────┘
                                 ▲              ▲
                            consumer A     consumer B
                          (reading old)   (reading recent)
```

Without this plugin, the tape is stored entirely on local disk. When retention trims the left end, that data is gone forever.

With this plugin, the tape has two layers:

```
  ◄──────── remote tier (S3) ─────────────►◄──── local tier (disk) ────►

  ┌────────────────────────────────────────┬────────────────────────────┐
  │  fragments in S3                       │  segments on disk          │
  │  (uploaded asynchronously)             │  (written by osiris)       │
  └────────────────────────────────────────┴────────────────────────────┘
  ▲                                         ▲                           ▲
  oldest data                          overlap zone               newest data
  (remote only)                    (exists in both)            (local only, not
                                                               yet uploaded)
```

Local disk is a sliding window over the full stream. New data arrives on the right. Old data is uploaded to S3 and eventually trimmed from local disk. Consumers near the tail read from disk. Consumers reading old data read from S3.

To make this concrete: a stream publishing 50 MB/s with local max-bytes = 10 GB keeps about 3 minutes of data on disk. A consumer starting from 24 hours ago reads roughly 4 TB from S3, then switches to local disk for the most recent 3 minutes.

## Following a message

This section traces a single message through the system.

### Publish

A client declares a stream and publishes a 100-byte message. The writer process on the leader node batches it with other messages arriving around the same time and writes the batch as a chunk to the current segment file. The chunk gets a 48-byte header (offset, timestamp, epoch, CRC). A 29-byte record is appended to the index file.

### Replicate

The writer sends the chunk to each replica node. Replicas append the chunk to their own segment files. Once a majority of nodes have the chunk, the writer advances the commit offset. The publisher receives a confirm.

### Upload to S3

On the writer node, a remote replica reader process monitors the commit offset. As committed chunks accumulate, it tracks their metadata (offsets, timestamps, sizes). When enough data has accumulated (typically 64 MiB), the remote replica reader cuts a fragment: it reads the chunk bytes from the segment file(s), packages them with the corresponding index records, and uploads the result as a single S3 object.

After upload, the remote replica reader updates the manifest and notifies replica nodes.

### Local retention

The stream has a max-bytes retention policy. When total local size exceeds the limit, the oldest segment files are deleted. The plugin's retention function also deletes segments whose data has been fully uploaded to S3. Either way, the data is gone from local disk but not from S3.

### Consume (local)

A consumer subscribes at a recent offset (an offset spec tells the broker where to begin: a specific offset, a timestamp, `first`, `last`, or `next`). The broker opens an offset reader. The reader binary-searches the index file for the chunk containing that offset, reads chunk data from the segment file, and delivers it. Sequential disk I/O, no network round-trips to S3.

### Consume (remote)

A consumer subscribes at offset 0, but that data no longer exists locally. The log reader checks the manifest, finds the fragment containing offset 0, binary-searches the fragment's index for the byte position, and streams data from S3 using HTTP range requests. When the consumer catches up to data that still exists locally, reading switches back to disk.

## Streams in depth

### Records, entries, and chunks

A record is a single message. Records are not written to disk individually. The writer batches them.

The batching has two levels:

```
  Records:    r1  r2  r3  r4  r5  r6  r7  r8  r9  r10  r11  r12
                  │           │               │              │
  Entries:    ┌───┴───┐   ┌───┴───┐       ┌───┴────┐    ┌────┴────┐
              │ entry │   │ entry │       │ entry  │    │  entry  │
              │(batch)│   │(batch)│       │(single)│    │ (batch) │
              └───┬───┘   └───┬───┘       └───┬────┘    └────┬────┘
                  │           │               │              │
  Chunk:      ┌───┴───────────┴───────────────┴──────────────┴─────┐
              │              chunk (one I/O operation)             │
              │  [48-byte header] [entry] [entry] [entry] [entry]  │
              └────────────────────────────────────────────────────┘
```

- **Records** are individual messages.
- **Entries** are either a single record or a client-side batch of records (sub-batch entry, possibly compressed). Sub-batches come from the Streams protocol when clients batch on their end.
- **Chunks** are what the writer writes to disk. One chunk = one `write()` syscall, one replication unit, one index record.

Fewer, larger writes mean less syscall overhead, less index bloat, and more efficient replication. At high throughput, chunks grow larger and per-record overhead drops.

Each chunk header contains:
- Chunk type
- First offset and number of records
- Epoch
- Timestamp
- CRC32 checksum
- Data length and trailer length
- Bloom filter for message filtering

### Chunk types and tracking data

There are three chunk types:

| Type | Value | Contents |
|------|-------|----------|
| User | 0x00 | Application messages (records). The common case. |
| Tracking delta | 0x01 | Incremental tracking updates only, no application data. |
| Tracking snapshot | 0x02 | Full snapshot of all tracking state. Written at the start of each new segment. |

**Tracking data** is metadata the server stores alongside stream data for two purposes:

1. **Producer deduplication.** A producer registers a tracking ID and sequence number. If a confirm is lost and the producer retries, the writer detects the duplicate by comparing the sequence number against stored tracking state. This prevents duplicate records in the stream.

2. **Consumer offset tracking.** A consumer stores its current offset on the server. On restart, the consumer resumes from the stored offset without maintaining state client-side.

Tracking data can appear in two places within a chunk:

- **Trailer.** User chunks (type 0x00) have a trailer section after the data entries. When tracking updates arrive in the same batch as user writes, the tracking data is appended as a trailer on the user chunk. The chunk header records the trailer length so readers can find it.
- **Standalone chunk.** When tracking updates arrive with no user writes in the batch, they are written as a tracking delta chunk (type 0x01). The entries in this chunk are tracking records, not application messages.

Tracking snapshots (type 0x02) are written at segment rollover. They contain the full tracking state at that point, so recovery only needs to read from the start of the current segment rather than replaying the entire log.

For tiered storage, tracking chunks are treated like any other chunk. They are uploaded as part of fragments and read back from S3 the same way. The plugin does not interpret tracking data.

### Segments and index files

Chunks are appended to segment files. A segment is a file containing a sequence of chunks. Each segment has a companion index file with one small fixed-size record per chunk. The segment holds the actual data (kilobytes to megabytes per chunk). The index holds just enough metadata to find a chunk without scanning the segment.

```
  Segment file: 00000000000000000000.segment (up to 500 MB)
  ┌───────────────────┬────────────────┬──────────────────────────┬────────┐
  │      chunk 0      │    chunk 1     │         chunk 2          │  ...   │
  │   (12 KiB data)   │  (85 KiB data) │     (200 KiB data)       │        │
  │  records + header │ records+header │   records + header       │        │
  └───────────────────┴────────────────┴──────────────────────────┴────────┘

  Index file: 00000000000000000000.index (one record per chunk, 29 bytes each)
  ┌─────┬─────┬─────┬─────┐
  │ idx │ idx │ idx │ ... │   Each: offset, timestamp, epoch, file position, type
  │  0  │  1  │  2  │     │   Just a pointer into the segment file above.
  └─────┴─────┴─────┴─────┘
```

Segment files are named with the offset of their first chunk. When a segment reaches its size limit (default 500 MB) or chunk count limit (256,000), it is closed and a new one is opened. This is segment rollover.

#### What you see on disk

A stream's data directory contains pairs of files:

```
  00000000000000000000.segment    (500 MB)
  00000000000000000000.index      (7 MB)
  00000000000000512000.segment    (500 MB)
  00000000000000512000.index      (7 MB)
  00000000000001024000.segment    (230 MB, current, still being written)
  00000000000001024000.index      (3 MB)
```

The filename is the offset of the first chunk in that segment, zero-padded to 20 digits. The segment file is large (up to 500 MB of chunk data). The index file is small (29 bytes per chunk, so a full segment with 256,000 chunks has a ~7 MB index).

#### The index file

The index exists so that readers can find a chunk by offset or timestamp without scanning the segment. Each 29-byte record maps one chunk to its position in the segment file:

| Field | Size | Purpose |
|-------|------|---------|
| Chunk ID (offset) | 8 bytes | Find a chunk by offset |
| Timestamp | 8 bytes | Find a chunk by timestamp |
| Epoch | 8 bytes | Identify which writer wrote this chunk |
| File position | 4 bytes | Seek directly to the chunk in the segment file |
| Chunk type | 1 byte | Skip non-data chunks during reads |

The search algorithm is a skip search: jump forward 2048 index records at a time, peek at the offset, and when the target is overshot, linear-scan the last block. This is more page-cache friendly than a binary search because it reads forward sequentially within blocks rather than jumping to random positions. The index file is small enough to fit in the page cache entirely for active segments.

The epoch field in each index record also serves a second purpose: when a replica connects to a writer, the writer scans its index files to reconstruct the epoch history (where each epoch began). This is how replicas determine where to truncate after an election.

To find offset 1,000,000 in a segment: skip-search the index, get the file position, `pread` directly to that byte in the segment file. Two reads total (index + segment), both likely served from page cache.

For the full binary layout of chunk headers, index records, and file headers, see [log-framing.md](./log-framing.md).

### Stream ID

Each stream has an internal identifier that is distinct from the user-visible queue name. When a stream queue is declared, RabbitMQ constructs the stream ID from the virtual host, queue name, and a timestamp, then encodes it into a URL-safe string:

```
  base64uri(VHost + "_" + QueueName + "_" + Timestamp)

  Example: ___my-stream_1745252105682952932
           ^   ^          ^
           |   |          creation timestamp (erlang:system_time())
           |   queue name
           vhost "/" encoded as "_"
```

The stream ID is used as:
- The directory name for the stream's segment and index files on disk.
- Part of the S3 key prefix for all remote tier objects: `rabbitmq/stream/<StreamId>/data/...` for fragments, `rabbitmq/stream/<StreamId>/metadata/...` for manifests.

The timestamp makes the ID unique even if a stream is deleted and recreated with the same name. This prevents a new stream from colliding with leftover S3 objects from a previous stream of the same name.

The stream ID is not exposed to clients. It is an internal detail visible to operators in the filesystem and in S3 bucket listings.

### Replication and commit

A stream cluster has one writer and zero or more replicas. The writer batches entries into chunks and writes to its local log. Replicas copy chunks from the writer.

```
  Publisher ──► Writer (node A)
                  │
                  ├──► Replica (node B)    ── majority ──► commit offset advances
                  │                                        publisher gets confirm
                  └──► Replica (node C)
```

The commit offset is the offset of the latest chunk replicated to a majority of nodes. Data at or below the commit offset is durable. Publishers receive confirms when their messages are committed. Consumers can only read committed data.

The remote tier (S3) does not count towards the majority needed to advance the commit offset. It is a non-voting observer: it receives committed data after the fact but has no influence on when data is considered committed. This is intentional. If S3 were a voter, an S3 outage would stall commits and block all publishing. The local cluster must remain fully operational regardless of remote tier availability.

### Epochs and elections

Epochs solve a fundamental problem: what happens to data that was written by a leader that is no longer the leader?

In a stream cluster, only one node writes at a time. If that node fails, a new writer is elected. But the old writer may have written chunks that were not yet replicated to a majority. Those chunks are not committed. They exist on some nodes but not others. The new writer cannot trust them because they might conflict with what it writes next.

The epoch is a monotonically increasing number that increments with each election. Every chunk header records the epoch in which it was written. This lets any node look at a chunk and know which writer produced it.

#### Example: a three-node cluster

Consider nodes A, B, and C. Node A is the writer in epoch 1.

```
  Epoch 1: Node A is writer

  Node A (writer):  chunk 0  chunk 1  chunk 2  chunk 3  chunk 4
  Node B (replica): chunk 0  chunk 1  chunk 2  chunk 3
  Node C (replica): chunk 0  chunk 1  chunk 2
                                               ▲
                                          commit offset
                                       (majority = 2 of 3)
```

Chunks 0-2 are on all three nodes. Chunk 3 is on A and B (a majority), so the commit offset is at chunk 3. Chunk 4 is only on A. It is not committed.

Now node A crashes. Nodes B and C elect node B as the new writer in epoch 2.

```
  Epoch 2: Node B is writer

  Node A (down):    chunk 0  chunk 1  chunk 2  chunk 3  chunk 4
  Node B (writer):  chunk 0  chunk 1  chunk 2  chunk 3
  Node C (replica): chunk 0  chunk 1  chunk 2
```

Node B starts writing in epoch 2. But what about chunk 4 on node A? It was never committed (only on one node). When node A eventually restarts and rejoins as a replica, it must discard chunk 4. The epoch tells it how: node A sees that the cluster is now in epoch 2, and any chunks it has from epoch 1 beyond the committed offset must be truncated.

What about chunk 3 on node C? Node C is missing chunk 3 but it was committed (on A and B). When C becomes a replica of B, it receives chunk 3 through normal replication. No data is lost.

#### Why not just use offsets?

Offsets alone cannot distinguish "chunk 4 written by writer A in epoch 1" from "chunk 4 written by writer B in epoch 2." Both have offset 4. Without epochs, a replica rejoining the cluster cannot tell whether its local chunk 4 matches the current writer's chunk 4 or is stale data from a previous timeline.

The epoch in each chunk header makes this unambiguous. During truncation, the rejoining node walks its log backwards: any chunk whose epoch does not match the current epoch history is discarded.

#### Epoch history

When a replica connects, it asks the writer for an overview of the log. The writer scans its index files to produce a list of `{epoch, first_offset}` pairs recording where each epoch began. The replica compares this against its own log to find the divergence point and truncates everything after it.

```
  Writer B's log overview includes epoch offsets:
    [{1, 0}, {2, 4}]

  Meaning: epoch 1 started at offset 0, epoch 2 started at offset 4.
```

Node A, rejoining with chunks 0-4 all in epoch 1, sees that epoch 2 starts at offset 4. It truncates chunk 4 (offset 4, epoch 1) because the current timeline has epoch 2 starting there.

This scan is computed from the index files on demand, not maintained in memory. Each index record includes the epoch of its chunk, so the writer can reconstruct the epoch history by walking the index files and noting where the epoch changes.

#### Implications for tiered storage

If the remote replica reader uploaded chunk 4 before node A crashed, S3 would contain data from a timeline that no longer exists. The new writer (B) will write a different chunk 4 in epoch 2. Now S3 has stale data that contradicts the authoritative local log.

This is why the remote replica reader only uploads committed data. Chunk 4 was never committed, so it was never uploaded. The remote tier stays consistent with the surviving timeline.

If an epoch change happens after data was uploaded (the remote tier is ahead of the new local log), the remote replica reader detects this on startup and truncates the manifest. The local log is always authoritative.

### Retention

Retention rules determine when old segments are deleted. A stream can have no retention (grow without bound), max-age, max-bytes, or both. When both are set, either condition can trigger deletion. Retention is evaluated on segment rollover. Entire segments are deleted, not individual chunks.

```
  Before retention:
  [segment 0] [segment 1] [segment 2] [segment 3] [segment 4]
   200 MB       500 MB       500 MB       500 MB       300 MB    = 2 GB total

  After retention (max-bytes = 1.5 GB):
                          [segment 2] [segment 3] [segment 4]
                             500 MB       500 MB       300 MB    = 1.3 GB total
```

Retention deletes from the head (oldest first). The unit of deletion is a segment. A segment is only deleted when all its data is older than max-age or when removing it brings total size below max-bytes.

## How the plugin extends this model

The plugin adds a remote tier without changing how Osiris writes or replicates. Disable the plugin and streams work exactly as before, just without S3.

### The remote replica reader

Uploads happen in a separate process that reads from the local log. The remote replica reader uses the same data reader interface that replicas use to copy chunks. From Osiris's perspective, it looks like another replica, except instead of writing to a local segment file, it uploads to S3.

This means:
- The writer never blocks on S3. Latency, throttling, or outages have zero impact on publish latency.
- The remote replica reader can fall behind during S3 outages and catch up later, like a slow replica.
- If the remote replica reader crashes, it restarts and resumes from where S3 left off. The local log still has everything.

### Fragments

A fragment is a remote-tier object containing a section of the stream. Fragments are typically 64 MiB (configurable), much smaller than the 500 MB segment size.

Why smaller?

- Waiting for a full segment to roll before uploading means minutes or hours of delay for low-throughput streams. Smaller fragments upload more frequently.
- When a consumer requests a specific offset from S3, it downloads at most one fragment to find it. Smaller fragments mean less wasted bandwidth.

Fragments can span segment boundaries. If a segment rolls in the middle of assembling a fragment, the fragment includes data from both segments. The upload cadence is decoupled from segment rollover.

Each fragment contains:
1. An 8-byte header (magic "OSIF" + version), same pattern as segment and index files
2. The raw chunk bytes (same bytes as in the segment file), starting at byte 8
3. Index records (20 bytes each: offset, timestamp, position within the object)

The manifest entry for a fragment stores `Size` (the byte length of the segment data). The index starts at byte `8 + Size` in the object. A consumer reading from S3 downloads the index (byte range from `8 + Size` to end), binary-searches it for the byte position, then reads chunk data starting there. Same algorithm as local reads, just over HTTP range requests instead of `pread`.

### The manifest

S3 has no efficient "find the object containing offset X" operation. Listing objects is slow and paginated. The manifest solves this: it is an object-based tree structure that maps offset ranges to fragment keys. Some objects are cached in memory for low-latency access.

When a consumer needs offset 1,000,000 and it is not local, the log reader binary-searches the manifest to find which fragment contains that offset, constructs the S3 key, and starts reading. No LIST calls, no scanning.

The manifest also first offset and timestamp to make retention evaluation.

See [manifest.md](./manifest.md) for the tree structure, binary format, and concurrency control.

### Two retentions

With tiered storage there are two retention domains:

```
  Local retention                          Remote retention
  (max-bytes, max-age,                     (max-bytes, max-age
   + plugin's upload-based reclaim)         applied to fragments in S3)
         │                                          │
         ▼                                          ▼
  ┌──────────────────┐                    ┌──────────────────┐
  │  local segments  │                    │  S3 fragments    │
  └──────────────────┘                    └──────────────────┘
```

**Local retention** has two components:
1. The user's configured policy (max-bytes, max-age), same as vanilla streams.
2. The plugin's `{'fun', ...}` spec, which deletes segments whose data has been fully uploaded to S3. This is what makes local disk a sliding window.

Both run independently. The user's policy can delete segments the plugin has not uploaded yet (if S3 is down for a long time). The plugin accepts the resulting gap.

**Remote retention** applies the same max-age/max-bytes rules to fragments in S3. The remote replica reader evaluates this periodically.

Local retention does not wait for uploads. If you configure max-bytes = 5 GB and S3 is unreachable, local retention still fires at 5 GB. The remote tier will have a gap for that period. Local disk availability is never sacrificed for remote tier completeness.

### The commit offset is the upload boundary

Only committed data is uploaded to S3. This is a hard rule.

Uncommitted data might be truncated on the next epoch change. If it were uploaded, S3 would contain data that no longer exists in the authoritative local log. Consumers can only read committed data anyway, so there is no consumer that could benefit from uncommitted data in S3.

The remote replica reader gates every read on the commit offset. Before reading the next chunk header, it checks whether that chunk is committed. If not, it stops and waits for the commit offset to advance.

## What can go wrong

This doc describes the happy path. In practice, several things can disrupt the clean model above:

- S3 becomes unreachable for an extended period. The remote replica reader falls behind. Local retention may delete data that was never uploaded, creating a gap in the remote tier.
- A leader election happens mid-upload. Two remote replica readers briefly compete. The optimistic lock in Khepri resolves the race, but the deposed reader's uploaded object becomes an orphan.
- Local retention races ahead of uploads. A consumer requesting old data finds a gap where neither the local tier nor the remote tier has the data.
- A power-off event loses data from the page cache that was committed but not yet flushed to disk by the OS.

Each of these is handled by the system. For details on what the system does, what operators observe, and what action is needed, see [failure-modes.md](./failure-modes.md).

## Next steps

- [architecture.md](./architecture.md) for how these concepts map to processes and modules
- [manifest.md](./manifest.md) for the manifest tree structure in detail
- [GLOSSARY.md](./GLOSSARY.md) for quick-reference definitions
