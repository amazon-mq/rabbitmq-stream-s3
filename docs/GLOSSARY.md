# Glossary

## Acceptor

Log access mode used by replicas to accept chunks replicated from the writer.

## Chunk

A batch of serialized entries written to the log as a single unit. Chunks are the unit of replication in Osiris. Each chunk has a 48-byte header containing the chunk type, offset, epoch, timestamp, checksum, and the size of the bloom filter, followed by the filter itself when the chunk carries one.

## Commit offset

The offset of the latest chunk replicated to a majority of nodes. Equivalent to Raft's committed index. Data at or below the commit offset is durable in the local cluster and visible to consumers. This is a local tier concept. See also: Persist.

## Data reader

Log access mode used on the writer node to send chunks to acceptors during replication.

## Entry

Either a single record (simple entry) or a batch of possibly compressed records (sub-batch entry from the RabbitMQ Streams protocol).

## Epoch

Monotonically increasing number incremented with each writer election. Used to truncate uncommitted data when a writer is deposed. Equivalent to Raft terms.

## Fragment

A remote-tier object containing a section of chunk-aligned segment data followed by index records. Fragments are typically around 64 MiB but vary in size. The object starts with an 8-byte header (magic + version), then segment data, then index records. The index starts at byte `8 + Size` where `Size` is from the manifest entry. Fragment keys include a UID to prevent overwrites during leader elections.

## Fragment iterator

A forward iterator over the manifest's leaf entries (fragments). Hides the tree structure from readers. Lazily downloads group objects as it descends into branches.

## Group

A manifest object that factors out the oldest entries from the root array. Contains up to `m` (1024) fragment entries. Groups keep the root small while supporting lookup over large streams.

## Index file

Companion file (`<offset>.index`) to a segment file. Contains a 29-byte record per chunk: chunk ID (8), timestamp (8), epoch (8), file position (4), chunk type (1).

## Khepri

RabbitMQ's metadata store. Used by this plugin to store manifest UIDs and epochs with optimistic concurrency control.

## Kilo-group

A manifest object that factors out `m` groups. One level above groups in the manifest tree.

## Local tier

Stream data stored on local disk as segment and index files. Managed by Osiris.

## Manifest

A tree structure describing all data stored in the remote tier for a stream. The root is cached in memory. Used for offset/timestamp resolution and retention evaluation. See [manifest.md](./manifest.md).

## Manifest cache

A per-node store (`rabbitmq_stream_s3_manifest_replica`) that caches manifest roots per stream in a public ETS table (`rabbitmq_stream_s3_manifest_cache`). Readers read the table directly; writes go through the gen_server. Receives manifest edits from the writer node's remote replica reader and keeps log reader routing in sync.

## Mega-group

A manifest object that factors out `m` kilo-groups. The largest level in the manifest tree.

## Offset

Unique incrementing integer addressing each record in the log. Consumers use offsets to start reading from arbitrary positions.

## Offset reader

Log access mode used by consumers to read from a stream starting at a specified offset spec.

## Offset spec

A description of where a reader should attach to the log. Examples: `first` (head), `last` (tail), `next` (after tail), an arbitrary offset, or a timestamp.

## Osiris

The library providing the streaming subsystem for RabbitMQ. See [github.com/rabbitmq/osiris](https://github.com/rabbitmq/osiris).

## Persist

The operation that makes uploaded fragments authoritative in the remote tier. The persist sequence is: PUT the manifest root to S3, then write the root UID to Khepri with an optimistic lock. Only after a successful persist do fragments become visible to consumers, retention can advance, and the range table updates. This is a remote tier concept. See also: Commit offset.

## Record

The smallest unit of user data in a stream. Corresponds to an individual message published to RabbitMQ.

## Remote tier

Stream data stored in S3 as fragment objects. Provides bottomless retention at low cost.

## Replica

A follower node that copies chunks from the writer. The commit offset is the highest offset a majority of writer and replicas have acknowledged.

## Replica reader

A process on the writer node that reads chunks from the local log and sends them over TCP to a replica. Part of Osiris's replication protocol (`osiris_replica_reader`). Not to be confused with the remote replica reader.

## Remote replica reader

A per-stream gen_server on the writer node that owns the upload path end to end. Reads committed chunks from the local log, assembles fragments, uploads them to S3, updates the manifest, and broadcasts edits to replica nodes. Mirrors the role of the replica reader but targets S3 instead of a replica node.

## Retention

Rules (max-age, max-bytes, or both) that determine when data is deleted. Local retention and remote retention run independently. The plugin adds a `{'fun', ...}` retention spec that reclaims local segments once their data is uploaded.

## Segment file

On-disk file (`<offset>.segment`) containing a sequence of chunks. Named with the offset of the first chunk. Default max size is 500 MB.

## Segment rollover

Closing the current segment and index files and opening new ones when limits are reached (default: 500 MB or 256,000 chunks).

## Stream

An ordered, immutable, append-only sequence of records. Can be truncated by retention but not compacted.

## Stream ID

Internal binary identifier for a stream, e.g. `<<"__stream-01_1745252105682952932">>`. Found in the queue's type state via `amqqueue:get_type_state/1`.

## Tracking data

Producer and consumer metadata used for message deduplication and server-side offset tracking. Stored as a snapshot chunk at the start of each segment, with delta chunks for edits within a segment.

## UID

A random 32-bit identifier included in fragment and manifest object keys. Prevents overwrites when competing writers upload to the same offset range. Formatted as 8 lowercase hex characters in S3 keys.

## Writer

The leader member in a stream cluster. Accepts entries from clients, batches them into chunks, and writes them to the local log.
