# `rabbitmq-stream-s3`

`rabbitmq-stream-s3` provides tiered storage for RabbitMQ streams using Amazon S3 as the remote tier to provide bottomless stream storage.

At a high level, this plugin replicates stream data from local `.segment` and `.index` files to S3 once the data becomes committed. Stream data can then be read either out of the local tier (if available) or the remote tier.

This document discusses the current design. This design is not set in stone and this document will change in the future.

### Glossary

* [**Osiris**](https://github.com/rabbitmq/osiris): The library providing the streaming subsystem used by RabbitMQ.
* **Stream**: Ordered, immutable, append-only sequence of records that can be truncated based on retention settings but cannot be compacted.
* **Record**: The smallest unit of user data in a stream, corresponding to individual messages published to RabbitMQ.
* **Entry**: Either a single record (simple entry) or a batch of possibly compressed records (batched entry from RabbitMQ Streams Protocol).
* **Offset**: Unique incrementing integer that addresses each record in the log, allowing consumers to start reading from any arbitrary position.
* **Chunk**: Batched and serialized entries written to the log, serving as the unit of replication in Osiris. Contains header with metadata including chunk type, offset, epoch, timestamp, checksum, and bloom filter.
* **Writer**: Leader member within a cluster which accepts entries from clients, batches them into chunks, and writes them to the local log.
* **Commit Offset**: The offset of the latest chunk which has been successfully replicated to a majority of nodes, identical to Raft's committed index concept.
* **Epoch**: Monotonically increasing number similar to Raft terms, incremented with each successful writer election. Used for truncating uncommitted data when a writer is deposed.
* **Replica**: Follower nodes that copy chunks from the writer to produce identical on-disk logs. The median offset across writer and replicas determines the commit offset.
* **Segment File**: On-disk file (`<offset>.segment`) containing a sequence of chunks. The filename offset indicates the first chunk's offset in the segment.
* **Index File**: Companion file (`<offset>.index`) to a segment file with fixed-size records tracking each chunk's offset, timestamp, epoch, byte position in segment, and chunk type.
* **Segment Rollover**: Process of closing current segment and index files and creating new ones when limits are reached (default: 500 MB or 256,000 chunks). Segment rollover also triggers writing a tracking snapshot and evaluating retention (see below).
* **Tracking Data**: Information about producers and consumers used for message deduplication and server-side consumer offset tracking. Segment files start with a tracking snapshot chunk and then use tracking deltas for edits to the data within a segment.
* **Acceptor**: Log access mode used by replicas to accept chunks replicated from the writer.
* **Data Reader**: Log access mode used on the writer node to send chunks to acceptors during replication.
* **Offset Reader**: Log access mode used by consumers to read from a replica starting at a specified offset spec.
* **Offset Spec**: A description of where an offset reader should attach to the log for reading. For example first attaches at the head, last at the tail, next for the next message after the tail. Offset specs may also be arbitrary offsets or timestamps of records in the stream.
* **Manifest**: A data structure describing each stream. The manifest holds information necessary for retention like total stream size and oldest timestamp, and is used by offset and data readers to find exact positions of chunks within the stream.
* **Retention**: Configurable rules (max age, max size, or both) that determine when oldest segments are truncated from the stream. Evaluating retention involves finding the oldest segments which can be deleted to satisfy the retention rules.

## Stream data representation

### Local-tier storage

This plugin does not change the on-disk storage of stream data used by Osiris. Stream data is stored in a `stream/` directory under RabbitMQ's data directory by default.

```
data/
├── stream/
│   ├── __stream-01_1745252105682952932/      // directory for stream named "stream-01" on default vhost
│   │   ├── 00000000000000000000.index        // first index file
│   │   ├── 00000000000000000000.segment      // first segment file
│   │   ├── 00000000000026061613.index        // second index file starting at offset 26061613
│   │   ├── 00000000000026061613.segment      // second segment file starting at offset 26061613
│   │   ├── 00000000000052132876.index        // second index file starting at offset 52132876
│   │   └── 00000000000052132876.segment      // second segment file starting at offset 52132876
│   ├── __stream-02_1745252105720074141/      // directory for stream named "stream-02" on default vhost
│   │   └── ..
│   ├── my-vhost_foo_1745253455243785338/     // directory for stream named "foo" on vhost "my-vhost"
│   │   └── ..
```

Each stream uses a directory under the `stream/` directory in the format `{vhost}_{stream-name}_{created-timestamp}`. Messages published to streams are stored in segment files. Segment files are named with the offset of the first entry in the segment. Messages are batched together by the writer process into chunks and appended to the latest segment file. Once the segment file exceeds a max size or a max number of chunks, the segment is closed and a new one is opened. Every segment has a corresponding index file with the same offset. The index file contains a small record for each chunk in the segment with metadata like the offset, timestamp, and byte offset of the chunk within the segment file. The default max size of a segment file is 500 MB. The size of the index depends on the number of chunks in the segment file. Publishing at high throughput results in smaller index files since there are fewer, larger chunks.

Segment and index files are identical between cluster members of a stream. All records are send to a writer member which writes the records as chunks. Any number of replica members then replicate the chunks from the writer's log. When a majority of members have written a chunk, publishers receive confirms that their messages have been written and the messages may then be read by consumers. Cluster membership, epoch numbers and writer/replica roles are decided by a Raft system.

### Remote-tier storage

`rabbitmq-stream-s3` uploads committed stream data to the remote tier aggressively to make local-tier data redundant quickly. In order to upload more aggressively, `rabbitmq-stream-s3` uses a different data representation in the remote tier.

### S3 bucket layout

`rabbitmq-stream-s3` uses one S3 bucket per cluster. The remote tier uses one [prefix](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html) per stream similar to the local tier's use of directories. Under this prefix there prefixes `data` to store stream data like segment and index file contents, and `metadata` to store tracking information used for consumers and retention.

<!-- NOTE: it would be easy to also support multi-tenant buckets by adding config for a prefix to use within a bucket. We wouldn't use this feature ourselves though. -->

```
rabbitmq/
├── stream/
│   ├── __stream-01_1745252105682952932/
│   │   ├── data/
│   │   │   └── ...      // segment and index data
│   │   └── metadata/
│   │       └── ...      // manifest data
```

### Fragments

The data representation between the local and remote tiers are separate. The local tier contains segment and index files while the remote tier contains smaller objects called _fragments_ which concatenate smaller sections of segment and index data together. Where a segment file typically reaches 500 MB, fragments store a smaller section of a segment around 64 MB of chunk-aligned segment data and their accompanying records from the index file. NOTE: the 64 MB figure will be tuned in testing. We expect a size in the range of 10 MB - 128 MB to be ideal.

![Fragment layout](./Fragment.svg)

![Fragments from segments](./FragmentsFromSegment.svg)

Fragments start with a header, contain a sequence of one-or-more chunks and then contain the index header, a sequence of index records and finally a trailer. The trailer contains metadata like pointers to the beginning of the index within the fragment, the fragment's sequence number and offset into the segment file, offset of the next fragment, etc..

```
rabbitmq/
├── stream/
│   ├── __stream-01_1745252105682952932/
│   │   ├── data/
│   │   │   ├── 00000000000000000000.fragment
│   │   │   ├── 00000000000000001234.fragment
│   │   │   ├── 00000000000000005678.fragment
│   │   │   └── ...
│   │   └── metadata/
│   │       └── ...
```

Uploading fragments of segment and index files means that committed data can be uploaded more aggressively - once a fragment of data becomes committed - and uploads within a segment can be done in parallel for better network utilization.

## Manifest

The manifest is a data structure which lives in the remote tier alongside stream data which helps answer questions about a stream's data.

Because a stream can become practically infinitely long in the remote tier and querying the remote tier has relatively high latency, answering questions about a stream like its first offset, timestamp or total size is not straightforward. It's also trickier to determine where to start reading in a stream, for example finding where to start reading the data which was published 5 days ago. To answer all of these questions in the local tier, Osiris lists the stream directory and reads through segment and index files. Querying an object store the same way could be prohibitively expensive in terms of time and request cost.

To make answering questions about a stream quick and cheap, `rabbitmq-stream-s3` stores a data structure in S3 which tracks uploaded stream data.

### How much data can the remote tier hold?

For some napkin math, publishing at a very very high throughput of 1 GB/sec to a single stream can publish a petabyte (PB) of data in a matter of days. An exabyte (EB) can be published in a hypothetically possible number of years (~32) and a zettabyte (ZB) would take an impossibly long time to publish. Publishing and retaining immense amounts of data like this is unlikely to happen in practice, but because storage in S3 is practically bottomless, the manifest should be able to handle these absurd sizes gracefully.

### M-way tree

The difficulty in tracking very large streams is the number of fragments. 1 PB of segment data at a fragment size of 64 MB would take 15,625,000 fragments to represent. To track very large numbers of fragments, `rabbitmq-stream-s3` stores a tree structure in the remote tier under each stream's `metadata` directory. Leaf nodes in this tree are records with metadata about a fragment: the offset, timestamp, file size, and sequence number within the segment. These records are individually very small - low double digits of bytes per fragment.

When a fragment is uploaded, a metadata record for it is appended to the array of records in the root node. Once the root node reaches a large number of records, the writer rebalances out a _group_ of fragments. Rebalancing puts the `M` oldest fragments under a new branch and replaces the records in the root's array with a new record which points to the group. A `{first-offset}.group` object is added to the metadata directory in S3 and then the manifest is overwritten with the rebalanced set of records. Rebalancing keeps the root node small so that downloads, manipulation, and uploads remain cheap. Rebalancing away the oldest `M` records biases for recent data. Following a branch means a round-trip to S3, so fragments in the root are always fastest to look up. For larger streams, once enough groups have been rebalanced, the root can further rebalance away `M` groups into a _kilo-group_. `M` kilo-groups could also be rebalanced away into a _mega-group_, but it's unlikely to happen in practice if `M` is large.

![M-way tree manifest](./ManifestMWay.svg)

The branching factor will be tuned in testing but a relatively high value like `M=1024` provides a theoretically good balance. With 30 bytes of metadata per fragment, group files would take around 30 KiB to represent - small enough to be comfortably downloaded and searched in-memory. A high branching factor keeps the height of the tree low. With `M=1024` and fragments even as small as 1 MB, a lookup within the tree for one of the oldest fragments would take at-most four round-trips to S3: the root (cached), a kilo-group, a group, and the leaf (fragment). Within tree nodes, fragment metadata is naturally stored in ascending order of offset. This mitigates the high branching factor: binary search can be used on the record arrays to quickly find specific fragments or groups within each tree node.

```
rabbitmq/
├── stream/
│   ├── __stream-01_1745252105682952932/
│   │   ├── data/
│   │   │   └── ...
│   │   └── metadata/
│   │       ├── 00000000000000000000.kgroup
│   │       ├── 00000000000000000000.group
│   │       ├── 00000000000012345678.group
│   │       ├── 00000000000091234567.group
│   │       ├── ...
│   │       └── manifest     // the root
```

### Resolving the manifest

The writer keeps the root node cached in memory and appends to it as new fragments are uploaded. To avoid high-frequency updates during high throughput publishing, the writer debounces updates to the manifest root. This means that the remote tier copy of the manifest can lag behind the latest information. And if the writer crashes before it can upload an updated copy, the remote tier might have an out-of-date copy when the next writer starts up. When a writer starts, it _resolves_ the manifest. It downloads the current copy from the remote tier and then looks up the latest fragment's trailer data. The trailer contains the offset of the next fragment, so the writer can continue following these 'next pointers' to find all uploaded fragments and add them to the downloaded copy of the manifest.

### Concurrency control

If a writer node is on the minority side of a network partition then the remaining majority can elect a new writer without the deposed writer's knowledge, so two writer processes might be alive at the same time. The network partition might not affect the writers' abilities to modify the remote tier, so we need a concurrency control to prevent conflicting changes to the manifest.

To handle this situation, `rabbitmq-stream-s3` uses an [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) to make conflicting changes practically impossible. Keys of all metadata objects include a unique identifier string (UID). So in practice the metadata directory looks like this:

```
rabbitmq/
├── stream/
│   ├── __stream-01_1745252105682952932/
│   │   ├── data/
│   │   │   └── ...
│   │   └── metadata/
│   │       ├── 00000000000000000000.f619c0873d14edeb.kgroup
│   │       ├── 00000000000000000000.d078ceab40232eff.group
│   │       ├── 00000000000012345678.79425118e949e556.group
│   │       ├── 00000000000091234567.edabc41fac4c6979.group
│   │       ├── ...
│   │       └── root.db868505ef4bc57a.manifest     // the root
```

The UID of the manifest root is stored in [Khepri](https://github.com/rabbitmq/khepri) (RabbitMQ's metadata store) associated with the stream name. So the Khepri tree looks like this:

```
rabbitmq
├── rabbitmq_stream_s3
│   └── <<"__stream-01_1745252105682952932">>
│       Data: {Uid: db868505ef4bc57a, Epoch: 5}. Version: 10
├── vhosts
│   └── <<"/">>
│       ├── queues
│       │   └── <<"stream-01">>
│       │       Data: ...
│       └── ...
└── ...
```

When updating the manifest, the writer node first creates the manifest object in the remote tier. Because of the randomness in the UID, the new object can be written without without conflict without any coordination. Then the writer updates the UID in Khepri with a conditional write: it sets a precondition that the current version of the Khepri tree node is the version it read. Khepri rejects the write if another writer has updated the tree node. When the precondition fails, the writer abandons the change, re-downloads the manifest and retries.

In the precondition, the writer node also checks that the stored epoch is less than or equal to the its epoch. Because epoch numbers are monotonically increasing and incremented for each new writer, this extra check causes a deposed writer's updates to fail once the newer writer has completed at least one update. The epoch check reduces the chance of a deposed writer interrupting a newer writer.

## Operations

### Writing

`rabbitmq-stream-s3` starts a [`gen_server`] called `rabbitmq_stream_s3_server` to coordinate tasks for all writing-related activity for streams on the local node. This server tracks each local stream and kicks off tasks to perform uploads and deletions. This server runs entirely in the background and does not interfere with the startup of writer or replica processes (for example no blocking calls during startup).

#### Publishing

The implementation of the `osiris_log_manifest` behaviour (`rabbitmq_stream_s3_log_manifest`) receives notifications when a chunk is written and when a segment file is rolled over by the `osiris_writer`. It tracks the metadata about the chunk like the segment's offset, first offset and timestamp, next chunk's offset, total size, etc.. When enough chunks have been published or when a segment file is rolled over, `rabbitmq_stream_s3_log_manifest` notifies `rabbitmq_stream_s3_server` that a fragment is available for upload. The server then kicks off a task to upload the section of the segment and index files for this fragment to a fragment object in the remote tier. This task then notifies `rabbitmq_stream_s3_server` when the upload is complete.

`rabbitmq_stream_s3_server` tracks completed uploads and when a _run_ of fragments has been uploaded, the server applies the fragment info to its in-memory copy of the manifest and uploads the new copy of the manifest to the remote tier. A set of fragments is a run when the next offset of each fragment is equal to the first offset on the next fragment - the fragments are in sequence without any gaps between them. Uploads for the manifest are debounced to avoid high-frequency updates during high throughput publishing.

#### Rebalancing

When `rabbitmq_stream_s3_server` applies uploaded fragments to the manifest, it evaluates whether the manifest is 'loaded' and a group (or kilo group or mega group) should be introduced to shrink the size of the root. The server kicks off a task to upload the group object and when that completes, the server updates the in-memory copy of the manifest and kicks off a manifest upload.

#### Retention

After fragments have been successfully uploaded to the remote tier, `rabbitmq_stream_s3_server` evaluates whether the manifest exceeds the configured retention for the stream. Evaluating these rules is cheap since the in-memory copy of the manifest contains the total size of segment data in the stream and the first timestamp. If the retention rules are exceeded and the manifest doesn't contain any groups, the server can perform retention in-place without spawning another process to perform the task. The server splits the entries array into a list of fragment offsets which should be deleted and the remaining entries array, and then the server kicks off a task to delete the fragments and a task to upload the new copy of the manifest.

For long streams where the manifest root contains groups, the manifest server kicks off a task to download the earliest group and then the task performs retention against that group, eventually updating the manifest with the new pointer to the updated group. This works recursively for kilo groups and mega groups. Performing retention in-place is an optimization for the common case since streams will typically have to exceed tens of gigabytes (1024 * ~64 MB fragments) in order to introduce a group.

#### Stream deletion

`rabbitmq-stream-s3` leverages [the data stored in Khepri](#concurrency-control) to automatically kick off tasks to delete stream data from the remote tier. The `rabbitmq_stream_s3_db` module covers the plugin's interactions with Khepri. When storing data in Khepri, `rabbitmq_stream_s3_db` sets a Khepri _keep-while condition_ that ties together the lifetime of the entry for the stream ID with the stream queue's metadata. When the stream queue is deleted from the metadata store, the plugin's data for the stream ID is automatically deleted as well. `rabbitmq_stream_s3_db` also sets a Khepri _trigger_ on the stream ID path of the tree which watches for deletions of those tree nodes and executes a _stored procedure_. The stored procedure kicks off a task to perform the deletion of remote tier objects.

For more details about keep-while conditions, triggers and stored procedures, see the [Khepri overview documentation](https://hexdocs.pm/khepri/).

NOTE: the details of this section might change in the future but we plan to rely on Khepri triggers regardless.

### Reading

The `rabbitmq_stream_s3_log_reader` module implements `osiris_log_reader`. This reader uses local data when possible and reads older data from the remote tier. Readers start at a chosen offset spec. For offset specs which access the stream tail (`next` and `tail`), `rabbitmq_stream_s3_log_reader` delegates to the local tier. For other specs like arbitrary offsets, timestamps, or the stream head, `rabbitmq_stream_s3_log_reader` fetches the manifest from the local `rabbitmq_stream_s3_server`. To find offsets and timestamps, the reader then performs binary search on the manifest's array of entries. If the target entry is a group, then the reader downloads the group object and recursively continues the search within. Once the reader finds the fragment, it downloads the index section from the end of the fragment object and further binary searches within the index array to find an exact position within the fragment object.

Once the reader has a fragment object and byte offset within, it starts a [`gen_server`] which performs byte range requests within the object to cache data for offset reader to read. As the reader works forward in the stream data, this reader `gen_server` downloads more data with byte range requests. By prefetching aggressively enough, the reader can approach near-local-tier throughput at the cost of memory usage.

[`gen_server`]: https://www.erlang.org/docs/28/apps/stdlib/gen_server.html
