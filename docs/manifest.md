# Manifest

The _manifest_ is a tree structure represented by objects in S3. A small number of these objects are also cached in memory by all stream members to enable fast access and modifications. Each stream has its own manifest. The manifest is tracked for two purposes:

* **Resolving offset specs**: consumers may start reading a stream from a requested offset or timestamp. The manifest must provide cheap lookup to find the fragment which contains a requested offset or timestamp.
* **Retention**: when a stream grows larger than its configured max-bytes or fragments become older than the configured max-age, the fragments should be deleted from the remote tier. The manifest should track enough metadata about each fragment so that determining this information by expensive remote-tier queries is unnecessary.

## Data structure

### Metadata array

At a high level, the manifest tracks a sorted array of metadata about every fragment in the stream. We can imagine the manifest like so:

```jsonc
{
  "total_size": 75753861427,
  "entries": [
    {"first_offset": 0, "first_timestamp": "2026-01-26T04:30:00Z", "last_timestamp": "2026-01-26T05:00:00Z", "size": 64000000},
    {"first_offset": 123456, "first_timestamp": "2026-01-26T05:00:00Z", "last_timestamp": "2026-01-26T05:30:00Z", "size": 65000000},
    {"first_offset": 234567, "first_timestamp": "2026-01-26T05:30:00Z", "last_timestamp": "2026-01-26T06:00:00Z", "size": 63000000},
    // ...
  ]
}
```

_JSON is just for display - the actual manifest uses a compact binary representation. See the [Representation section](#representation) below._

As new objects are uploaded, they are appended to the array in ascending order. This array can be quickly binary searched to find a given offset or timestamp. And we can quickly check the head of the array to see if max-age retention should delete the first fragment. We could also track the total size of the stream alongside this array to make max-bytes retention quick. The offset metadata about a fragment acts as a kind of pointer - the offset plus stream ID gives us enough information to create the fragment object's key.

The problem with a single array is how it grows in size as the stream gets longer. If each entry cost 30 bytes (say 8 byte offset, two 8 byte timestamps, 6 bytes for size), a 50 TB stream would have an array size around 25 MB. While this doesn't sound like much for a very large stream, this array must be kept in memory for access and modifications. Megabyte size objects become expensive in terms of memory and network bandwidth footprint.

### Factoring out groups

Streams are typically accessed near the tail (most recently written data). We can factor out the oldest `m` entries in the array into a new object called a _group_, replacing their array entries in the root with metadata about the group. This naturally keeps the root array and the group arrays sorted, so we can continue to binary search. We can't easily find the oldest timestamp, though, so we can track that on the root object.

```jsonc
{
  // Root-level metadata to track to make retention cheap:
  "oldest_last_timestamp": "2026-01-26T05:00:00Z",
  "total_size": 75753861427,
  // Then the root array:
  "entries": [
    {"kind": "group", "first_offset": 0, "first_timestamp": "2026-01-26T04:30:00Z", "last_timestamp": "2026-01-30T05:00:00Z"},
    {"kind": "group", "first_offset": 12354168, "first_timestamp": "2026-01-30T05:00:00Z", "last_timestamp": "2026-02-02T05:00:00Z"},
    {"kind": "fragment", "first_offset": 23465279, "first_timestamp": "2026-02-02T05:00:00Z", "last_timestamp": "2026-02-02T05:30:00Z", "size": 65000000},
    // ...
  ]
}
```

With this layout we update the root-level information necessary for retention and avoid storing metadata about all fragments. Only the oldest `m` fragments are factored out into groups, so recently written stream data is always accessible at the end of the entries array. To lookup older data we first determine which group object to search, then download the group object and search within.

Manifests of small streams only have a root object which points directly to fragments.

![Root-only manifest](./Manifest-Tree-Small.drawio.svg)

Larger streams have the earliest sections of the array factored out into groups.

![Manifest with groups](./Manifest-Tree-Medium.drawio.svg)

### Kilo-groups and mega-groups

To support practically infinitely long streams, we can factor out `m` groups into a kilo-group. And once we have `m` kilo-groups, they can be refactored out into a mega-group. This recursive 'factoring out' makes a tree structure which can support a very high capacity with a low memory footprint and quick search.

![Manifest with kilo-groups](./Manifest-Tree-Large.drawio.svg)

### Choosing `m`

An `m` which is too small increases the number of requests to the remote tier during factoring and search, but reduces memory footprint. And vice versa for an `m` which is too large. We're using `1024` as a nice round power of two, though this could be tuned in the future. With `m=1024` and 30 bytes for each array entry, each (kilo-/mega-)group object only takes 30 kiB, which is small enough to fit into memory comfortably even when there are many streams. The root can grow beyond this, but remains small anyways. Small streams (less than 1024 fragments) do not need any groups at all and can store all metadata in the root. If `m=1024` and mega-groups are the largest kind of group, a 'fully loaded' root of 1024 mega-groups points to 1024^4 fragments. At an average fragment size of 64 MiB this covers 70 EiB of data in a single stream, which is a large enough amount that publishing it is very difficult in the first place.

A large `m` makes lookup fast. With mega-groups being the largest kind of group, lookup within even EiB of data would make 3 round-trips to the remote tier to determine the fragment for a given offset or timestamp. The root is cached, then one round trip gets the mega-group, another gets the right kilo-group, another gets the right group and then search within that group finds the right fragment. If the remote tier has reasonably low latency (10-100ms) then lookup is always sub-second.

### Complexity

`n` is the length of a stream.

| Characteristic                        | Complexity | Notes
|---                                    |---         |---
| Space                                 | O(n)       | The manifest tracks all fragments uploaded for a stream. The oldest objects are deleted when the oldest fragments in the stream are deleted by retention.
| Memory footprint                      | O(1)       | The tree's root is held in memory. The root has an upper bound on size and factoring out groups keeps the practical size small. The first group may also be cached in memory to make repeated retention evaluations cheaper.
| Lookup (offset/timestamp resolution)  | O(log(n))  | The tree structure makes descending from object to object cheap. Each manifest object is sorted by offset/timestamp ascending, so offsets/timestamps can be found within objects with binary search. Lookups of older data take longer while lookup of recently-written data is cheaper.
| Calculate total stream size           | O(1)       | Total size is stored in the manifest root and updated gradually as fragments are added or removed from the manifest.
| Find oldest fragment's last timestamp | O(1)       | Oldest fragment's last timestamp is also tracked in the root and is updated when retention removes fragment(s).

### Representation

Manifest objects are serialized in a custom binary format to minimize memory and network footprint. This representation is not guaranteed to be stable - it might change over time. Stream readers should rely on RabbitMQ to read and write these objects instead of interacting with the remote tier objects directly.

<details><summary>Root...</summary>

```
|0              |1              |2              |3              | Bytes
|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
+---------------+---------------+---------------+---------------+
| Manifest magic (0x4f534952 = "OSIR")                          |
+---------------------------------------------------------------+
| Manifest version (0x00000001)                                 |
+---------------------------------------------------------------+
| First offset (u64)                                            |
|                                                               |
+---------------------------------------------------------------+
| Next offset (u64)                                             |
|                                                               |
+---------------------------------------------------------------+
| First timestamp (i64)                                         |
|                                                               |
+---------------------------------------------------------------+
| First-last timestamp (i64)                                    |
|                                                               |
+----+----------------------------------------------------------+
+ 0  + Total size (u70)                                         |
+----+                                                          |
|                                                               |
|               +-----------------------------------------------|
+---------------+                                               |
| Contiguous list of entries                                    |
: (until EOF)                                                   :
:                                                               :
+---------------------------------------------------------------+
```

---

</details>

<details><summary>Array entries...</summary>

Fragment entry:

```
|0              |1              |2              |3              | Bytes
|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
+---------------+---------------+---------------+---------------+
| First offset (u64)                                            |
|                                                               |
+---------------------------------------------------------------+
| First timestamp (i64)                                         |
|                                                               |
+---------------------------------------------------------------+
| Last timestamp (i64)                                          |
|                                                               |
+----+-+--------------------------------------------------------+
+  0 +i+ Size (u45)                                             |
+----+-+--------------------------------------------------------+
i: boolean, 1 = is sequence zero of source segment, 0 otherwise
```

Group entry:

```
|0              |1              |2              |3              | Bytes
|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
+---------------+---------------+---------------+---------------+
| First offset (u64)                                            |
|                                                               |
+---------------------------------------------------------------+
| First timestamp (i64)                                         |
|                                                               |
+---------------------------------------------------------------+
| Last timestamp (i64)                                          |
|                                                               |
+------+--------------------------------------------------------+
+ Kind + UID (u46)                                              |
+------+--------------------------------------------------------+
```

---

</details>

<details><summary>Group...</summary>

```
|0              |1              |2              |3              | Bytes
|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
+---------------+---------------+---------------+---------------+
| Group  magic                                                  |
+---------------------------------------------------------------+
| Group version (0x00000001)                                    |
+---------------------------------------------------------------+
| First offset (u64)                                            |
|                                                               |
+---------------------------------------------------------------+
| First timestamp (i64)                                         |
|                                                               |
+---------------------------------------------------------------+
| 0 (u72)                                                       |
|                                                               |
|                                                               |
|               +-----------------------------------------------|
+---------------+                                               |
| Contiguous list of entries                                    |
: (until EOF)                                                   :
:                                                               :
+---------------------------------------------------------------+
```

---

</details>

## Operations

When the next fragment of stream data has been successfully uploaded, the coordination server `rabbitmq_stream_s3_server` appends the fragment's metadata to its in-memory copy of the root and evaluates whether the root has grown too large. The server debounces uploads of the updated root to the remote tier to keep requests-per-second low. Once the root is too large, the server uploads a new group object and once that has been successfully uploaded, the server replaces the fragments in its cached copy of the root with the new group's metadata.

Periodically the server evaluates whether the stream's retention rules should delete fragments. If the root has no groups, the server decides which fragments to delete, removes them from the root array, and deletes them in the background. If there are groups, the server downloads the first group object and evaluates retention against the fragments within. Once that process completes, the server updates the total-size and oldest-last-timestamp metadata in the root. Group objects are never updated after being written, but once all fragments pointed to by a group (or all groups pointed to by a kilo-group, etc.) are deleted, the group object is deleted and removed from the root.

Changes to the manifest are made only by the server on the stream writer's node. Nodes with replica copies receive metadata about changes to each manifest through a generic edit type which can express the three kinds of changes: 1/ appending new fragments, 2/ factoring out groups and 3/ changes caused by retention. Since all stream members know information about the data in the remote tier, all members can perform local retention to evict fully uploaded segments aggressively.

### Resolving the manifest

When a writer or replica starts up for a stream, `rabbitmq_stream_s3_server` must download the manifest root from the remote tier to determine what local data needs to be uploaded and what is redundant. Since uploads of the manifest are debounced, the last writer may have shut down before it uploaded an updated copy. _Resolving_ the manifest is downloading the current root from the remote tier and then attempting to find any fragments which were uploaded by the last writer. The _resolving_ process reads the header of the last fragment in the root to find the next fragment's offset, and then repeats that process to discover any fragments which were successfully uploaded but not yet applied to the manifest. These fragments are appended to the end of the in-memory copy of the root.

## Concurrency control

A network partition might elect a new leader on the majority side while a deposed writer continues running on the minority side. Both writers might have access to S3, so both writers may modify the manifest. `rabbitmq-stream-s3` uses an [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) to avoid conflicting writes. Keys of all metadata objects include a unique identifier string (UID). In practice the metadata directory looks like this:

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

The UID of the manifest root is stored in [Khepri](https://github.com/rabbitmq/khepri) (RabbitMQ's metadata store) associated with the stream name. The Khepri tree looks like this:

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
