# Read Path

This document describes how consumers read data that spans local and remote tiers. After reading it you will know: how offset specs are resolved, how the log reader routes between tiers, how the remote reader prefetches data, and how the fragment iterator navigates the manifest tree.

## Overview

A consumer subscribes to a stream with an offset spec (`first`, `last`, `next`, an absolute offset, or a timestamp). The log reader resolves this spec to a position in either the local tier (segment file + byte offset) or the remote tier (fragment + byte position within the fragment). Reading then proceeds forward from that position.

```
  Consumer
     │
     ▼
  rabbitmq_stream_s3_log_reader (osiris_log_reader behaviour)
     │
     ├── Local mode: delegates to osiris_log (pread from segment files)
     │
     └── Remote mode: delegates to rabbitmq_stream_s3_remote_reader
            │
            └── HTTP range requests to S3 fragment objects
```

## Offset spec resolution

Resolution determines which tier holds the requested offset and where within that tier to start reading.

The log reader checks the manifest cache (`rabbitmq_stream_s3_manifest_replica:get_range/1`) to get the remote tier's `{first_offset, next_offset}`. It also checks the local log's first chunk ID via shared atomics.

Decision logic:

- If the offset is below the local first chunk ID and within the manifest's range: resolve to remote.
- If the offset is at or above the local first chunk ID: resolve to local (delegate to `osiris_log`).
- If the offset is below both the local range and the manifest range: the data has been retained away. Return `{error, {offset_out_of_range, ...}}`.

For timestamp-based specs, the log reader binary-searches the manifest's entries (each entry has `first_timestamp` and `last_timestamp`) to find the fragment containing the target timestamp.

## Remote mode

When resolution points to the remote tier, the log reader:

1. Finds the fragment containing the target offset by binary-searching the manifest entries.
2. Constructs a `#remote_location{}` containing the fragment ref, byte position within the fragment, and a fragment iterator positioned at the current entry.
3. Spawns a `rabbitmq_stream_s3_remote_reader` gen_server linked to the caller.
4. Returns the reader state in remote mode.

Subsequent reads (`send_file/3`, `chunk_iterator/3`) are forwarded to the remote reader process.

## Remote reader

The remote reader is split into a functional core and a gen_server shell, following the same pattern as the upload path (`replica_reader` / `replica_reader_core`).

**`rabbitmq_stream_s3_remote_reader_core`** is a pure module containing all decision logic: buffer management, AIMD prefetch sizing, fragment transitions, retry/timeout decisions, and error classification. It receives events and returns a new state plus a list of effects. It never performs I/O.

**`rabbitmq_stream_s3_remote_reader`** is the gen_server shell. It translates external events (gun HTTP messages, timer fires, gen_server calls from the log reader) into core events, feeds them to the core, and executes the resulting effects (start S3 requests, set timers, reply to callers, look up the manifest cache).

Effects that require local data (manifest range lookups, iterator refreshes) are resolved synchronously in the shell's effect-execution loop and fed back to the core immediately. This avoids extra async round-trips for what are fast ETS lookups.

### Prefetch

When the remote reader opens a fragment, it issues a range request for the index (at the end of the fragment object, starting at byte `8 + Size`). The index tells it where each chunk starts within the fragment. It then issues range requests for the data, reading ahead of the consumer's position.

Read size grows using AIMD (additive increase, multiplicative decrease): successful reads grow the window additively, while errors or slow responses shrink it multiplicatively. This avoids wasting bandwidth on consumers that read a few records and disconnect, while allowing sustained sequential readers to approach local-tier throughput.

### Fragment transitions

When the consumer reads past the end of the current fragment:

1. The remote reader calls `next/1` on the fragment iterator to get the next fragment's `{offset, uid, size}`.
2. It constructs the S3 key and begins prefetching the next fragment.
3. Reading continues seamlessly from the new fragment.

If the iterator returns `end_of_manifest`, the remote reader requests a fresh iterator from the manifest cache. If the fresh iterator has entries (new fragments uploaded since the previous iterator was created), reading continues from remote. If not, the remaining data is in the local tier.

### Transition to local

When the remote reader exhausts the manifest and no new fragments are available, the log reader transitions to local mode. It opens a local offset reader at the current offset and continues reading from segment files. The remote reader process exits.

This transition is transparent to the consumer. The stream of chunks is continuous.

### Handling 404

If a GET returns 404 (fragment deleted by retention between iterator creation and fetch):

1. The remote reader marks the current fragment as not found.
2. On the next read attempt, it checks the manifest cache for the current range.
3. It repositions at the oldest available offset (`first_offset` from the manifest).
4. Reading continues from the new position.

The consumer sees a gap (offsets between the deleted fragment and the oldest available are skipped). This is the same behavior as vanilla streaming retention: consumers are repositioned at the oldest available data.

## Fragment iterator

The fragment iterator (`rabbitmq_stream_s3_fragment_iterator`) provides forward iteration over the manifest tree's leaf entries (fragments). It hides the tree structure from the remote reader.

### Interface

```erlang
-spec init(#manifest{}, osiris:offset(), get_group_fun()) -> iterator().
-spec next(iterator()) ->
    {ok, {osiris:offset(), uid(), Size}, iterator()}
    | end_of_manifest
    | {error, {group_fetch_failed, term()}}.
```

### Internal structure

The iterator holds:
- A position (index) in the current entries array.
- The current entries array (a slice of the root, or a group's entries).
- A stack of `{parent_array, parent_index}` for ascending back up the tree after exhausting a group.
- A `get_group_fun` for lazily downloading group objects when the iterator descends into a branch.

Walking forward: advance the index. If the next entry is a fragment, return it. If it is a group, descend (download the group object, push the current position onto the stack, iterate the group's entries). If the array is exhausted, pop the stack and continue in the parent. If the stack is empty and the root is exhausted, return `end_of_manifest`.

### Properties

- Hides tree structure. The remote reader does not know whether entries live in the root, a group, or a mega-group.
- Lazy group fetching. Group objects are only downloaded when the iterator reaches them. Readers near the tail never touch groups.
- Snapshot semantics. The iterator is a point-in-time view. New fragments appended after creation are not visible until a fresh iterator is requested.
- Independently testable. The module is pure (given a mockable `get_group_fun`) and tested against manifest structures of any depth without S3.

## Index lookup within a fragment

Once the remote reader has the fragment data, it needs to find the byte position of a specific offset within the fragment. The index (at the end of the fragment object) contains 20-byte records:

```
offset:64, timestamp:64, fragment_position:32
```

The remote reader binary-searches the index for the target offset and reads chunk data starting at the returned `fragment_position`. This is the same algorithm as local reads (binary search the index, pread the segment), just over HTTP range requests instead of file I/O.

## Counters

The log reader maintains seshat counters for observability:

- `remote_init` / `local_init`: how many readers start in each mode.
- `remote_close` / `local_close`: reader teardown by mode.
- `resolve_remote_*`: breakdown of which offset specs resolve to remote (first, next, last, offset, timestamp).
- `resolve_local`: offset specs that resolve to local.
- `resolve_duration_ms`: total time spent in resolution.
- `resolve`: total number of resolutions.
