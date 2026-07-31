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

The log reader checks the manifest cache (`rabbitmq_stream_s3_manifest_replica:get_manifest/1`) for the remote tier's extent. It also checks the local log's first chunk ID via shared atomics. The cache row is in one of two states, and the decision depends on the state, not just the extent (see the Cached state section of [invariants.md](./invariants.md)):

Decision logic with a resolved manifest:

- If the offset is at or above the local first chunk ID: resolve to local (delegate to `osiris_log`). The cache is not consulted; `last` and `next` also short-circuit to local.
- If the offset is below the local first chunk ID and within the manifest's range: resolve to remote.
- If the offset is below both the local range and the manifest range: the data has been retained away. Attach at the oldest available data, matching vanilla stream retention behavior.

Decision logic without one:

- If the row is `pending` (the plugin is attached on this node but the manifest has not been resolved or synced yet, for example in the first moments after a member starts) or there is no row at all (the pending marker was never written, or has not landed yet): fail closed with `osiris_log_reader`'s `{error, unavailable}` for any spec the remote tier may hold (`first`, a below-floor offset, a timestamp, `{abs, _}`). The subscription fails and the client retries; falling back to the local tier here would silently skip the remote range below the local floor. The `resolve_failed_closed` counter records these. A missing row is never treated as "un-tiered stream, local is exact": tiering is unconditional and plugin-wide, so there is no stream a missing row could confirm as un-tiered, and a genuinely brand-new stream is resolved to an explicit empty manifest (not left with a missing row) by the writer's replica reader before any reader can observe it.

For timestamp-based specs, the log reader binary-searches the manifest's entries (each entry has `first_timestamp` and `last_timestamp`) to find the fragment containing the target timestamp.

## Remote mode

When resolution points to the remote tier, the log reader:

1. Finds the fragment containing the target offset by binary-searching the manifest entries.
2. Constructs a `#remote_location{}` containing the fragment ref, byte position within the fragment, and a fragment iterator positioned at the current entry.
3. Spawns a `rabbitmq_stream_s3_remote_reader` gen_server (unlinked; the remote reader monitors the caller and stops when it exits).
4. Returns the reader state in remote mode.

Subsequent reads (`send_file/3`, `chunk_iterator/3`) are forwarded to the remote reader process.

## Remote reader

The remote reader is split into a functional core and a gen_server shell, following the same pattern as the upload path (`replica_reader` / `replica_reader_core`).

**`rabbitmq_stream_s3_remote_reader_core`** is a pure module containing all decision logic: buffer management, AIMD prefetch sizing, fragment transitions, retry/timeout decisions, and error classification. It receives events and returns a new state plus a list of effects. It never performs I/O.

**`rabbitmq_stream_s3_remote_reader`** is the gen_server shell. It translates external events (gun HTTP messages, timer fires, gen_server calls from the log reader) into core events, feeds them to the core, and executes the resulting effects (start S3 requests, set timers, reply to callers, look up the manifest cache).

Effects that require local data (manifest range lookups, iterator refreshes) are resolved synchronously in the shell's effect-execution loop and fed back to the core immediately. This avoids extra async round-trips for what are fast ETS lookups.

### Buffer

The core buffers fragment data in a `rabbitmq_stream_s3_read_buffer`: a queue of the delivered binaries ("blocks", batched to ~1 MiB by the S3 API layer) held as-is in offset order, addressed by absolute byte positions within the fragment object. One buffer holds the current fragment's window; a second accumulates the prefetched next fragment and is moved into place whole at the transition.

This design complements the Erlang VM binary implementation. A naive approach, growing a binary by appending each delivery, interacts badly with an owned binary structure that lends out references. Serving a read as a sub-binary seals it, preventing the runtime from optimizing appends.

- Blocks are contiguous and immutable: `end_pos - start_pos` equals the sum of block sizes, and a delivery is never copied into place (append is O(1)).
- Consumed data is freed block-by-block: reads are non-decreasing, so when a read is served every block entirely below its start offset is dropped. `start_pos` advances block-granularly, never past the last read's start.
- A read pins at most the blocks it overlaps: reads ≤ 512 bytes (chunk-header over-reads are 303 bytes) are copied and pin nothing; larger single-block reads share a sub-binary of one block; reads spanning blocks are assembled into a fresh binary.

Reads spanning blocks concatenate only the requested bytes (chunk-sized, not window-sized), which is rare and cheap once the AIMD window is large relative to chunks.

### Prefetch

The starting byte position within a fragment is determined once, at offset-spec resolution time, by the log reader (see [Index lookup within a fragment](#index-lookup-within-a-fragment)), not by the remote reader. Once reading begins, the remote reader issues forward range requests only for the chunk-data region `[8 + start, 8 + Size)` (it never re-fetches the index), reading ahead of the consumer's position and walking chunk headers sequentially within the buffered data.

Read size grows using AIMD (additive increase, multiplicative decrease). The window starts at `initial_read_size` (4 MiB). After a run of consecutive buffer hits it grows additively (by 1 MiB, capped at `read_size_max`, 64 MiB); a buffer miss (the consumer outrunning the prefetch) halves it (floored at `read_size_min`, 1 MiB). The decrease is driven by buffer misses, not by S3 errors, which instead drive a separate exponential retry-delay backoff. This avoids wasting bandwidth on consumers that read a few records and disconnect, while allowing sustained sequential readers to approach local-tier throughput.

A read deadline expiring drops every outstanding range, so both timers are cancelled with them, and a message either had already left is dropped on arrival — by its token, since the batch that cancels a kind's timer can arm a fresh one for that same kind. A timer carries the delay it was armed with — up to `max_retry_delay_ms` — so one left running would land part-way through a later backoff round and release that round's ranges before the pause they earned had elapsed.

The bytes prefetched for the next fragment are dropped as well. They count against the window like any other, and the ranges that were filling them have just been cancelled, so keeping them would hold window space nothing is left to fetch into: with the window at its ceiling no range would ever be issued again and every later read would wait out a deadline of its own.

### Fragment transitions

When the consumer reads past the end of the current fragment:

1. The remote reader calls `next/1` on the fragment iterator to get the next fragment as a `#fragment_ref{}` (its `offset`, `uid`, and `size`).
2. It constructs the S3 key and begins prefetching the next fragment.
3. Reading continues seamlessly from the new fragment.

If the iterator returns `end_of_manifest`, the remote reader requests a fresh iterator from the manifest cache. If the fresh iterator has entries (new fragments uploaded since the previous iterator was created), reading continues from remote. If not, the remaining data is in the local tier.

### Transition to local

When the remote reader exhausts the manifest and no new fragments are available, the log reader transitions to local mode. It opens a local offset reader at the current offset and continues reading from segment files. The remote reader process exits.

This transition is transparent to the consumer. The stream of chunks is continuous.

### Handling 404

If a GET returns 404 (fragment deleted by retention between iterator creation and fetch):

1. The remote reader marks the current fragment as not found and stops fetching: every range it still holds is dropped and no new one is issued, since the fetch frontier points into an object that is gone. Reads that the bytes already buffered can serve are still served from them.
2. On the first read attempt those bytes cannot serve, it checks the manifest cache for the current range.
3. It repositions at the oldest available offset (`first_offset` from the manifest).
4. Reading continues from the new position.

The consumer sees a gap (offsets between the deleted fragment and the oldest available are skipped). This is the same behavior as vanilla streaming retention: consumers are repositioned at the oldest available data.

## Fragment iterator

The fragment iterator (`rabbitmq_stream_s3_fragment_iterator`) provides forward iteration over the manifest tree's leaf entries (fragments). It hides the tree structure from the remote reader.

### Interface

```erlang
-spec init(#manifest{}, osiris:offset(), get_group_fun()) -> iterator().
-spec next(iterator()) ->
    {ok, #fragment_ref{}, iterator()}
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

To start reading a fragment at a specific offset, the byte position of that offset within the fragment must be found. This happens once, during offset-spec resolution in the log reader (`find_position/3`), not on every read in the remote reader. The log reader issues a single range request for the index at the tail of the fragment object (starting at byte `8 + Size`), which contains 20-byte records:

```
offset:64, timestamp:64, fragment_position:32
```

It binary-searches the index for the target offset and hands the resulting `fragment_position` to the remote reader as the starting position. This is the same algorithm as local reads (binary search the index, then read forward), just over an HTTP range request instead of file I/O. After this starting position is established, the remote reader reads chunk data forward and walks chunk headers sequentially; it does not consult the index again for that fragment.

## Counters

The log reader maintains seshat counters for observability:

- `remote_init` / `local_init`: how many readers start in each mode.
- `remote_close` / `local_close`: reader teardown by mode.
- `resolve_remote_*`: breakdown of which offset specs resolve to remote (first, next, last, offset, timestamp).
- `resolve_local`: offset specs that resolve to local.
- `resolve_duration_ms`: total time spent in resolution.
- `resolve`: total number of resolutions.
