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

**`rabbitmq_stream_s3_remote_reader_core`** is a pure module containing all decision logic: buffer management and reassembly, prefetch sizing and concurrency, fragment transitions, retry/timeout decisions, and error classification. It receives events and returns a new state plus a list of effects. It never performs I/O.

**`rabbitmq_stream_s3_remote_reader`** is the gen_server shell. It translates external events (gun HTTP messages, timer fires, gen_server calls from the log reader) into core events, feeds them to the core, and executes the resulting effects (start S3 requests, set timers, reply to callers, look up the manifest cache).

Effects that require local data (manifest range lookups, iterator refreshes) are resolved synchronously in the shell's effect-execution loop and fed back to the core immediately. This avoids extra async round-trips for what are fast ETS lookups.

### Buffer

The core buffers fragment data in a `rabbitmq_stream_s3_read_buffer`: a queue of the delivered binaries ("blocks", batched to ~1 MiB by the S3 API layer) held as-is in offset order, addressed by absolute byte positions within the fragment object. One buffer holds the current fragment's window; a second accumulates the prefetched next fragment and is moved into place whole at the transition.

This design complements the Erlang VM binary implementation. A naive approach, growing a binary by appending each delivery, interacts badly with an owned binary structure that lends out references. Serving a read as a sub-binary seals it, preventing the runtime from optimizing appends.

- Blocks are contiguous and immutable: `end_pos - start_pos` equals the sum of block sizes, and a delivery is never copied into place (append is O(1)).
- Consumed data is freed block-by-block: reads are non-decreasing, so when a read is served every block entirely below its start offset is dropped. `start_pos` advances block-granularly, never past the last read's start.
- A read pins at most the blocks it overlaps: reads ≤ 512 bytes (chunk-header over-reads are 303 bytes) are copied and pin nothing, however they are taken; larger reads share the blocks they cover, as a sub-binary at each edge.

Reads come back as those blocks. The send path puts them straight into the socket's iolist, so a read spanning blocks costs no copy at all; callers that need one binary to slice - the chunk iterator, record by record - flatten it themselves, concatenating only the requested bytes (chunk-sized, not window-sized), which is rare and cheap once the prefetch window is large relative to chunks.

Because several ranges of a fragment are in flight at once, their responses interleave, but the buffer only accepts contiguous appends. Outstanding ranges are therefore held in an ordered queue that doubles as a reassembly queue: bytes for a range whose predecessors have not finished are staged against that range and appended once it reaches the head. Staged bytes count against the prefetch window, so reassembly cannot grow memory beyond the window bound.

A range that fails, or that is answered with fewer bytes than it asked for, goes back in the queue restarted at the last byte that reached the buffer. Only that range is re-requested: co-pending ranges keep streaming, no bytes are fetched twice, and a hole in the middle of the pipeline cannot be papered over. A range that fails having already delivered every byte it owed - only its closing frame was outstanding - is dropped rather than restarted, since there is nothing left to fetch.

A range in that state that has *not* failed stays queued until its closing frame arrives, and it must not be mistaken for one that is still owed bytes. Whether a queued range can flush is decided from what it still owes, never from where the buffer's end happens to be: the ranges behind it flush past it in the meantime, so the buffer's end moves beyond it and the two positions stop agreeing. For the same reason, where the next range starts is the later of the buffer's end and one past the last range queued - the queue alone can point backwards, since the ranges that flushed past the open one have been dropped from it.

### Prefetch

The starting byte position within a fragment is determined once, at offset-spec resolution time, by the log reader (see [Index lookup within a fragment](#index-lookup-within-a-fragment)), not by the remote reader. Once reading begins, the remote reader issues forward range requests only for the chunk-data region `[8 + start, 8 + Size)` (it never re-fetches the index), reading ahead of the consumer's position and walking chunk headers sequentially within the buffered data.

Several ranges are fetched at once. A single S3 connection transfers at roughly 40 MB/s whatever range size is asked of it, so delivered bandwidth is `W / (TTFB + W/BW)`, which asymptotes to one connection's rate as the range size `W` grows. Only concurrency moves that ceiling, so the request size is fixed (`prefetch_request_size`, 4 MiB) and what adapts is the *prefetch window*: the bytes the reader holds or has asked for ahead of the consumer's read position. New ranges are issued at the fetch frontier while the window has room and fewer than `prefetch_max_depth` (8) requests are in flight.

The window starts at one request and:

- doubles on a buffer miss, capped at `prefetch_window_max` (32 MiB) - a miss means the reader is not fetching far enough ahead, so it is a signal to fetch more, not to back off
- gives one request back after a window's worth of bytes has been served without a miss

Only the first miss for a given read counts: the serve attempt re-runs on every delivery while a read waits, and counting each of those would run the window to its ceiling on one slow read. Growing once per missed read also bounds what a consumer that reads a few records and disconnects can pull, since it only ever misses once or twice.

The window is also the reader's memory bound: it holds or has outstanding at most `prefetch_window_max` plus one request. A consumer that stops reading needs no separate throttle - the buffer fills to the window and no further requests are issued.

The pending-read deadline scales with the read for the same reason. A read cannot complete faster than the tier can deliver the bytes it asked for, so a fixed deadline would cap the chunk size the remote tier can serve - past the cap every attempt expires, and the log reader's retries do not rescue it, since the bytes a retry waits for are the ones that did not arrive in time. The budget is therefore a base plus the bytes to fetch over a pessimistic floor on tier throughput; the read's offset counts towards that as an upper bound rather than a tight one, since a consumer that attaches mid-fragment starts with nothing buffered. The caller's timeout is derived from the same figure so it always exceeds the deadline, which is what keeps a caller from timing out first and leaving a read pending underneath the next one.

The one thing that lifts that bound is the read in hand. The window governs *prefetch*, but a read of N bytes cannot be served while fewer than N are outstanding, so the fetch ceiling rises to whatever the pending read needs and the bound becomes the larger of `prefetch_window_max` and that read, plus one request. Reads are chunk sized, so this only bites when a chunk is larger than the window. Refusing to fetch instead would not bound anything - it would simply never serve the read: at the ceiling the window cannot grow, so nothing further is ever requested, and the read deadline only refetches to the same ceiling and stalls again.

S3 errors do not move the window. They drive a separate exponential retry-delay backoff, and `pool_busy` (the connection pool saturated) drives a third, milder one. Both grow once per retry round rather than once per failed range: a reset connection fails every pipelined range at once, and one fault must cost one doubling.

The two backoffs are independent down to their timers, and a failed range is released only by the timer of the kind that failed it. They measure unrelated conditions three orders of magnitude apart, so sharing either would let the pool's 25 ms clock re-issue a range S3 has just asked the reader to slow down. For the same reason a delivery resets only a backoff nothing is waiting on: with several ranges in flight, S3 answering some while throttling others is what throttling looks like from here, and resetting on every delivered frame would hand back the delay the throttled ranges just earned. A round the clock's timer has just released counts as waiting on it too, until the ranges it put back on the wire have answered - otherwise the first of them S3 answers resets the clock and the ones it throttles in the same breath start a fresh minimum round, and the delay never grows however long the throttling lasts.

A read deadline expiring drops every outstanding range, so both timers are cancelled with them, and a message either had already left is dropped on arrival - by its token, since the batch that cancels a kind's timer can arm a fresh one for that same kind. What has already been buffered is kept: those bytes are a contiguous run of the current fragment that nothing in flight contributed to, so the retry is usually a read the buffer answers outright, and the fetch frontier - which is read off the buffer's end - resumes where the reader had got to rather than at the fragment's first byte. A timer carries the delay it was armed with - up to `max_retry_delay_ms` - so one left running would land part-way through a later backoff round and release that round's ranges before the pause they earned had elapsed.

The bytes prefetched for the next fragment are dropped as well. They count against the window like any other, and the ranges that were filling them have just been cancelled, so keeping them would hold window space nothing is left to fetch into: with the window at its ceiling no range would ever be issued again and every later read would wait out a deadline of its own.

### Fragment transitions

Once every byte of the current fragment has been asked for, the fetch frontier spills into the next one rather than waiting for the current fragment to arrive first, so the window stays full across a boundary instead of draining at every fragment. The reader looks one fragment ahead and no further: it holds the current fragment's buffer and one prefetched next-fragment buffer.

Looking ahead means advancing the fragment iterator, which downloads a group object when the next entry sits behind a group node. That answer is memoised, along with the advanced iterator, until the iterator is replaced at the next transition, so a fragment behind a group node costs one GET rather than one per frame the reader receives while sitting on its predecessor.

The pass that places the reader on a fragment is the one exception: it issues that fragment's ranges but never looks ahead, and the first read or delivery spills. The reader is started with `gen_server:start/3`, so its `init` runs while the *consumer* process waits, and the look-ahead's GET would block the consumer rather than the reader. It is reachable there whenever the fragment has less than one request left in it - a consumer attaching near a fragment's end, or to a short one.

A group fetch that fails transiently is memoised too, as *failed* rather than as an answer, and the look-ahead arms the retry backoff itself. Nothing else on that path would: the ranges already queued are healthy, so no request error runs. That memo records only that the last attempt failed - whether to attempt again is read off the fault clock rather than stored beside it, so the memo is honoured while that clock is armed and re-attempted once it is not. Something has to pace the attempts, since each is a synchronous GET that blocks the reader, and that clock is what paces every other retry here. The pool's much shorter clock cannot stand in: it fires for as long as the pool has no free connection, which says nothing about whether the group object can be fetched. The ranges in flight are left alone when this happens: the failure is in advancing the iterator, which says nothing about the fragment GETs already on the wire, and cancelling one closes its pooled connection.

When the consumer then reads past the end of the current fragment:

1. The prefetched next fragment's buffer is moved into place whole, and its `#fragment_ref{}` (its `offset`, `uid`, and `size`) becomes the current one.
2. Any range still outstanding against the fragment being left behind is cancelled, so it cannot block reassembly of the new one.
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
