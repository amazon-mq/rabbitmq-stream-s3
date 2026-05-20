# Upload Path

The remote replica reader owns the upload path end to end. This document describes how data moves from the local log to the remote tier and when that data becomes visible to the rest of the system.

## Durability boundary

A fragment object in S3 is not meaningful on its own. It becomes part of the stream's remote tier only when a manifest referencing it has been persisted. Until that point the fragment is an orphan that will be cleaned up by garbage collection.

The persist sequence is:

1. Fragment uploaded to S3 (PUT with UID-bearing key).
2. Manifest root PUT to S3 (new UID, references the fragment).
3. Khepri conditional write (epoch + revision precondition).

Only after step 3 succeeds does the data exist from the perspective of consumers, retention, and the range table. Before that:

- A crash loses the in-memory manifest. The fragment becomes an orphan.
- Consumers cannot see the data (the manifest cache is not updated).
- Retention cannot reclaim local segments covering that data.
- The range table does not advance.

This ordering is the fundamental correctness invariant of the upload path. Every downstream effect (manifest cache update, range table advance, retention evaluation, replica broadcast) is gated on the persist completing successfully.

### In-memory manifest vs persisted manifest

The remote replica reader maintains an in-memory manifest that advances as transfer completions arrive. This is speculative state used to track progress and detect when a persist should be triggered. It is not authoritative.

The authoritative state is the last successfully persisted manifest (stored in S3 and anchored in Khepri). On crash the replica reader resolves this manifest from the remote tier and resumes from its `next_offset`. Any fragments uploaded but not persisted are orphans.

### Terminology note

"Persist" in this document refers exclusively to the remote tier operation (manifest written to S3 + anchored in Khepri). "Commit" refers exclusively to the local tier operation (majority replication, commit offset advance). These are different durability boundaries with different latency characteristics. See [GLOSSARY.md](./GLOSSARY.md) for precise definitions.

## Drain loop

The drain loop is the inner loop of the remote replica reader. It runs on each `{osiris_offset, ...}` notification from the writer.

1. Call `osiris_log:read_header/1` to get the next committed chunk's metadata (48 bytes: offset, timestamp, num_records, data_size, position).
2. Feed the chunk metadata to the fragment assembly, which tracks cumulative size, offsets, timestamps, and segment spans.
3. If the assembly says cut (payload bytes exceed the fragment target size), seal the fragment. Call `core:fragment_cut/2` which returns a reference and a `{submit_transfer, ...}` effect.
4. Execute the effect: submit the transfer to the governor.
5. Create a fresh assembly and continue draining.
6. When `read_header` returns `end_of_stream`, re-register the offset listener and wait.

The drain loop reads only chunk headers (48 bytes each), not chunk bodies. Bodies are read at upload time via `pread` from the segment file. This keeps memory usage negligible during the drain.

### Offset listener re-registration

The offset listener is one-shot. Between `end_of_stream` and re-registration, the committed offset may advance. After re-registering, the replica reader immediately checks whether more data is available and drains again if so. This prevents hanging when a commit races with re-registration.

## Fragment assembly

The fragment assembly (`rabbitmq_stream_s3_fragment_assembly`) tracks the in-progress fragment:

- First offset, next offset, first timestamp, last timestamp
- Cumulative payload size (data_size of all chunks)
- Number of chunks
- Segment spans: `[{segment_offset, start_pos, end_pos}]`
- Index records (20 bytes per chunk: offset, timestamp, position within fragment)

A fragment is cut when `payload_size >= fragment_target_size`. Fragments may span segment boundaries. The assembly handles segment transitions transparently because `read_header/1` follows segment rolls internally.

## Governor interaction

The drain loop must not block on S3 transfers. At 500 MB/s of committed data with a 64 MiB fragment target, a new fragment is cut every 130ms. Transferring 64 MiB to S3 takes 500ms to 2s depending on conditions. If the drain loop waited for each upload to complete, it would fall behind the commit offset, delay offset listener re-registration, and create a growing backlog of unuploaded data.

Additionally, a node may host thousands of streams. Without coordination, each stream's replica reader would independently spawn upload tasks, potentially saturating the network interface with concurrent PUTs. The governor centralizes rate control without centralizing the drain logic.

The per-node governor (`rabbitmq_stream_s3_governor`) uses a token bucket to pace bytes sent to S3. The bucket refills at a configured rate (default unlimited). Transfer tasks consume tokens as they stream bytes. When the bucket is empty, tasks pause until tokens are available.

The interaction from the replica reader's perspective:

1. The shell submits the transfer to the governor: an opaque function (the upload closure), the byte size, and a reply-to reference.
2. The governor paces the transfer via its token bucket and spawns a task when tokens are available.
3. The task executes the upload function (pread from segment files, stream to S3).
4. On completion, the governor sends `{transfer_result, Ref, {ok, Uid}}` back to the replica reader.
5. The shell calls `core:transfer_complete/3`. The core applies the completion in cut order.

Multiple fragments can be in flight simultaneously. The core tracks them in an ordered queue and applies completions in cut order, buffering out-of-order arrivals. This means the manifest always advances contiguously even though S3 latency varies per request.

## Functional core

The async upload path creates complex state: an ordered queue of in-flight transfers, a persist state machine (idle / timer-running / in-flight), pending out-of-order completions, waiters blocked on `await_offset`, and retention edits. These interact in many ways. A transfer can complete while a persist is in flight. A persist can fail while completions are buffered. A waiter can be satisfied by a completion that also triggers a persist.

Testing these interleavings in a gen_server requires mocking the governor, faking S3 responses, and manipulating timers. Every new interleaving is a new integration test with setup cost and potential flakiness.

The replica reader splits this problem. The core module (`rabbitmq_stream_s3_replica_reader_core`) is pure: it takes events and state, returns `{NewState, [Effect]}`. No processes, no I/O, no timers, no process dictionary. The shell (`rabbitmq_stream_s3_replica_reader`) is a gen_server that receives OTP messages, calls into the core, and executes the returned effects.

Effects are tuples the shell interprets: `{submit_transfer, ...}`, `{start_persist, ...}`, `{update_range, ...}`, `{broadcast, ...}`, `{evaluate_retention, ...}`, `{reply_waiters, ...}`. The core never performs I/O directly.

This means every interleaving is testable as a sequence of function calls with assertions on the returned effects. No mocks, no timing, no flakiness. The `replica_reader_core_SUITE` exercises all interleavings this way: out-of-order completions, persist triggers, failures, gaps, waiters, and timer ticks.

The shell is deliberately thin. Its job is message dispatch, effect execution, and owning the osiris log handle. The shell tests (`replica_reader_SUITE`) verify integration. The core tests verify logic.

## Upload mechanics

The upload function (executed inside the governor's task):

1. Generate a random UID for the fragment key.
2. Open a streaming PUT to S3 with `Content-Length = 8 + Size + (num_chunks * 20)`.
3. Write the 8-byte header (`"OSIF"` + version).
4. For each segment span: open the segment file, `pread` the byte range, stream to S3. Compute CRC as bytes flow.
5. Write the index records (20 bytes each: offset, timestamp, position within fragment object).
6. Finish the PUT with the CRC32 checksum for server-side verification.

The bytes are page-cache-hot in the common case (the writer recently wrote them). The upload is a single whole-object PUT, not multipart.

## Manifest persist

The core triggers a manifest persist when either:
- `since_persist >= persist_threshold` (default 5 fragments), or
- `persist_interval_ms` has elapsed since the last persist (default 2000ms).

Only one persist is in flight at a time. Persist is also deferred while a rebalance group upload is in flight. The rebalance must complete first so the persisted manifest includes the rebalanced root. Once the group upload completes and the rebalance edit is applied, persist proceeds normally.

The sequence:

1. Shell spawns a task that PUTs the manifest root to S3 (new UID).
2. Task writes to Khepri with epoch + revision precondition.
3. On success: sends `{persist_result, {ok, Revision}}` back to the shell.
4. Shell calls `core:persist_complete/2`. Core returns effects:
   - `{update_range, ...}`: update manifest cache and shared atomics.
   - `{broadcast, ...}`: send sequenced edits to replica nodes.
   - `{evaluate_retention, ...}`: run local and remote retention.

If the Khepri write fails with a conflict (another writer won), the core returns `{reinitialize}`. The shell re-resolves the manifest from S3 and restarts.

## Startup and manifest resolution

On startup, the remote replica reader:

1. Resolves the manifest. Checks the local manifest cache first, then Khepri for the root UID, then downloads the root from S3.
2. Compares the manifest's `next_offset` against the local log.
3. Opens a data reader at `next_offset` and begins draining.

If S3 is unreachable, resolution retries with backoff. The writer is unaffected.

### S3-ahead detection

If the manifest's `next_offset` is ahead of the local log's first offset in the current epoch, the remote tier contains data from a previous timeline. The replica reader discards the remote manifest, deletes the stale fragment objects in the background, and restarts from the local log's first offset.

### Local-ahead detection

If the local log's first offset is ahead of the manifest's `next_offset` (local retention deleted data the remote tier never received), the replica reader discards the remote manifest and restarts from the local log's first offset. The gap in the remote tier is accepted.

## Retention interaction

### Local retention

After each persist, the `{evaluate_retention, ...}` effect triggers local retention evaluation. The `{'fun', ...}` retention spec checks the manifest cache's `next_offset` and deletes local segments whose data has been fully uploaded.

### Remote retention

After local retention, the shell evaluates the user's retention policy (max-bytes, max-age) against the persisted manifest. If fragments at the head of the manifest exceed the policy:

1. Compute the edit (entries to remove, size delta).
2. Apply the edit to the core's manifest (both in-memory and persisted).
3. Send the fragment keys to the reaper for deletion.
4. Broadcast the edit to replica nodes.

Remote retention keeps at least one entry in the manifest to preserve position metadata.

When the first entry in the root is a group, retention downloads the group object and evaluates the policy against its fragment entries. Fragments are deleted individually. If the entire group is consumed, the group entry is removed from the root and the group object is deleted. If partially consumed, the root's metadata (first_offset, first_timestamp, total_size) is updated to reflect the oldest surviving fragment. See [manifest.md](./manifest.md) "Retention within groups" for details.

## Error handling

### Segment deleted before upload

If local retention deletes a segment between the drain (which read headers) and the upload (which preads bodies), the upload's `pread` fails with `enoent`. The replica reader discards the in-progress fragment and jumps forward to the oldest available segment.

### Transfer failure

Retriable errors (500, 503, timeout): the core returns `{resubmit_transfer, ...}`. The fragment stays in the in-flight queue.

Fatal errors (checksum mismatch, segment gone): the core removes the entry from the queue and accepts the gap.

### Persist conflict

Another writer (from a leadership election) persisted first. The core returns `{reinitialize}`. The shell re-resolves the manifest and restarts. In-flight transfers whose fragments are already in S3 become orphans.

## Test barrier: `await_offset`

`await_offset/2` blocks until the requested offset has been persisted. It gates on the persisted manifest's `next_offset`, not the speculative in-memory manifest. When it returns `ok`:

- The fragment is in S3.
- The manifest referencing it is in S3.
- The metadata store has accepted the manifest.
- The range table reflects the new offset.
- The manifest cache is updated.
- Retention has been evaluated.
