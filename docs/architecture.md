# Architecture

This document describes how the plugin's processes fit together: what starts them, what they own, and how data flows between them.

## Plugin lifecycle

The plugin is enabled via `rabbitmq-plugins enable rabbitmq_stream_s3`. RabbitMQ starts it after the broker is fully up (after `core_started`). On start, the plugin's supervisor initializes all infrastructure and sets the osiris hooks. Streams that already exist at this point are discovered and attached to (see [Stream discovery](#stream-discovery)). Streams created after plugin start are handled by the hooks.

Disabling the plugin stops the supervision tree, kills all replica readers, and clears the osiris hooks (`log_hooks` and `log_reader` are unset from the osiris application environment). Re-enabling rediscovers existing streams and resumes uploading from where the manifest left off.

The plugin never blocks or interferes with the local write path. All upload work happens in background processes. S3 availability is a soft dependency: outages cause uploads to fall behind but do not affect publishing, replication, or local consumption.

## Supervision tree

```
rabbitmq_stream_s3_sup (one_for_one)
├── rabbitmq_stream_s3_manifest_replica (worker, permanent)
│     Per-node manifest cache. Owns the ETS table of cached manifests.
│     Receives sequenced edits from writer-node replica readers. Detects
│     gaps and requests re-sync. Triggers retention on replica nodes.
├── rabbitmq_stream_s3_reaper (worker, permanent)
│     Batched S3 object deletion. Collects keys from retention and stream
│     deletion, issues DeleteObjects in batches of 1000. Stream deletion
│     spawns a short-lived task that pages through LIST and feeds keys back.
├── rabbitmq_stream_s3_membership_reconciliation (worker, permanent)
│     Continuous Membership Reconciliation for streams. Evaluates whether
│     streams have the correct replica set and triggers corrections.
├── rabbitmq_stream_s3_upload_pool (worker, permanent)
│     HTTP connection pool for S3 uploads. Returns `ignore` when the
│     backend is not AWS.
├── rabbitmq_stream_s3_general_pool (worker, permanent)
│     HTTP connection pool for S3 reads and manifest operations. Returns
│     `ignore` when the backend is not AWS.
├── rabbitmq_stream_s3_governor (worker, permanent)
│     Per-node transfer pacing. Accepts fragment upload submissions from
│     replica readers, paces them via a token bucket, spawns tasks, and
│     reports completions back.
└── rabbitmq_stream_s3_replica_reader_sup (simple_one_for_one)
      Factory. Its dynamic children are per-stream supervisors, started
      `temporary` so the factory never restarts them.
      └── rabbitmq_stream_s3_stream_sup (one per stream)
            Per-stream supervisor. Owns its own restart-intensity budget
            and auto-shuts-down when its reader exits normally.
            └── rabbitmq_stream_s3_replica_reader (per-stream worker)
                  Owns the full upload lifecycle for one stream: drain
                  committed chunks, assemble fragments, submit to governor,
                  apply completions in order, persist manifest, broadcast
                  edits, evaluate retention.
```

The supervisor init also performs one-time setup: creates the seshat counter group, initializes the API backend, sets the osiris hooks (`log_hooks` and `log_reader`), registers the Khepri deletion trigger, and creates the process registry ETS table.

### Replica reader fault isolation

Replica readers are supervised in two layers so that one stream's failure stays confined to that stream.

The factory (`rabbitmq_stream_s3_replica_reader_sup`) is a `simple_one_for_one` whose dynamic children are per-stream supervisors (`rabbitmq_stream_s3_stream_sup`), each owning exactly one replica reader. The reason for the extra layer is the restart-intensity budget. A supervisor's `intensity`/`period` is a single counter shared by all of its children; if it is exceeded the supervisor terminates every child. With replica readers as direct children of one supervisor, a single reader that crash-loops on bad state (or several unrelated readers crashing in the same window) could exhaust that shared budget and take down every reader on the node. Giving each stream its own supervisor gives each stream its own budget, so a poison stream parks alone.

The worker is `transient` and `significant`, and the per-stream supervisor sets `auto_shutdown => any_significant`. The replica reader stops with reason `normal` when its osiris writer goes down (the reader monitors the writer; writer DOWN is the normal end of a stream's life on this node, e.g. leadership transfer or stream deletion). `transient` means that normal stop is not restarted; `auto_shutdown` then terminates the now-childless per-stream supervisor, so a departed stream does not leave an idle supervisor behind. A genuine crash is an abnormal exit, which is restarted within the per-stream budget; only after that budget is exhausted does the per-stream supervisor exit (`shutdown`) and the stream park.

Because the per-stream supervisors are `temporary`, the factory never restarts them and never spends its own budget on their termination, whether they park or auto-shut-down. A parked stream has a live writer but no reader, and the supervisor does not re-attach it. Re-attachment happens only when the writer's `on_init` hook runs again (its `osiris_log` is re-initialized, for example on a leadership transfer back to this node) or when `discover/0` runs at plugin start. There is no periodic reconciler, so a stream whose writer keeps running after its reader parked stays un-tiered until one of those triggers fires.

## Process registry

`rabbitmq_stream_s3_registry` implements the `{via, Module, Name}` callbacks backed by a local ETS table. Names are `{StreamId, Node}` tuples. Replica readers register on init and unregister on terminate.

The registry serves two purposes:
- Hooks and replicas can address the writer node's replica reader for a given stream without knowing its pid.
- Cross-node lookups use `erpc:call/4` to read the remote node's ETS table.

The registry returns `undefined` (not an error) when the ETS table does not exist. This makes it safe to call during plugin disable when the table has been destroyed.

## Osiris hooks

The plugin sets two application environment variables on osiris:

- `{osiris, log_hooks}` points to `rabbitmq_stream_s3_hooks`. This module is called during writer and acceptor init. It spawns the replica reader and configures retention.
- `{osiris, log_reader}` points to `rabbitmq_stream_s3_log_reader`. This module implements the `osiris_log_reader` behaviour for consumer-side reads that span local and remote tiers.

Both are cleared on plugin stop to prevent new writers from calling into dead modules.

### `on_init(writer, Pid, Config)`

Called at the end of `osiris_log:init/1` after the writer's counter and shared atomics are created. The hook:
1. Extracts the user retention spec (filtering out `{'fun', ...}` entries added by the plugin).
2. Spawns a replica reader under `rabbitmq_stream_s3_replica_reader_sup` with the stream ID, writer pid, directory, shared atomics, counter ref, queue resource reference, epoch, and user retention spec.
3. Appends the local retention function to the config's retention spec.
4. Returns the modified config.

### `on_init(acceptor, Pid, Config)`

Called when a replica (acceptor) starts. The hook:
1. Registers the replica's context (directory, shared atomics, counter) with the manifest replica process so that retention evaluation can run on replica nodes.
2. Sends `{register_acceptor, node()}` to the writer's replica reader via the registry. The writer responds with a sync message containing the current manifest and sequence number.
3. Appends the local retention function.

### `on_retention_updated(Retention, Config)`

Called when a user changes retention on a running stream. The hook re-appends the local retention function and forwards the new spec to the replica reader for remote tier retention evaluation.

## Stream discovery

When the plugin starts after streams are already running (normal production startup, or re-enable after disable), it discovers existing writers and replicas and attaches to them.

Discovery iterates `supervisor:which_children(osiris_server_sup)` and for each child:

**Writers** (identified by `modules = [osiris_writer]`):
1. Calls `osiris_util:get_reader_context(Pid)` to obtain `name`, `dir`, `shared`, and `reference`.
2. Fetches the counter ref via `osiris_counters:fetch({osiris_writer, Reference})`.
3. Reads the epoch via `osiris_counters:overview({osiris_writer, Reference})`.
4. Starts a replica reader. Handles `{error, {already_started, _}}` gracefully (the hook may have already spawned one).

**Replicas** (identified by `modules = [osiris_replica]`):
1. Calls `osiris_util:get_reader_context(Pid)` to obtain `name`, `dir`, `shared`, and `reference`.
2. Fetches the counter ref via `osiris_counters:fetch({osiris_replica, Reference})`.
3. Registers the replica context with the manifest replica process.

Each child is wrapped in a try/catch so a process that crashes between listing and querying does not take down discovery.

## Replica reader architecture

The replica reader uses a functional-core / imperative-shell split. The core module (`rabbitmq_stream_s3_replica_reader_core`) contains all state transition logic as pure functions. The shell (`rabbitmq_stream_s3_replica_reader`) is a gen_server that receives OTP messages, calls into the core, and executes the returned effects.

### Core responsibilities

- In-flight transfer queue (ordered by cut sequence, keyed by reference)
- Pending completions (out-of-order arrivals buffered until predecessors complete)
- Durable persist state machine (idle / timer-running / in-flight)
- Waiter management (callers blocked on `await_offset`)
- Remote retention edit application

### Shell responsibilities

- Drain loop (read chunk headers from osiris log, feed to fragment assembly)
- Effect execution (submit to governor, start persist task, update range, broadcast, evaluate retention)
- Writer monitoring (stop on DOWN)
- Offset listener registration
- Commit timer management

## Replica reader lifecycle

Each stream on the writer node has exactly one replica reader. Its lifecycle:

1. **Spawn.** Started by the hook (or discovery) under the replica reader supervisor.
2. **Resolve manifest.** Fetches the current manifest state to determine where to resume uploading. If S3 is unreachable, retries with backoff. The writer is unaffected.
3. **Open data reader.** Calls `osiris_writer:init_data_reader/3` starting at the manifest's `next_offset`.
4. **Drain loop.** On each `{osiris_offset, ...}` notification, reads committed chunk headers via `osiris_log:read_header/1`. Accumulates metadata in the fragment assembly.
5. **Cut.** When the assembly reaches the size target (default 64 MiB), the fragment is sealed. The core assigns a reference and returns a `{submit_transfer, ...}` effect.
6. **Submit.** The shell submits the transfer to the governor. The drain loop continues immediately without waiting for the upload to complete.
7. **Transfer complete.** The governor reports completion. The core applies the completion in cut order (buffering out-of-order arrivals). When a contiguous prefix of completions is ready, they are applied to the in-memory manifest.
8. **Durable persist.** When the persist threshold is reached (N fragments applied, or timer fires), the shell spawns a task that PUTs the manifest root to S3 and writes to Khepri (optimistic lock).
9. **Commit complete.** On success: update the manifest cache, advance the range table, broadcast edits to replicas, evaluate local and remote retention.
10. **Re-register.** Registers an offset listener for the next committed offset and returns to step 4.

The replica reader monitors the writer. On writer DOWN it stops. It does not delete remote data on DOWN because writer DOWN can mean leadership transfer, not stream deletion. Stream deletion is handled by the Khepri trigger.

## Governor

The governor (`rabbitmq_stream_s3_governor`) is a per-node process that paces fragment transfers to S3. It uses a token bucket to enforce a byte-rate ceiling across all streams on the node.

Replica readers submit transfer work (an opaque function + byte size + reply-to reference). The governor spawns tasks subject to the token bucket budget. On completion, it sends `{transfer_result, Ref, Result}` back to the submitting replica reader.

When the rate is configured as `unlimited` (the default), the governor imposes no pacing and dispatches work immediately.

## Deletion path

When a stream queue is deleted:
1. RabbitMQ removes the queue from `rabbit_db_queue` in Khepri.
2. The plugin's keep-while condition on its Khepri entry fires, removing the entry.
3. The stored procedure `handle_queue_deletion` runs, calling `rabbitmq_stream_s3_reaper:delete_stream(StreamId)`.
4. The reaper spawns a short-lived task that pages through `rabbitmq_stream_s3_api:list/2` on the stream's prefix.
5. Each page of keys is sent back to the reaper as a delete cast.
6. The reaper batches keys (up to 1000) and calls `rabbitmq_stream_s3_api:delete/1`.

If the task crashes or S3 is unavailable, the orphan detection mechanism eventually cleans up.

## Read path (consumer side)

When a consumer subscribes at an offset that no longer exists locally:

1. `rabbitmq_stream_s3_log_reader` checks the manifest cache. If the offset is below the local range and within the manifest's range, it resolves the starting fragment.
2. A `rabbitmq_stream_s3_remote_reader` process is spawned for the consumer. It delegates all decisions (buffering, retry, fragment transitions) to `rabbitmq_stream_s3_remote_reader_core` and executes the resulting effects (S3 requests, timers, replies).
3. The remote reader core uses the fragment iterator to navigate forward through the manifest tree without knowing its internal structure (groups, kilo-groups, etc.).
4. When the consumer catches up to the local tier, the log reader transitions to local reads. The remote reader exits.

If the remote reader encounters a 404 (fragment deleted by retention between iterator creation and fetch), it refreshes the iterator from the manifest cache and repositions at the oldest available offset.

## Manifest propagation

Only the writer node uploads fragments and updates the manifest. Replica nodes learn about manifest changes via sequenced edits.

### Sequence numbers

Each edit broadcast carries a monotonic sequence number and the writer's epoch. The manifest replica on each replica node tracks `{last_seq, epoch, writer_node}` per stream. On receiving an edit:

- If `seq == last_seq + 1` and epoch matches: apply the edit.
- Otherwise: gap or epoch mismatch. Request a re-sync from the writer.

On re-sync, the writer sends a sync message containing the full manifest and current `{seq, epoch}`. The replica resets to this state.

### Partition recovery

No heartbeat or reconnection mechanism is needed because:
- Message loss on a live connection is detected when the next edit arrives (gap in sequence). The durable persist interval (default 2s) bounds the window before the next edit.
- Network partitions cause the stream coordinator to restart the acceptor. Acceptor restart fires the `on_init(acceptor, ...)` hook which re-registers with the writer, triggering a fresh sync.
- An inactive stream (no writes) cannot grow disk regardless of whether the replica's manifest is stale.

## Retention

The plugin manages two retention domains.

### Local retention

The `{'fun', ...}` retention spec deletes local segments whose data has been fully uploaded to S3. This is what makes local disk a sliding window over the full stream. It runs on both writer and replica nodes, triggered when the manifest's `next_offset` advances.

The user's configured retention (max-bytes, max-age) runs independently alongside the plugin's spec. If user retention deletes segments the plugin has not uploaded, the remote tier has a gap. The replica reader accepts the gap and jumps forward.

### Remote retention

After each durable commit, the replica reader evaluates the user's retention policy against the committed manifest. If fragments exceed `max-bytes` or `max-age`, they are removed from the manifest and their S3 objects are sent to the reaper for deletion. The edit is broadcast to replicas.

Remote retention only evaluates fragment entries at the head of the root. If the root starts with a group entry, retention downloads the group object and evaluates the policy against its fragment entries. Fragments are deleted individually. When all fragments in a group are expired, the group entry is removed from the root and the group object is deleted. See [manifest.md](./manifest.md) for details.

Retention can delete all entries from the manifest. An empty manifest (`entries = <<>>`) signals "no remote data available" and the remote tier becomes invisible to consumers. The `next_offset` is preserved in the manifest root so the replica reader knows where to resume uploading.

## Offset listener and event formatter

The replica reader registers as an offset listener on the writer to be notified when committed data is available. In a full RabbitMQ context, the writer has a process-scoped event formatter set by `rabbit_stream_queue` that wraps notifications into queue events. The replica reader overrides this by passing an explicit identity formatter (`{rabbitmq_stream_s3_replica_reader, identity_formatter, []}`) so it receives raw `{osiris_offset, Ref, Offset}` messages.
