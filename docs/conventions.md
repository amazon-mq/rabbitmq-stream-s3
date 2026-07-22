# Conventions

This document describes the patterns and conventions used in this codebase. Read it after [architecture.md](./architecture.md) to understand not just what exists, but how to write code that belongs here.

## Downstream from Osiris

This plugin is completely downstream from regular streaming. Osiris owns the write path, replication, commit semantics, segment management, and retention evaluation. The plugin hooks in at well-defined extension points and never modifies Osiris's core behavior.

Follow Osiris conventions wherever they do not conflict with the plugin's goals. When in doubt, read how Osiris does it and do the same.

### Config map overrides app env

Osiris uses a pattern where values passed in the config map take priority over application environment. The config map is the highest priority source. Application environment serves as the default when the config map does not contain a key.

```erlang
%% Osiris pattern: config map first, app env as fallback.
FilterSize = maps:get(filter_size, Config, ?DEFAULT_FILTER_SIZE),
MaxSegSize = maps:get(max_segment_size_bytes, Config, ?DEFAULT_MAX_SEGMENT_SIZE_B),
```

The `log_hooks` key follows this pattern. Passing `log_hooks => undefined` in the config map suppresses hooks regardless of what the application environment says. This is how test helpers like `seed_log/2` write to the log without triggering replica reader spawns.

### Chunk header is variable-size

The chunk header is not a fixed 48 bytes. It includes a bloom filter whose size depends on `filter_size` configuration. Do not hardcode chunk header sizes. Use `data_size` and `position`/`next_position` from the header map to reason about byte offsets.

## Derived state has no catch-alls

Any state derived from the manifest (the per-node manifest cache, the osiris first-offset counters, iterator snapshots) can be missing or not yet established, and "missing" is never the same value as "empty". When branching on such state, write one clause per state and no catch-all: a `_ ->` arm over cache states is how a cache miss gets silently collapsed into "no remote tier", which is the boot-window read bug (see the Cached state section of [invariants.md](./invariants.md)). If a new state is added, every consumer should fail to compile or crash loudly, not inherit an arbitrary neighbor's behavior.

For the manifest cache this is enforced structurally, not by convention: branching goes through `rabbitmq_stream_s3_manifest_replica:with_manifest/2`, whose handler map has mandatory keys for all three states (`resolved`, `pending`, `absent`). A call site that fails to consider a state does not compile a quiet fallback into place; it fails to match. `get_manifest/1` remains only for non-branching uses (diagnostics, tests). New derived-state APIs should follow the same shape: expose a total fold, not a bare accessor that invites per-caller `case` expressions.

The direction of each explicit miss clause matters: consumers must fail in the direction that cannot lose data. Readers fail closed (error, the consumer retries), retention deletes nothing, GC skips, resolution goes to the authoritative store. A fallback that degrades silently reads as robustness in review and is exactly where correctness leaks out.

## Cold-state test dimension

Every stateful fixture a test warms up implicitly is a state the suite must also construct cold. An end-to-end test that publishes and then reads has, as a side effect, seeded the manifest cache, attached the hooks, and resolved the manifest: the act of arranging the fixture destroys the cold-start state, so the warm path is the only path such suites can ever exercise.

When a subsystem keeps per-node volatile state, its suite needs at least one case that restarts the owning process or node and exercises the consumer before anything re-warms the state (for streams: subscribe before any publish). Note the topology: with replicas present, the writer's sync re-seeds an acceptor's cache within milliseconds, so cold-cache cases need the single-node shape where no peer can heal the state (see `broker_SUITE:restarted_node_serves_first_from_remote_tier/1`).

## Pure functional cores

Complex state machines use a functional-core / imperative-shell split inspired by Ra's `ra_server` / `ra_server_proc` separation.

The core is a pure module. It takes events and state, returns `{NewState, [Effect]}`. No processes, no I/O, no process dictionary. The shell is a gen_server that receives OTP messages, calls into the core, and executes the returned effects.

This makes interleavings testable as sequences of function calls without mocks, timers, or flakiness.

Examples: `rabbitmq_stream_s3_replica_reader_core` (core) and `rabbitmq_stream_s3_replica_reader` (shell).

## Binary arrays

The manifest stores entries as a flat binary (34 bytes per entry) rather than a list of records. `rabbitmq_stream_s3_array` provides operations over these binary arrays: `at/3`, `slice/3`, `partition_point/3`, `last/2`.

Use binary arrays for any fixed-size record collection that needs binary search or sequential access. Do not convert to lists of records for processing.

## Never grow a binary that reads are served from

Do not accumulate a long-lived buffer with `<<Buffer/binary, Data/binary>>` if sub-binaries of it are handed to other processes (replies, messages). Copying a sub-binary into a message seals the writable binary, so the next append copies the whole buffer; the sub-binary also pins the entire buffer in the receiver's heap until it collects garbage.

Keep the delivered binaries whole in a queue instead and assemble reads from the pieces. `rabbitmq_stream_s3_read_buffer` implements this for the remote read path (see its moduledoc and [read-path.md](./read-path.md#buffer)); reuse it rather than growing a flat binary.

## List accumulation

Do not use `List ++ [Element]` to build lists incrementally. Tail-append copies the entire list on every call, making N appends O(N²).

Use the prepend-and-reverse idiom instead: accumulate with `[Element | Acc]` (O(1) per element), then call `lists:reverse/1` once when the final order is needed (O(N) total).

```erlang
%% Bad: O(N²) over the accumulation cycle.
Edits ++ [Edit]

%% Good: O(1) prepend, O(N) reverse at consumption time.
[Edit | Edits]
...
lists:reverse(Edits)
```

More generally, avoid `++/2` in loops or recursive accumulation. It is fine for one-off concatenation of short, bounded lists (e.g. merging effect lists from two calls).

## Storage backend abstraction

The `rabbitmq_stream_s3_api` behaviour abstracts the remote tier. Production uses `rabbitmq_stream_s3_api_aws` (real S3). Tests use `rabbitmq_stream_s3_api_fs` (local filesystem).

The backend is configured via application environment (`{rabbitmq_stream_s3, rabbitmq_stream_s3_api, Module}`). The CT hook (`rabbitmq_stream_s3_cth`) sets this to the FS backend automatically.

Integration tests never require AWS credentials or an S3 bucket.

## Test infrastructure

### CT hook

All test suites use `rabbitmq_stream_s3_cth` which sets up the FS backend with a per-suite directory, the osiris data directory, and the plugin supervisor (without pulling in the full rabbit application).

### Barriers

The write flow is asynchronous. Use barriers, not sleeps.

`flush_writer(Writer)` blocks until all prior writes are on disk. `await_offset(Config, Offset)` blocks until the replica reader has durably committed past `Offset`. When `await_offset` returns, fragments are in the remote tier, the manifest is committed, and retention has been evaluated.

#### Writer readiness

The `on_init` hook fires inside `osiris_log:init/2` during the writer's `handle_continue`. The hook spawns the replica reader, which registers in the registry immediately. Tests that need the writer to be fully initialized (not just spawned) must call `osiris_writer:query_replication_state(Writer)`, a `gen_batch_server:call` that queues behind `handle_continue` and returns once it has run.

`sys:get_state` is not a reliable barrier for `gen_batch_server`: the call is processed by a different code path that can return before `handle_continue` finishes.

The replica reader uses the same trick. `start_reading/1` calls `osiris_writer:query_replication_state(WriterPid)` before `init_data_reader/3` to ensure the writer's log is initialized.

### Test helpers

`rabbitmq_stream_s3_test_helpers` provides:

- `seed_log(Config, SegmentSpecs)` writes a deterministic local log with explicit segment boundaries and chunk sizes. Bypasses the writer process entirely. Returns metadata (next_offset, total_size, per-chunk and per-segment details).
- `build_manifest(TreeSpec)` constructs a manifest and get_group_fun from a declarative tree spec. Used for testing the fragment iterator and read path without uploading real data.
- `start_writer/2,3` and `start_cluster/3,4` start osiris infrastructure with plugin hooks active.
- `write_sequential/3` writes N records with deterministic content compatible with `assert_sequential/2`.

### Deterministic test construction

Osiris batches writes non-deterministically. To control layout precisely, use `seed_log/2` which gives exact control over chunk sizes and segment boundaries. The fragment assembly cuts based on payload bytes (`data_size`), so the `size` field in a chunk spec directly determines fragment geometry.

When using the writer (for integration tests that need the full async path), use `flush_writer/1` between writes to force one-record-per-chunk.

## Naming

The upload process is "remote replica reader" (not "replica reader," which is `osiris_replica_reader`). Use the full term in module names, logs, and metrics.

A fragment is a remote tier object. Not "segment" (that is local). The manifest is the tree structure mapping offsets to fragment keys. Stream ID is the internal identifier (includes timestamp), not the user-visible queue name.
