# Instructions for AI Agents

This file covers the `rabbitmq_stream_s3` plugin specifically. For general
RabbitMQ conventions (comments, git, building, testing), see the parent
repository's `AGENTS.md` at the root of [`rabbitmq/rabbitmq-server`](https://github.com/rabbitmq/rabbitmq-server).

## Overview

`rabbitmq_stream_s3` provides tiered storage for RabbitMQ streams using Amazon
S3 as the remote tier. It uploads committed stream data as fragment objects and
maintains a manifest per stream. Consumers read from local data when available
and fall back to the remote tier for older data.

The plugin is hosted at [`amazon-mq/rabbitmq-stream-s3`](https://github.com/amazon-mq/rabbitmq-stream-s3).

## Documentation

- `docs/README.md` — design overview, glossary, data representation, operations
- `docs/manifest.md` — manifest structure, concurrency control, operations
- `docs/configuration.md` — all configuration keys with examples
- `docs/TODO.md` — known issues and planned work

Read the relevant doc before modifying any module.

## Repository Structure

### Source (`src/`)

| Module | Role |
|--------|------|
| `rabbitmq_stream_s3.erl` | Utility functions: key construction, index file helpers |
| `rabbitmq_stream_s3_api.erl` | Storage backend behaviour (callbacks for get, put, delete) |
| `rabbitmq_stream_s3_api_aws.erl` | AWS S3 backend implementation (production) |
| `rabbitmq_stream_s3_api_fs.erl` | Filesystem backend implementation (tests) |
| `rabbitmq_stream_s3_app.erl` | OTP application module |
| `rabbitmq_stream_s3_array.erl` | Binary array operations (at, slice, partition_point, etc.) |
| `rabbitmq_stream_s3_db.erl` | Khepri interactions: stream ID storage, triggers, keep-while conditions |
| `rabbitmq_stream_s3_log_manifest.erl` | `osiris_log_manifest` behaviour: local tier interface, fragment recovery on boot |
| `rabbitmq_stream_s3_log_reader.erl` | `osiris_log_reader` behaviour: reads from local or remote tier |
| `rabbitmq_stream_s3_log_reader_sup.erl` | Supervisor for remote reader `gen_server` processes |
| `rabbitmq_stream_s3_machine.erl` | Manifest state machine: fragment tracking, retention, groups |
| `rabbitmq_stream_s3_server.erl` | `gen_server` coordinating uploads, deletions, manifest updates |
| `rabbitmq_stream_s3_sup.erl` | Top-level supervisor |

### Key relationships

- `rabbitmq_stream_s3_log_manifest` implements `osiris_log_manifest` — called by `osiris_writer` during segment rollover and chunk writes
- `rabbitmq_stream_s3_log_reader` implements `osiris_log_reader` — called by `rabbit_stream_reader` when a consumer subscribes
- `rabbitmq_stream_s3_server` is the central coordinator; `_log_manifest` notifies it of available fragments, it kicks off upload tasks
- `rabbitmq_stream_s3_machine` is a pure state machine (no side effects) that `_server` uses to track manifest state
- `rabbitmq_stream_s3_api` is the storage abstraction; `_api_aws` is the real backend, `_api_fs` is used in tests

### Include (`include/`)

`rabbitmq_stream_s3.hrl` — record definitions (`#fragment{}`, `#manifest{}`),
constants (`?INDEX_RECORD_SIZE_B`, `?CHUNK_HEADER_B`, `?MAX_FRAGMENT_SIZE_B`).

### Tests (`test/`)

| Suite | What it tests |
|-------|---------------|
| `unit_SUITE.erl` | Property-based tests for `_array`, `find_fragment`, `find_index_position` |
| `s3_streams_SUITE.erl` | Integration tests: publish, read from remote tier by offset and timestamp |
| `rabbitmq_stream_s3_machine_SUITE.erl` | State machine transitions |
| `rabbitmq_stream_s3_api_aws_SUITE.erl` | AWS API client (requires credentials) |

## Building and Testing

```bash
# Build (including test dependencies)
gmake test-build

# Run all Common Test suites
gmake ct

# Run a specific suite
gmake ct-s3_streams

# Run a specific test case
gmake t=TEST_GROUP:TEST_CASE ct-s3_streams
```

Integration tests in `s3_streams_SUITE` use the filesystem backend (`_api_fs`)
and do not require AWS credentials or an S3 bucket.

## Key Concepts

### Fragment recovery on boot

When a writer starts, `rabbitmq_stream_s3_log_manifest:init_manifest/2` scans
the most recent local index file to recover fragment boundaries. The function
`recover_fragments/7` walks the index array and splits it at
`?MAX_FRAGMENT_SIZE_B` boundaries. If the index array is empty (stream created
but no data flushed), the guard clause returns a default empty fragment.

### Stream ID

Streams are identified internally by a binary like
`<<"__stream-name_1234567890">>`. This is the `name` field in the queue's type
state, not the user-visible queue name. Use `amqqueue:get_type_state/1` to
retrieve it.

### Manifest updates

Fragment uploads are tracked by `rabbitmq_stream_s3_server`. When a contiguous
run of fragments has been uploaded, the server applies them to the in-memory
manifest via `rabbitmq_stream_s3_machine` and uploads the new manifest to S3.
Manifest uploads are debounced to avoid high-frequency updates.

### Storage backend abstraction

The `rabbitmq_stream_s3_api` behaviour allows swapping the storage backend.
Production uses `_api_aws` (real S3). Tests use `_api_fs` (local filesystem).
The backend is configured via application environment, not `rabbitmq.conf`.

## Common Pitfalls

- `rabbitmq_stream_s3_array:at/3` crashes with `badarg` on an empty binary.
  Always guard against empty arrays or use `try_at/3` which returns `undefined`
- `recover_fragments/7` is called recursively with a shrinking `IdxArray` via
  `slice/3`. The empty-array guard clause must be the first function head
- `resolve_remote_location/2` in `_log_reader` must return `{local, Spec}` for
  tail offset specs (`next`, `last`), not delegate directly to
  `init_local_reader` which returns `{ok, Reader}`
- The `rabbitmq_stream_s3_server` public API (`get_manifest/1`, `get_range/1`)
  uses the stream ID, not the user-visible queue name
- Index records are 29 bytes (`?INDEX_RECORD_SIZE_B`):
  `<<ChunkId:64, Timestamp:64, Epoch:64, FilePos:32, ChunkType:8>>`
