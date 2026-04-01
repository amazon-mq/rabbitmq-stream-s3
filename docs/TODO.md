# TODO

## `stream_test_utils` bug: `receive_stream_commands/3` discards accumulated data on retry

`stream_test_utils:receive_stream_commands/3` loops up to 10 times calling
`gen_tcp:recv/3`, accumulating incoming data into the `rabbit_stream_core`
state. When the loop is exhausted without a complete frame, it returns the
atom `empty` — but discards the updated core state containing the partially
accumulated frame data.

Any caller that retries by calling `receive_stream_commands` again with the
original core state will re-read from an empty buffer, losing the bytes
already received. For large chunks (e.g. a 2 MB message), this means the
frame can never be assembled.

The fix is to either return the updated core state alongside `empty`, or for
callers to implement their own recv loop that preserves the accumulated state
across iterations. `s3_streams_SUITE` works around this with a local
`receive_deliver/3` helper that calls `rabbit_stream_core:next_command/1` and
`rabbit_stream_core:incoming_data/2` directly.

## Orphaned S3 objects after data directory reset

### Problem

When a node's data directory is wiped (e.g. during node replacement, disaster
recovery, or development) and RabbitMQ is restarted, the Khepri database is
recreated from scratch. Any streams that existed before the wipe are gone from
Khepri — but not via a delete operation. The keep-while condition that triggers
S3 cleanup never evaluates, so the S3 objects for those streams are never
deleted. They remain in the bucket indefinitely with no corresponding stream.

### How to reproduce

1. Create a stream and publish enough data for fragments to be uploaded to S3
2. Stop RabbitMQ
3. Wipe the data directory (`rm -rf /var/lib/rabbitmq/*`)
4. Restart RabbitMQ
5. The stream no longer exists in Khepri, but its S3 objects remain

### Ideas for addressing

- On startup (or periodically), list the stream ID prefixes under the
  configured S3 bucket path (`rabbitmq/stream/`) and compare them against
  streams known to Khepri. Any S3 prefix with no matching Khepri entry is
  a candidate for deletion
- The check should only run on the Khepri leader to avoid races with
  replication lag during cluster startup
- Consider a grace period before deleting orphaned objects — a stream might
  be in the process of being created or replicated and not yet visible in
  Khepri
- Alternatively, expose a CLI command or management API endpoint for
  operators to list and manually delete orphaned S3 data
