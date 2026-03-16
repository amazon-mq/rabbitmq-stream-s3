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
