# TODO

## Implemented

- `high-throughput` — publish at high rate with head-tracking consumers, parallel replay from first available offset, verified with 1200s (20-minute) runs
  - S3 object count monitoring (per-stream prefix, server-side filtered)
  - S3 bytes sent/received via Prometheus scraping (handles labeled metrics)
  - Per-node cluster health: memory, disk free, file descriptors, alarm detection
  - Retention verification: fails if S3 object count grows monotonically without decrease
  - S3 fallback detection: fails if bytes_received drops to zero during replay (when S3 is populated)
  - Parallel replay with offset slicing across `[committedOffset - messageCount, committedOffset]`
  - Subscribe retry to work around gen_statem blocking (issue #191)
  - Separate Environment for publish and replay phases (avoids stale subscription state)
  - S3 cleanup before stream deletion (objects deleted before Khepri trigger races)
- `cleanup` — delete all streams, close connections, optionally clean S3 bucket

## Remaining test commands

### From manual test scripts

- **Retention test:** Publish until remote tier has data, verify S3 objects are deleted when max-length-bytes is exceeded, verify manifest first_offset advances correctly
- **Retention stress test:** Consumer reading deep in the remote tier while retention deletes fragments underneath it. Tests deferred deletion and 404 → refresh-iterator → advance path. Consumer should not crash and should make forward progress
- **Node restart test:** Publishing and consuming continue correctly across replica and leader node restarts. Consumer at 'first' can read all confirmed messages including those from the remote tier after restart
- **Full cluster restart test:** Publish until fragments are in S3, stop all nodes, restart all nodes, verify stream is available and consumers can read from both local and remote tiers
- **S3 unavailability test:** Publish until fragments are in S3, revoke S3 access for 60 seconds, restore access. Verify: no crashes during outage, upload tasks retry, S3 object count increases after recovery
- **Stream deletion test:** Publish until fragments are in S3, delete stream, verify all S3 objects under the stream's prefix are removed by the Khepri trigger

### From FUTURE.md test gaps

- **Pool exhaustion under controlled pressure:** Reduce `general_pool_max_size` to a small value (e.g. 2), run multiple consumers, verify `pool_busy` retries succeed and consumers make forward progress
- **Multiple consumers at different offsets:** Head consumer + lagging consumer on the same stream simultaneously. Stresses the connection pool and exercises concurrent local + remote tier reads. (Partially covered by `MultiOffsetConsumerTest` stub)
- **Manifest replication lag:** Read from a replica node while retention runs on the writer. Verify 404 → refresh-iterator recovery works across nodes with stale manifest caches

### Lower priority / long-running

- **Long-running stability (8h+):** Not a new command — just `--duration 28800` on high-throughput. Surfaces memory leaks, connection pool drift, binary reference accumulation, GC pressure
- **Group object retention:** Publish long enough for rebalancing to produce group objects, then tighten retention. Verify the `maybe_spawn_group_retention` path works end-to-end

## Removed

- **RemoteOnlyReadTest:** Removed because it duplicates what `high-throughput` already verifies. The replay phase of `high-throughput` consumes from offset 0 through data that only exists in the remote tier and asserts S3 bytes_received > 0. A standalone remote-read-only test would exercise the same code path without adding coverage

## Notes

- Node restart, full cluster restart, and S3 unavailability tests require the ability to SSH to cluster nodes and run `rabbitmqctl stop_app` / `rabbitmqctl start_app` or manipulate IAM policies. The Java harness may need a mechanism for this (SSH exec, or a separate Makefile target that coordinates)
- S3 bucket cleanup is implemented in `CleanupCommand.java` using AWS SDK v2 batch delete under the `rabbitmq/stream/` prefix
- S3 key structure: `rabbitmq/stream/__<streamName>_<timestamp>/...` — the stream ID is `osiris_util:to_base64uri(Vhost ++ "_" ++ Name ++ "_" ++ Timestamp)`. For the default `/` vhost, `/` becomes `_`, giving prefix `rabbitmq/stream/__<name>_`
- `streamStats.firstOffset()` returns the local-tier first offset only (not S3-aware). Use `committedOffset - messagesReady` for the true first readable offset
- Prometheus metrics include a `{module="..."}` label — regex patterns must account for optional labels between metric name and value
- MetricsClient delta computation skips intervals where the number of successfully-scraped endpoints changes (avoids false spikes from partial scrapes)
