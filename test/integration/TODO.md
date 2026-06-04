# TODO

## Implemented

- `high-throughput` — publish at high rate, parallel replay from offset 0, Prometheus S3 bandwidth checks, memory alarm detection
- `cleanup` — delete all streams, close connections, optionally clean S3 bucket
- Parallel replay consumers (`--replay-consumers`)
- Prometheus metrics scraping for S3 bytes received/sent during publish and replay phases

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

## Notes

- Node restart, full cluster restart, and S3 unavailability tests require the ability to SSH to cluster nodes and run `rabbitmqctl stop_app` / `rabbitmqctl start_app` or manipulate IAM policies. The Java harness may need a mechanism for this (SSH exec, or a separate Makefile target that coordinates)
- S3 bucket cleanup is implemented in `CleanupCommand.java` using AWS SDK v2 batch delete under the `rabbitmq/stream/` prefix
