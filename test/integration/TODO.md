# TODO

## Implemented

- `high-throughput` - publish at high rate with head-tracking consumers, parallel replay from first available offset, verified with 2700s (45-minute) runs
  - S3 object count monitoring (per-stream prefix, server-side filtered)
  - S3 bytes sent/received via Prometheus scraping (handles labeled metrics)
  - Per-node cluster health: memory, disk free, file descriptors, alarm detection
  - Retention verification: fails if S3 object count grows monotonically without decrease (gated on cumulative bytes sent exceeding max-length-bytes)
  - Retention first_offset assertion: fails if message count equals total published (retention never advanced first_offset)
  - S3 fallback detection: fails if bytes_received drops to zero during replay (when S3 is populated)
  - Parallel replay fan-out: each `--replay-consumers` consumer independently replays `first()`..tail; the pass gate is the slowest consumer reaching the threshold
  - Subscribe retry to work around gen_statem blocking (issue #191)
  - Separate Environment for publish and replay phases (avoids stale subscription state)
  - S3 cleanup before stream deletion (objects deleted before Khepri trigger races)
- `multi-offset-consumer` - head-tracking consumer + rate-limited lagging consumer (from 'first') on the same stream while publishing
  - Guava RateLimiter for application-level throttling
  - `creditWhenHalfMessagesProcessed` for wire-level throttling (keeps offset genuinely behind head)
  - Warmup phase: publish until S3 is populated before starting lagging consumer
  - Asserts both consumers make progress, lagging stays behind head, S3 recv observed, no alarms
- `content-verification` - publish a bounded amount (below `max-length-bytes` so no retention fires), then have each `--replay-consumers` consumer independently replay `first()`..tail and verify per-consumer content with no gaps or duplicates; the reference implementation of the fan-out pattern (see README)
  - `--max-length-bytes` must exceed the peak un-uploaded backlog (issue #233)
- `stream-deletion` - publish until S3 is populated, delete the stream, verify all S3 objects removed by Khepri trigger
  - Found bug #196: one dangling fragment from in-flight upload consistently remains after deletion
- `cleanup` - delete all streams, close connections, optionally clean S3 bucket

## Remaining test commands

### Require SSH/infra access

These tests cannot be implemented purely in Java - they need the ability to stop/start RabbitMQ nodes or manipulate IAM policies on cluster nodes. Implementation options: Makefile targets that orchestrate SSH commands around the Java test, or a `--ssh-host` option.

- **Node restart test:** Publishing and consuming continue correctly across replica and leader node restarts. Consumer at 'first' can read all confirmed messages including those from the remote tier after restart
- **Full cluster restart test:** Publish until fragments are in S3, stop all nodes, restart all nodes, verify stream is available and consumers can read from both local and remote tiers
- **S3 unavailability test:** Publish until fragments are in S3, revoke S3 access for 60 seconds, restore access. Verify: no crashes during outage, upload tasks retry, S3 object count increases after recovery
- **Pool exhaustion under controlled pressure:** Reduce `general_pool_max_size` to a small value (e.g. 2) via `rabbitmqctl eval`, run multiple consumers, verify `pool_busy` retries succeed and consumers make forward progress

### High effort / specialized

- **Retention stress test:** Consumer reading deep in the remote tier while retention deletes fragments underneath it. Tests deferred deletion and 404 -> refresh-iterator -> advance path. Similar to `multi-offset-consumer` but specifically targeting the 404/refresh path with tighter retention. The `multi-offset-consumer` test partially exercises this.
- **Manifest replication lag:** Read from a replica node while retention runs on the writer. Verify 404 -> refresh-iterator recovery works across nodes with stale manifest caches. Requires pinning a consumer to a specific replica node.

### Lower priority / long-running

- **Long-running stability (8h+):** Not a new command - just `--duration 28800` on high-throughput. Surfaces memory leaks, connection pool drift, binary reference accumulation, GC pressure
- **Group object retention:** Publish long enough for rebalancing to produce group objects, then tighten retention. Verify the `maybe_spawn_group_retention` path works end-to-end

## Covered by existing tests (no standalone test needed)

- **Retention test:** Covered by `high-throughput` - S3 object count deltas show retention firing, first_offset assertion verifies manifest advances
- **Remote-only read:** Covered by `high-throughput` replay phase - consumes from first available offset through S3 data, asserts bytes_received > 0
- **Multiple consumers at different offsets:** Covered by `multi-offset-consumer`

## Notes

- Node restart, full cluster restart, and S3 unavailability tests require the ability to SSH to cluster nodes and run `rabbitmqctl stop_app` / `rabbitmqctl start_app` or manipulate IAM policies
- S3 bucket cleanup is implemented in `CleanupCommand.java` using AWS SDK v2 batch delete under the `rabbitmq/stream/` prefix
- S3 key structure: `rabbitmq/stream/__<streamName>_<timestamp>/...` - the stream ID is `osiris_util:to_base64uri(Vhost ++ "_" ++ Name ++ "_" ++ Timestamp)`. For the default `/` vhost, `/` becomes `_`, giving prefix `rabbitmq/stream/__<name>_`
- `streamStats.firstOffset()` returns the local-tier first offset only (not S3-aware). Use `committedOffset - messagesReady` for the true first readable offset
- Prometheus metrics include a `{module="..."}` label - regex patterns must account for optional labels between metric name and value
- MetricsClient delta computation skips intervals where the number of successfully-scraped endpoints changes (avoids false spikes from partial scrapes)
- Stream deletion test found bug #196: Khepri trigger misses in-flight uploads that complete after the deletion sweep
