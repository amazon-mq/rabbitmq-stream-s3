# Jepsen test for RabbitMQ streams with S3 tiered storage

This is a black-box, real-cluster Jepsen test for the `rabbitmq_stream_s3` plugin. It runs a multi-node RabbitMQ cluster against a MinIO S3 store, drives it with the [`jepsen.tests.kafka`][kafka] ordered-log workload over the RabbitMQ Stream protocol, injects faults, and checks the resulting history for lost writes, duplicates, reordering and offset anomalies.

It complements, and does not replace, the per-seam P and TLA+ models and the `prop_SUITE` tests. Those prove seams in isolation; this exercises the whole stack against a real S3 emulator under composed faults.

## Why the Kafka workload

A RabbitMQ stream is an offset-addressed, append-only, single-writer log, structurally a Kafka topic-partition rather than a register. So we reuse Jepsen's Kafka log model (generator and anomaly checkers) rather than the register/set model used by [`ra-kv-store`][rakv]. Each integer key is one stream (`jepsen-<k>`).

A publish confirm returns only a `publishingId`, not the committed offset, and the Stream client exposes no offset on confirm. For a single uninterrupted writer on a stream that starts empty the publishingId equals the offset, so we report it as the `:send` offset (the kafka workload needs it to learn each key's tail for its final reads). That equality breaks under the leader-move nemesis: producer recovery makes the publishingId drift from the true offset, so the kafka offset-consistency analyzers go red on artifacts (a value appears at both its true offset and its drifted publishingId) even though nothing is actually lost. Under leader-move we therefore downgrade the kafka workload checker to advisory and let the durability checker carry the safety verdict: it reads every stream end-to-end after the run, over a fresh client using true consumer offsets only, and fails if any acknowledged write is missing or duplicated. See `jepsen.streams3/src/jepsen/streams3/client.clj` and `checker.clj`.

## Bounded durability: read before tuning retention

The plugin promises durability for the range `[f, n)` only, not for the un-uploaded tail past the user's retention bound (see `../docs/invariants.md`, "Non-guarantee"). A stock "no acknowledged write is lost" checker is therefore only sound when retention is wide enough that nothing legitimately drops, so retention is configured wide for exactly this reason. Aggressive retention faults will need a bounded-durability checker that only flags loss inside `[f, n)`.

## Topology

```
control ──ssh──► n1..n5 (RabbitMQ + rabbitmq_stream_s3, Erlang 27)
                    │
                    └─ https://jepsen.s3.jepsen.local:443
                         └─ toxiproxy (L4 passthrough, fault injection)
                              └─ minio:443  (virtual-host routing, our CA)
```

The AWS backend is hardwired to TLS/443 with `verify_peer` and virtual-hosted addressing (`<bucket>.<endpoint>`). MinIO therefore serves HTTPS with a cert our nodes trust, and the S3 vhost DNS names alias to Toxiproxy so S3 faults are injectable. Static credentials in `rabbitmq.conf` keep the IMDS/container credential paths out of the picture.

## Running

Requires Docker and Compose and a working `make package-generic-unix` at the umbrella root.

```sh
cd docker
./up.sh                       # builds tarball + certs, brings the cluster up
docker compose exec control \
  lein run test --nodes-file /shared/nodes \
    --username root --ssh-private-key /shared/ssh_key \
    --time-limit 300 --rate 200 --concurrency 20
```

## Status

The partition nemesis runs against a 5-node cluster with the kafka anomaly checkers and retention wide. A run under partition faults reports `:valid? true`, with fault-induced failures correctly classified as indeterminate rather than lost or duplicate, and broker logs collected back to the control node.

The storage-tier nemeses make data really tier to S3 and exercise the read-from-S3 path under fault. Small segments and fragments plus padded payloads force segments to roll, upload and trim within a short run; MinIO needs a KMS key for the plugin's SSE uploads (see `docker/docker-compose.yml`). The `--faults` option selects `s3-outage` and `s3-latency` (via Toxiproxy), `trim` (periodic `evaluate_local_retention`, which trims uploaded segments locally so reads of older offsets fall to the S3 tier), and `leader-move` (repeatedly transfer every stream's leader to a replica, relocating each writer and bumping the stream epoch so a deposed writer's in-flight upload must be fenced).

Two extra checkers run alongside the kafka anomaly checkers. The `:tiering` coverage checker scrapes the plugin's S3 counters before teardown and fails the run unless fragments were uploaded, and with `trim` that reads were served from S3, and with `leader-move` that the epoch advanced; this keeps a run from passing green while silently not using the remote tier or not moving leaders. The `:durability` checker reads every stream end-to-end after the run and fails if any acknowledged write is lost or duplicated; it is authoritative under `leader-move`, where the kafka offset analyzers are downgraded to advisory (see "Why the Kafka workload").

A run under `partition,s3-outage,s3-latency,trim` stays `:valid? true` while serving thousands of reads from the remote tier:

```sh
docker compose exec control \
  lein run test --nodes-file /shared/nodes \
    --username root --ssh-private-key /shared/ssh_key \
    --time-limit 150 --rate 50 --concurrency 10 \
    --faults partition,s3-outage,s3-latency,trim
```

A run under `leader-move,trim` stays `:valid? true` with the durability checker confirming zero loss and zero duplication across epochs in the double digits:

```sh
docker compose exec control \
  lein run test --nodes-file /shared/nodes \
    --username root --ssh-private-key /shared/ssh_key \
    --time-limit 150 --rate 50 --concurrency 10 \
    --faults leader-move,trim
```

## Planned

- A bounded-durability checker for aggressive retention (a `force-trim` past the upload seam, with the checker scoped to `[f, n)`)
- Super-streams (multi-partition)
- CI (see `ci/`)

[kafka]: https://jepsen-io.github.io/jepsen/jepsen.tests.kafka.html
[rakv]: https://github.com/rabbitmq/ra-kv-store
