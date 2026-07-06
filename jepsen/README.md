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

Requires Docker and Compose. Building the broker tarball needs a working `make package-generic-unix` at the umbrella root; if that is inconvenient (for example the umbrella is pre-compiled by another checkout, which trips an erlang.mk escript-zip duplicate), stage a prebuilt tarball with `TARBALL=` instead.

```sh
cd docker
./up.sh                       # build the tarball + certs, bring the cluster up
./run.sh                      # run one test (default faults), leaving it up
./down.sh                     # tear the cluster down
```

`up.sh` stages the broker tarball at `shared/rabbitmq.tar.xz`, picking, in order:

```sh
TARBALL=/path/to/rabbitmq-server-generic-unix-*.tar.xz ./up.sh   # use a prebuilt one, skip the build
SKIP_BUILD=1 ./up.sh                                             # reuse an already-staged tarball
./up.sh                                                          # build from the umbrella
```

`run.sh` runs one test against the already-up cluster and, unlike `ci/run-jepsen.sh`, leaves it running so you can iterate and inspect `store/`. It reads `FAULTS`, `TIME_LIMIT`, `RATE` and `CONCURRENCY` from the environment and passes any extra arguments through to `lein run test`:

```sh
FAULTS=s3-outage,leader-move ./run.sh
FAULTS=leader-move,trim TIME_LIMIT=300 ./run.sh
```

`down.sh` kills any in-container test process first (on podman, killing the host-side `docker compose exec` shell does not stop the in-container JVM, so a hung run would otherwise contaminate the next one) and then brings the cluster down; `./down.sh --clean` also removes the generated `shared/`.

## Status

The partition nemesis runs against a 5-node cluster with the kafka anomaly checkers and retention wide. A run under partition faults reports `:valid? true`, with fault-induced failures correctly classified as indeterminate rather than lost or duplicate, and broker logs collected back to the control node.

The storage-tier nemeses make data really tier to S3 and exercise the read-from-S3 path under fault. Small segments and fragments plus padded payloads force segments to roll, upload and trim within a short run; MinIO needs a KMS key for the plugin's SSE uploads (see `docker/docker-compose.yml`). The `--faults` option selects `s3-outage` and `s3-latency` (via Toxiproxy), `trim` (periodic `evaluate_local_retention`, which trims uploaded segments locally so reads of older offsets fall to the S3 tier), `leader-move` (each tick, transfer a random stream's leader to a replica, relocating that writer and bumping the stream epoch so a deposed writer's in-flight upload must be fenced), and `member-churn` (each tick, `delete_replica` a random replica from a random stream so an osiris member departs its node for good mid-sync; bounded to keep every stream at leader + at least one replica).

Three extra checkers run alongside the kafka anomaly checkers. The `:tiering` coverage checker scrapes the plugin's S3 counters before teardown and fails the run unless fragments were uploaded, and with `trim` that reads were served from S3, and with `leader-move` that the epoch advanced; this keeps a run from passing green while silently not using the remote tier or not moving leaders. The `s3-outage` fault is instead proven injected by the Toxiproxy status check in the nemesis (it fails on a non-2xx response), since the plugin tolerates an outage gracefully and need not surface an error counter. The `:durability` checker reads every stream end-to-end after the run and fails if any acknowledged write is lost or duplicated; it is authoritative under `leader-move`, where the kafka offset analyzers are downgraded to advisory (see "Why the Kafka workload").

The `:replica` manifest-replica consistency checker snapshots each node's per-stream manifest cache (the table `rabbitmq_stream_s3_manifest_cache`) after the run quiesces and asserts three things: every stream's cached floor is identical across the nodes that cache it (convergence); the cross-node-agreed floor never sits beyond the stream's committed offset (a stale or corrupt floor would claim data the remote tier does not hold); and no cache row on a non-leader node lacks a registered replica context (a contextless replica row is one a sync re-created after the owning osiris member's `DOWN`, with no monitor to reclaim it). The cached epoch is reported alongside each floor for diagnosis but is deliberately left out of the convergence equality: a deposed leader's cache legitimately keeps the older epoch on an idle stream until the next edit, and the plugin's GC explicitly tolerates a cache that lags the committed epoch (`get_manifest_and_epoch/1`), so an epoch lag is not a convergence violation. The leader's own row is written by the writer path with no replica context, so the leader node is excluded from the last test to avoid a false positive. The convergence and stale-floor assertions are genuinely exercised by the `s3-outage`, `leader-move` and `partition` faults; the leaked-row assertion only *fires* when a member departs a node permanently mid-sync. `leader-move` does not cause that — it restarts every member in place on the same node set, so each departed member re-registers a context that reclaims any orphan (the DOWN-then-racing-sync window it does exercise is what the `syncs_dropped_no_context` counter records). The `member-churn` fault targets the leaked-row assertion directly: it `delete_replica`s a member so it leaves its node for good, leaving a sync-raced orphan visible at the end-of-run snapshot (see `BACKLOG.md`; note the assertion can only be made to positively fire against a build with the fix removed).

A run under `s3-outage,leader-move` drives manifest churn (uploads stall and resume while leaders relocate and epochs bump) and stays `:valid? true` with the caches converged:

```sh
FAULTS=s3-outage,leader-move TIME_LIMIT=150 RATE=50 CONCURRENCY=10 ./run.sh
```

A run under `partition,s3-outage,s3-latency,trim` stays `:valid? true` while serving thousands of reads from the remote tier:

```sh
FAULTS=partition,s3-outage,s3-latency,trim TIME_LIMIT=150 RATE=50 CONCURRENCY=10 ./run.sh
```

A run under `leader-move,trim` stays `:valid? true` with the durability checker confirming zero loss and zero duplication across epochs in the double digits:

```sh
FAULTS=leader-move,trim TIME_LIMIT=150 RATE=50 CONCURRENCY=10 ./run.sh
```

A run under `member-churn,s3-latency` permanently removes replicas while syncs are delayed, exercising the manifest-replica leaked-row assertion (a member leaves its node for good while a sync races its teardown) and staying `:valid? true` with the caches converged:

```sh
FAULTS=member-churn,s3-latency TIME_LIMIT=150 RATE=50 CONCURRENCY=10 ./run.sh
```

GitHub Actions runs these scenarios with the `Jepsen` workflow (`.github/workflows/jepsen.yaml`), on a schedule and on demand. It clones the server, checks the plugin out into the umbrella, and invokes `ci/run-jepsen.sh`, which composes the same `up.sh`, `run.sh` and `down.sh`, building the tarball from the clean tree and failing the job if the checker reports anomalies.

## Planned

- A bounded-durability checker for aggressive retention (a `force-trim` past the upload seam, with the checker scoped to `[f, n)`)
- Super-streams (multi-partition)

[kafka]: https://jepsen-io.github.io/jepsen/jepsen.tests.kafka.html
[rakv]: https://github.com/rabbitmq/ra-kv-store
