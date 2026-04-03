## rabbitmq-stream-s3

[![CI](https://github.com/amazon-mq/rabbitmq-stream-s3/actions/workflows/build-test.yaml/badge.svg)](https://github.com/amazon-mq/rabbitmq-stream-s3/actions/workflows/build-test.yaml)

RabbitMQ Streams stores data in an append-only log on local disk, managed by the [osiris](https://github.com/rabbitmq/osiris) library. Each stream is a sequence of segments; old segments are deleted by retention policies. Once a segment is deleted locally, any consumer that hasn't read it yet loses access to that data permanently.

`rabbitmq_stream_s3` adds a **remote tier** by hooking into osiris via two plugin behaviours:

- **`osiris_log_manifest`** (write path) — receives events for every chunk written and every segment roll. It tracks fragment metadata in memory and notifies the upload server when a fragment is ready. Uploads happen asynchronously in the background and do not block writes. The server ensures a fragment is fully uploaded before the corresponding local segment is eligible for deletion.
- **`osiris_log_reader`** (read path) — when a consumer requests an offset or timestamp that no longer exists locally, instead of failing, the broker resolves the manifest to find the right S3 object and byte position, then streams the data directly from S3 to the consumer using HTTP range requests.

Streams can retain data indefinitely in S3 at low cost, while local disk only holds recent data. Consumers see a single continuous stream regardless of whether the data is local or remote.

For a detailed description of the design, see [docs/README.md](docs/README.md).

## Important: `rabbitmq_prometheus` dependency

This plugin depends on `rabbitmq_prometheus`. Enabling `rabbitmq_stream_s3`
**implicitly enables** `rabbitmq_prometheus`, which opens the Prometheus
metrics endpoint on port 15692.

## Project Maturity

rabbitmq-stream-s3 is not stable, with frequent changes in design and functionality.

## Prerequisites

This project currently requires specific development branches of the `rabbitmq-server` and `osiris` repositories:

### rabbitmq-server
Branch: [`streams-tiered-storage`](https://github.com/amazon-mq/upstream-to-rabbitmq-server/tree/streams-tiered-storage)

### osiris
Branch: [`tiered-storage-abstractions`](https://github.com/amazon-mq/upstream-to-osiris/tree/tiered-storage-abstractions)

See [Tiered Storage Support for RabbitMQ Streams](https://github.com/rabbitmq/osiris/issues/184).

## Build

1. Clone the RabbitMQ server repository:
```
git clone https://github.com/amazon-mq/upstream-to-rabbitmq-server.git
cd upstream-to-rabbitmq-server
```
2. Switch to the required branch:
```
git checkout streams-tiered-storage
```
3. Build:
```
make
```

For more information on building and developing RabbitMQ plugins, see [plugin-development](https://www.rabbitmq.com/plugin-development).

## Configure

See [docs/configuration.md](docs/configuration.md) for all configuration options.

The minimum required configuration is:

```
streams.log_reader = rabbitmq_stream_s3_log_reader
streams.log_manifest = rabbitmq_stream_s3_log_manifest

s3.bucket = my-rabbitmq-streams-bucket
s3.region = us-east-1
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
