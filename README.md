## rabbitmq-stream-s3

[![CI](https://github.com/amazon-mq/rabbitmq-stream-s3/actions/workflows/build-test.yaml/badge.svg)](https://github.com/amazon-mq/rabbitmq-stream-s3/actions/workflows/build-test.yaml)

Tiered storage plugin for RabbitMQ streams. Uploads committed stream data to Amazon S3 in the background. Consumers read from local disk when data is available and fall back to S3 for older data.

Streams retain data in S3 indefinitely (or according to a remote retention policy) at low cost, while local disk only holds recent data. Consumers see a single continuous stream regardless of where the data lives.

For a detailed description of the design, start with [docs/README.md](docs/README.md).

## Project Maturity

rabbitmq-stream-s3 is not stable, with frequent changes in design and functionality.

## Important: `rabbitmq_prometheus` dependency

This plugin depends on `rabbitmq_prometheus`. Enabling `rabbitmq_stream_s3`
**implicitly enables** `rabbitmq_prometheus`, which opens the Prometheus
metrics endpoint on port 15692.

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

See [docs/operations.md](docs/operations.md) for all configuration options.

The minimum required configuration is:

```
stream_s3.bucket = my-rabbitmq-streams-bucket
stream_s3.region = us-east-1
```

## Documentation

- [docs/README.md](docs/README.md): overview and reading guide
- [docs/user-guide.md](docs/user-guide.md): behavior (no implementation details)
- [docs/concepts.md](docs/concepts.md): streaming primitives and how the plugin extends them
- [docs/manifest.md](docs/manifest.md): manifest tree structure and concurrency control

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
