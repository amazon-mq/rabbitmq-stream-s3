## rabbitmq-stream-s3

This is an ongoing project for integrating RabbitMQ streams with S3 storage.

## Project Maturity

rabbitmq-stream-s3 is not stable, with frequent changes in design and functionality.

## Prerequisites

This project currently requires specific development branches of the `rabbitmq-server` and `osiris` repositories:

### rabbitmq-server
Branch: `aws/streams-tiered-storage-dev`

Contains changes needed for S3 integration.

### osiris
Branch: `md/log-reader-and-manifest`

Contains the abstraction layer in Osiris. See [Tiered Storage Support for RabbitMQ Streams](https://github.com/rabbitmq/osiris/issues/184)

## Build

1. **Clone the RabbitMQ server repository**
```
git clone https://github.com/rabbitmq/rabbitmq-server.git
cd rabbitmq-server
```
2. **Switch to the required branch**
```
git checkout aws/streams-tiered-storage-dev
```
3. **Build with the stream-s3 plugin with the correct osiris branch**
```
ADDITIONAL_PLUGINS=rabbitmq_stream_s3 \
dep_rabbitmq_stream_s3="git git@github.com:amazon-mq/rabbitmq-stream-s3.git main" \
dep_osiris="git https://github.com/rabbitmq/osiris md/log-reader-and-manifest" \
make
```

For more information on how to build and develop plugins in RabbitMQ, see [plugin-development](https://www.rabbitmq.com/plugin-development)

## Configure

### Osiris
Osiris needs to be configured to use our s3 log_reader and log_manifest:

```
streams.log_reader = rabbitmq_stream_s3_log_reader
streams.log_manifest = rabbitmq_stream_s3_log_manifest
```

### AWS Credentials

See [rabbitmq_aws](https://github.com/rabbitmq/rabbitmq-server/blob/su_aws/replace_httpc_with_gun_fmt/deps/rabbitmq_aws/README.md#configuration)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
