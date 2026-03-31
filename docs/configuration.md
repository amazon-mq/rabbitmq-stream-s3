# Configuration

## Enabling the plugin

Add the following to `rabbitmq.conf` to route stream reads and writes through the plugin:

```ini
streams.log_reader = rabbitmq_stream_s3_log_reader
streams.log_manifest = rabbitmq_stream_s3_log_manifest
```

## S3 settings

### `s3.bucket`

The S3 bucket to use for remote tier storage. Required.

```ini
s3.bucket = my-rabbitmq-streams-bucket
```

### `s3.region`

The AWS region of the S3 bucket. If not set, the plugin attempts to determine the region automatically via the EC2 instance metadata service.

```ini
s3.region = us-east-1
```

### `s3.region_endpoints.$region`

Overrides the endpoint hostname for a given region. Useful for S3-compatible storage or VPC endpoints.

```ini
s3.region_endpoints.us-east-1 = s3.us-east-1.amazonaws.com
```

## Credentials

The plugin resolves AWS credentials in the following order:

1. Static credentials from `rabbitmq.conf` (`s3.access_key_id` and `s3.secret_key`)
2. Container credentials endpoint (if the `AWS_CONTAINER_CREDENTIALS_FULL_URI` environment variable is set)
3. EC2 instance metadata service (IMDSv2)

### `s3.access_key_id` and `s3.secret_key`

Static AWS credentials. Not recommended for production; prefer IAM roles.

```ini
s3.access_key_id = AKIAIOSFODNN7EXAMPLE
s3.secret_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

## Continuous Membership Reconciliation (CMR)

This plugin provides the same functionality as [Quorum Queue CMR](https://www.rabbitmq.com/docs/quorum-queues#member-reconciliation) for streams, if enabled in configuration.

```ini
# Whether membership should be periodically evaluated.
# Type: boolean. Default: false.
stream.continuous_membership_reconciliation.enabled = true
# The desired size to which stream clusters should grow.
# Type: positive integer. Default: none.
stream.continuous_membership_reconciliation.target_group_size = 3
# Whether to remove 'dangling' members which are no longer
# part of the RabbitMQ cluster.
# Type: boolean. Default: false.
stream.continuous_membership_reconciliation.auto_remove = true
# Interval at which membership is evaluated, in milliseconds.
# Type: positive integer. Default: 360000 (60 minutes).
stream.continuous_membership_reconciliation.interval = 360000
# Delay in milliseconds after which membership is evaluated
# following any trigger event, for example when a node joins the
# RabbitMQ cluster.
# Type: positive integer. Default: 10000 (10 seconds).
stream.continuous_membership_reconciliation.trigger_interval = 10000
```
