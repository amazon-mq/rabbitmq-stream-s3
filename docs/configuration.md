# Configuration

## Enabling the plugin

Add the following to `rabbitmq.conf` to route stream reads and writes through the plugin:

```
streams.log_reader = rabbitmq_stream_s3_log_reader
streams.log_manifest = rabbitmq_stream_s3_log_manifest
```

## S3 settings

### `s3.bucket`

The S3 bucket to use for remote tier storage. Required.

```
s3.bucket = my-rabbitmq-streams-bucket
```

### `s3.region`

The AWS region of the S3 bucket. If not set, the plugin attempts to determine the region automatically via the EC2 instance metadata service.

```
s3.region = us-east-1
```

### `s3.region_endpoints.$region`

Overrides the endpoint hostname for a given region. Useful for S3-compatible storage or VPC endpoints.

```
s3.region_endpoints.us-east-1 = s3.us-east-1.amazonaws.com
```

## Credentials

The plugin resolves AWS credentials in the following order:

1. Static credentials from `rabbitmq.conf` (`s3.access_key_id` and `s3.secret_key`)
2. Container credentials endpoint (if the `AWS_CONTAINER_CREDENTIALS_FULL_URI` environment variable is set)
3. EC2 instance metadata service (IMDSv2)

### `s3.access_key_id` and `s3.secret_key`

Static AWS credentials. Not recommended for production; prefer IAM roles.

```
s3.access_key_id = AKIAIOSFODNN7EXAMPLE
s3.secret_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```
