# TODO

## S3 bucket cleanup

The `cleanup` command deletes all streams and closes connections, but it does NOT clean the S3 bucket. After streams are deleted, orphaned objects remain in S3 until the garbage collector runs (or indefinitely if the stream IDs no longer exist in the broker's metadata).

For a truly clean environment between test runs, the S3 bucket contents under the `rabbitmq/stream/` prefix must be deleted. This requires:

- The bucket name (available in `~/env.mk` or from the deployment state)
- AWS credentials (available on EC2 via instance profile)
- The AWS SDK or shelling out to `aws s3 rm --recursive`

Options:
1. Add `--s3-bucket` to the cleanup command and use the AWS SDK (adds a Maven dependency)
2. Add an `s3-cleanup` Makefile target that calls `aws s3 rm` directly
3. Keep it manual: `aws s3 rm s3://BUCKET/rabbitmq/stream/ --recursive --region us-west-2`

Option 2 is simplest and avoids pulling in the AWS SDK for a single operation.
