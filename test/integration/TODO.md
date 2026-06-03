# TODO

## Full environment reset

A complete reset requires both `make cleanup` (streams + connections) and `make s3-cleanup` (orphaned S3 objects). These are separate because S3 cleanup is slow and sometimes not needed between short test iterations.

For a full reset: `make cleanup s3-cleanup`

## Consolidate AWS CLI into Java

The `s3-cleanup` target shells out to `aws s3 rm`. Eventually all AWS CLI usage should move into the Java harness (via the AWS SDK) so the test suite is fully self-contained with no external tool dependencies beyond `java` and `make`.

## Remaining test commands

- Pool exhaustion test (FUTURE.md #2): reduce pool size, run consumers, verify progress under pool pressure
- Retention during active reads (FUTURE.md #5): consumer mid-fragment when retention fires
- S3 bandwidth / Prometheus checks during high-throughput (deferred from initial implementation)
