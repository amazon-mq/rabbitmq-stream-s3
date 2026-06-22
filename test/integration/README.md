# `rabbitmq_stream_s3` integration tests

Java-based integration tests that exercise the S3 tiered-storage plugin against a
running cluster. Each test is a subcommand; see `Main.java` for the list and
`TODO.md` for what each one covers and what remains to be built.

Run them through the `Makefile` targets (each target documents its variables) or
directly via the shaded jar:

```
java -jar target/stream-s3-integration-test-*.jar <subcommand> --uris ... --mgmt-uri ...
```

## Consumption patterns (read before writing a replay test)

A RabbitMQ stream is a **replayable log, not a work queue**. This distinction has
caused real bugs in this suite, so it is worth stating plainly.

In a work queue, each message is delivered to exactly one consumer, so you
parallelize by dividing the work: N workers each take a disjoint subset, and the
union is the whole. **Streams do not work this way.** A stream consumer subscribes
at a start offset and the broker pushes every record from that offset forward to
the tail. There is no "stop at offset X" and no server-side partitioning of a
plain stream across cooperating consumers.

Two consequences follow directly:

1. **You cannot hand disjoint offset-slices of one plain stream to N consumers.**
   Subscribing consumer *i* at `startOffset(i)` does not bound it to its slice — it
   reads to the tail like every other consumer, so the consumers overlap
   completely. Filtering messages client-side (`if (offset >= endOffset) return;`)
   discards records after the fact but does not stop delivery, and it does not make
   the slices real. Partitioning one logical stream across consumers is exactly
   what [super streams](https://www.rabbitmq.com/docs/streams#super-streams)
   provide; each partition is a *separate* stream, not an offset range of one.

2. **Offsets are not message counts.** Deriving a per-consumer message target from
   offset arithmetic (`sliceSize = offsetRange / consumers`) couples the test to an
   assumption (1 offset == 1 message) that is incidental, and combining it with
   client-side slice filtering produces counters that either never reach their
   target (the test stalls) or trip early off overlapping reads (the test passes
   for the wrong reason and masks the bug).

### The fan-out pattern these tests model

The real-world pattern a plain stream serves is **independent fan-out**: several
applications each subscribe and replay the whole log non-destructively — analytics,
audit, and multiple services all reading the same stream from their own offset.

So a parallel replay test should have **every consumer read the whole stream from
`first()` to the tail independently**, and verify each consumer's view on its own.
This is also the stronger test for tiered storage: N consumers each re-reading the
full S3-resident history multiply the concurrent remote-read pressure on the same
fragments, and any per-consumer divergence (a gap or duplicate one consumer sees
and another does not) becomes visible.

`ContentVerificationTest` is the reference implementation of this pattern: each
consumer replays `first()`..tail, completion is *all* consumers reaching the tail,
and the pass gate is the *slowest* consumer reaching the threshold so a single
stuck consumer fails the test rather than being masked by faster ones.

## S3-side request logging (for diagnosing read latency)

When investigating slow or stalled remote-tier reads, it helps to see S3's own
view of each request alongside the broker logs. Two complementary mechanisms can
be enabled on the bucket: S3 Server Access Logging (includes S3-side
`TotalTime` and `TurnAroundTime` latency) and CloudTrail S3 data events
(near-real-time, structured per-object events).

These are diagnostic aids, not part of a normal test run. Enable them only while
investigating, and disable them afterward. The commands below assume the bucket
`$BUCKET` in region `$REGION`; substitute the values for the environment under
test.

```bash
BUCKET=<data-bucket>
REGION=us-west-2
ACCT=<account-id>
```

### S3 Server Access Logging

Logs are delivered as objects under `s3-access-logs/` in the same bucket. The
log prefix is outside the `rabbitmq/stream/` prefix the tests write to and clean
up, so test runs do not disturb the logs. Delivery is best-effort and lags from
minutes up to a few hours.

Enable (requires a bucket policy granting the S3 logging service principal write
access to the log prefix):

```bash
cat > /tmp/s3-log-bucket-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ServerAccessLogsPolicy",
      "Effect": "Allow",
      "Principal": { "Service": "logging.s3.amazonaws.com" },
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::${BUCKET}/s3-access-logs/*",
      "Condition": {
        "ArnLike": { "aws:SourceArn": "arn:aws:s3:::${BUCKET}" },
        "StringEquals": { "aws:SourceAccount": "${ACCT}" }
      }
    }
  ]
}
EOF
aws s3api put-bucket-policy --bucket "$BUCKET" --policy file:///tmp/s3-log-bucket-policy.json

aws s3api put-bucket-logging --bucket "$BUCKET" --bucket-logging-status '{
  "LoggingEnabled": { "TargetBucket": "'"$BUCKET"'", "TargetPrefix": "s3-access-logs/" }
}'
```

Verify, then fetch logs once a run has completed:

```bash
aws s3api get-bucket-logging --bucket "$BUCKET"
aws s3 ls "s3://${BUCKET}/s3-access-logs/"
```

Disable (turn off logging; remove the delivery policy if nothing else relies on
it):

```bash
aws s3api put-bucket-logging --bucket "$BUCKET" --bucket-logging-status '{}'
aws s3api delete-bucket-policy --bucket "$BUCKET"
```

### CloudTrail S3 data events

A dedicated trail keeps the data-event selector isolated from any
account-managed trail. It needs its own log bucket with a CloudTrail service
policy.

Enable:

```bash
TRAIL=<data-bucket>-s3-data-events
LOGBUCKET=<data-bucket>-cloudtrail-${ACCT}

aws s3api create-bucket --bucket "$LOGBUCKET" --region "$REGION" \
  --create-bucket-configuration LocationConstraint="$REGION"

cat > /tmp/cloudtrail-bucket-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AWSCloudTrailAclCheck",
      "Effect": "Allow",
      "Principal": { "Service": "cloudtrail.amazonaws.com" },
      "Action": "s3:GetBucketAcl",
      "Resource": "arn:aws:s3:::${LOGBUCKET}",
      "Condition": { "StringEquals": {
        "aws:SourceArn": "arn:aws:cloudtrail:${REGION}:${ACCT}:trail/${TRAIL}"
      }}
    },
    {
      "Sid": "AWSCloudTrailWrite",
      "Effect": "Allow",
      "Principal": { "Service": "cloudtrail.amazonaws.com" },
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::${LOGBUCKET}/AWSLogs/${ACCT}/*",
      "Condition": { "StringEquals": {
        "s3:x-amz-acl": "bucket-owner-full-control",
        "aws:SourceArn": "arn:aws:cloudtrail:${REGION}:${ACCT}:trail/${TRAIL}"
      }}
    }
  ]
}
EOF
aws s3api put-bucket-policy --bucket "$LOGBUCKET" --policy file:///tmp/cloudtrail-bucket-policy.json

aws cloudtrail create-trail --name "$TRAIL" --s3-bucket-name "$LOGBUCKET" --region "$REGION"

# Scope data events to objects in the data bucket only, to bound volume and cost.
aws cloudtrail put-event-selectors --region "$REGION" --trail-name "$TRAIL" \
  --event-selectors '[{"ReadWriteType":"All","IncludeManagementEvents":false,"DataResources":[{"Type":"AWS::S3::Object","Values":["arn:aws:s3:::'"$BUCKET"'/"]}]}]'

aws cloudtrail start-logging --region "$REGION" --name "$TRAIL"
```

Verify and query:

```bash
aws cloudtrail get-trail-status --region "$REGION" --name "$TRAIL"
aws s3 ls "s3://${LOGBUCKET}/AWSLogs/${ACCT}/CloudTrail/${REGION}/" --recursive
```

Disable (stop and delete the trail; delete the log bucket once its contents are
no longer needed):

```bash
aws cloudtrail stop-logging --region "$REGION" --name "$TRAIL"
aws cloudtrail delete-trail --region "$REGION" --name "$TRAIL"
aws s3 rb "s3://${LOGBUCKET}" --force
```
