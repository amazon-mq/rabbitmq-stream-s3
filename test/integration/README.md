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
