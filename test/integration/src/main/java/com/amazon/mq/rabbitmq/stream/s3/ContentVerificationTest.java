package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "content-verification",
    description =
        "Publish messages with sequential IDs, then replay the whole stream from "
            + "'first' independently from every consumer and verify ordering, no "
            + "duplicates, and no gaps in the confirmed sequences. Catches corruption, "
            + "reordering, and silent message skips.")
public class ContentVerificationTest implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ContentVerificationTest.class);

  @CommandLine.Mixin private ClusterOptions cluster;

  @CommandLine.Option(
      names = "--duration",
      description = "Publishing duration in seconds",
      defaultValue = "300")
  private int durationSeconds;

  @CommandLine.Option(
      names = "--producers",
      description = "Number of producers (use 1 for strict ordering verification)",
      defaultValue = "1")
  private int numProducers;

  @CommandLine.Option(
      names = "--message-size",
      description = "Message body size in bytes (minimum 8 for the sequence header)",
      defaultValue = "256")
  private int messageSize;

  @CommandLine.Option(
      names = "--max-length-bytes",
      description = "Stream max-length-bytes (small to force data into S3 quickly)",
      defaultValue = "500000000")
  private long maxLengthBytes;

  @CommandLine.Option(
      names = "--progress-interval",
      description = "Seconds between progress reports",
      defaultValue = "30")
  private int progressInterval;

  @CommandLine.Option(
      names = "--replay-timeout",
      description = "Maximum seconds for the replay phase",
      defaultValue = "1800")
  private int replayTimeoutSeconds;

  @CommandLine.Option(
      names = "--replay-consumers",
      description = "Number of parallel consumers for the replay phase",
      defaultValue = "4")
  private int replayConsumers;

  @Override
  public void run() {
    LOG.info(
        "Starting content-verification test: stream={} duration={}s producers={}"
            + " msg-size={} max-length-bytes={}",
        cluster.stream,
        durationSeconds,
        numProducers,
        messageSize,
        maxLengthBytes);

    if (messageSize < 8) {
      LOG.error("FAIL: --message-size must be >= 8 (need 8 bytes for sequence number)");
      System.exit(1);
    }

    ManagementApi mgmt = new ManagementApi(cluster.mgmtUri);
    MetricsClient metrics = new MetricsClient(cluster.metricsUris);
    ClusterHealthMonitor health = new ClusterHealthMonitor(cluster.mgmtUri);
    S3Monitor s3Monitor = cluster.buildS3Monitor();

    // Records which sequence numbers the broker confirmed, so replay can tell a
    // real gap from a hole left by an unconfirmed send.
    ConfirmedSequences confirmed = new ConfirmedSequences();

    long[] result;
    try (Environment env = cluster.buildEnvironment()) {
      TestSetup.setupStream(env, mgmt, s3Monitor, cluster.stream, maxLengthBytes);
      result = publishPhase(env, metrics, health, s3Monitor, confirmed);
    } catch (Exception e) {
      LOG.error("FAILED during publish phase", e);
      System.exit(1);
      return;
    }
    long published = result[0];
    long maxSequence = result[1];
    LOG.info("Publish phase complete: confirmed={} maxSequence={}", published, maxSequence);

    if (published == 0) {
      LOG.error("FAIL: zero messages published");
      System.exit(1);
    }

    try (Environment replayEnv = cluster.buildEnvironment()) {
      replayPhase(replayEnv, maxSequence, confirmed, metrics);
      LOG.info("SUCCESS: content-verification test passed");
    } catch (Exception e) {
      LOG.error("FAILED during replay phase", e);
      System.exit(1);
    } finally {
      if (s3Monitor != null) {
        s3Monitor.close();
      }
    }
  }

  private long[] publishPhase(
      Environment env,
      MetricsClient metrics,
      ClusterHealthMonitor health,
      S3Monitor s3Monitor,
      ConfirmedSequences confirmed)
      throws InterruptedException {
    AtomicLong sequence = new AtomicLong(0);
    AtomicLong totalConfirmed = new AtomicLong(0);
    CountDownLatch stop = new CountDownLatch(1);

    java.util.List<Thread> publisherThreads = new java.util.ArrayList<>();
    java.util.List<Producer> producers = new java.util.ArrayList<>();

    for (int i = 0; i < numProducers; i++) {
      Producer producer = env.producerBuilder().stream(cluster.stream).build();
      producers.add(producer);
      int idx = i;
      Thread t =
          new Thread(
              () -> {
                byte[] padding = new byte[messageSize - 8];
                try {
                  while (!stop.await(0, TimeUnit.MILLISECONDS)) {
                    long seq = sequence.getAndIncrement();
                    byte[] body = encodeMessage(seq, padding);
                    producer.send(
                        producer.messageBuilder().addData(body).build(),
                        status -> {
                          if (status.isConfirmed()) {
                            totalConfirmed.incrementAndGet();
                            // Record the confirmed sequence so replay can tell a
                            // real gap from a hole left by an unconfirmed send.
                            confirmed.set(seq);
                          }
                        });
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              },
              "publisher-" + idx);
      t.start();
      publisherThreads.add(t);
    }

    long startTime = System.currentTimeMillis();
    long deadline = startTime + Duration.ofSeconds(durationSeconds).toMillis();
    long nextReport = startTime + Duration.ofSeconds(progressInterval).toMillis();

    LOG.info("Publishing for {}s...", durationSeconds);

    while (System.currentTimeMillis() < deadline) {
      Thread.sleep(1000);
      long now = System.currentTimeMillis();
      if (now >= nextReport) {
        long elapsed = (now - startTime) / 1000;
        long confirmedCount = totalConfirmed.get();
        long sent = sequence.get();
        double rate = confirmedCount * 1000.0 / (now - startTime);
        MetricsClient.Snapshot snap = metrics.snapshot();
        ClusterHealthMonitor.Snapshot healthSnap = health.snapshot();

        String s3Info = "";
        if (s3Monitor != null) {
          S3Monitor.Snapshot s3Snap = s3Monitor.snapshot();
          s3Info = String.format(" s3-objects=%d", s3Snap.objectCount);
        }

        LOG.info(
            "Publish [{}s]: sent={} confirmed={} ({} msg/s) s3-sent={} MiB/s{}",
            elapsed,
            sent,
            confirmedCount,
            String.format("%.0f", rate),
            String.format("%.1f", snap.sentMiBPerS(progressInterval)),
            s3Info);
        LOG.info("  Health: {}", healthSnap.format());

        if (healthSnap.hasAlarm()) {
          LOG.error("ALARM detected - aborting");
          System.exit(1);
        }

        nextReport = now + Duration.ofSeconds(progressInterval).toMillis();
      }
    }

    stop.countDown();
    for (Thread t : publisherThreads) {
      t.join(5000);
    }

    // Wait for in-flight confirms to settle before closing producers. The
    // confirm callback records the sequence in `confirmed`, and closing a
    // producer does not drain outstanding confirms. Replay distinguishes a real
    // gap from an unconfirmed hole using `confirmed`, so every confirm that will
    // arrive must be recorded first. totalConfirmed increments in the same
    // callback as confirmed.set, so a stable totalConfirmed means the bitset is
    // complete.
    long settled = -1;
    for (int i = 0; i < 20; i++) {
      long current = totalConfirmed.get();
      if (current == settled) {
        break;
      }
      settled = current;
      Thread.sleep(500);
    }

    for (Producer p : producers) {
      p.close();
    }

    return new long[] {totalConfirmed.get(), sequence.get()};
  }

  private void replayPhase(
      Environment env, long maxSequence, ConfirmedSequences confirmed, MetricsClient metrics)
      throws InterruptedException {
    // Real-world fan-out: every consumer independently replays the whole stream
    // from 'first' to the tail, the way separate applications each read the same
    // log non-destructively. Each consumer must observe the identical complete,
    // ordered, gap-free sequence. Reading the full S3-resident history from N
    // consumers also multiplies the concurrent remote-read pressure on the same
    // fragments, which is exactly the path we want to stress.
    //
    // The readable message count is whatever the consumers observe, not a number
    // queried up front: the management message count lags retention and is an
    // eventually-consistent approximation. Ground truth is what the consumers
    // actually read, cross-checked for agreement across all of them.

    // The tail at replay start. Publishing has stopped, so this offset is stable
    // and is the last offset every consumer must reach.
    com.rabbitmq.stream.StreamStats stats = env.queryStreamStats(cluster.stream);
    long endOffset = stats.committedOffset();

    LOG.info(
        "Replay: {} consumers each reading first..{}, timeout={}s",
        replayConsumers,
        endOffset,
        replayTimeoutSeconds);

    AtomicLong outOfOrder = new AtomicLong(0);
    AtomicLong duplicates = new AtomicLong(0);
    AtomicLong corruptMessages = new AtomicLong(0);
    AtomicLong gaps = new AtomicLong(0);

    // Per-consumer counters so each independent replay is verified on its own.
    AtomicLong[] counts = new AtomicLong[replayConsumers];
    long[] lastSeq = new long[replayConsumers];
    boolean[] finished = new boolean[replayConsumers];
    for (int i = 0; i < replayConsumers; i++) {
      counts[i] = new AtomicLong(0);
    }
    java.util.Arrays.fill(lastSeq, -1);

    // One count-down per consumer; the replay is done when all consumers have
    // reached the tail.
    CountDownLatch done = new CountDownLatch(replayConsumers);

    java.util.List<Consumer> consumers = new java.util.ArrayList<>();

    for (int i = 0; i < replayConsumers; i++) {
      int consumerIdx = i;

      Consumer c = null;
      for (int attempt = 1; attempt <= 5; attempt++) {
        try {
          var builder =
              env.consumerBuilder().stream(cluster.stream).offset(OffsetSpecification.first());
          builder.flow().initialCredits(10);
          c =
              builder
                  .messageHandler(
                      (context, message) -> {
                        long off = context.offset();
                        // Ignore anything past the tail captured at replay start.
                        if (off > endOffset) {
                          return;
                        }
                        counts[consumerIdx].incrementAndGet();

                        byte[] body = message.getBodyAsBinary();
                        if (body == null || body.length < 8) {
                          corruptMessages.incrementAndGet();
                        } else {
                          long seq = decodeSequence(body);
                          if (seq < 0 || seq >= maxSequence) {
                            corruptMessages.incrementAndGet();
                          } else {
                            verifySequence(
                                consumerIdx, seq, lastSeq, confirmed, duplicates, outOfOrder, gaps);
                          }
                        }

                        // Reaching the tail offset marks this consumer complete.
                        if (off >= endOffset && !finished[consumerIdx]) {
                          finished[consumerIdx] = true;
                          done.countDown();
                        }
                      })
                  .build();
          break;
        } catch (StreamException e) {
          if (attempt == 5) {
            throw e;
          }
          LOG.warn(
              "  Consumer {} subscribe attempt {}/5 failed: {} — retrying in 15s",
              consumerIdx,
              attempt,
              e.getMessage());
          Thread.sleep(15000);
        }
      }
      consumers.add(c);
      LOG.info("  Consumer {}: reading first..{}", consumerIdx, endOffset);
    }

    long startTime = System.currentTimeMillis();
    long nextReport = startTime + Duration.ofSeconds(progressInterval).toMillis();

    while (!done.await(1, TimeUnit.SECONDS)) {
      long elapsed = System.currentTimeMillis() - startTime;
      if (elapsed > Duration.ofSeconds(replayTimeoutSeconds).toMillis()) {
        for (Consumer c : consumers) {
          c.close();
        }
        LOG.error(
            "REPLAY TIMEOUT: slowest consumer at {} after {}s (not all consumers reached the tail)",
            ReplayProgress.min(counts),
            replayTimeoutSeconds);
        LOG.info("  Per-consumer: {}", ReplayProgress.format(counts));
        System.exit(1);
      }
      long now = System.currentTimeMillis();
      if (now >= nextReport) {
        long min = ReplayProgress.min(counts);
        long sum = ReplayProgress.sum(counts);
        double rate = sum * 1000.0 / (now - startTime);
        long elapsedSec = (now - startTime) / 1000;
        MetricsClient.Snapshot snap = metrics.snapshot();
        LOG.info(
            "Replay [{}s]: slowest={} total={} ({} msg/s) s3-recv={} MiB/s"
                + " corrupt={} dup={} gap={} ooo={}",
            elapsedSec,
            min,
            sum,
            String.format("%.0f", rate),
            String.format("%.1f", snap.receivedMiBPerS(progressInterval)),
            corruptMessages.get(),
            duplicates.get(),
            gaps.get(),
            outOfOrder.get());
        nextReport = now + Duration.ofSeconds(progressInterval).toMillis();
      }
    }

    for (Consumer c : consumers) {
      c.close();
    }

    long elapsedSec = (System.currentTimeMillis() - startTime) / 1000;
    long corrupt = corruptMessages.get();
    long dups = duplicates.get();
    long ooo = outOfOrder.get();
    long gap = gaps.get();
    long min = ReplayProgress.min(counts);
    long max = ReplayProgress.max(counts);

    LOG.info("Replay complete: each consumer read {} messages in {}s", min, elapsedSec);
    LOG.info("  Per-consumer: {}", ReplayProgress.format(counts));
    LOG.info(
        "Integrity: corrupt={} duplicates={} gaps={} out-of-order={}", corrupt, dups, gap, ooo);

    // Fan-out consistency: every consumer subscribed at 'first' and read to the
    // same tail, so they must all observe the same number of messages. The
    // consumers subscribe in a tight loop with no reads in between, so retention
    // cannot advance the readable start between them and skew the counts. A
    // divergence therefore means a consumer saw a different view of the log.
    if (min != max) {
      LOG.error(
          "REPLAY FAILED: consumers disagree on message count (min={} max={})", min, max);
      System.exit(1);
    }
    if (min == 0) {
      LOG.error("REPLAY FAILED: consumers read zero messages");
      System.exit(1);
    }
    if (corrupt > 0) {
      LOG.error("CORRUPTION DETECTED: {} messages had invalid sequence numbers", corrupt);
      System.exit(1);
    }
    // gaps counts only confirmed sequences that were skipped, so any gap is a
    // real missing message rather than a hole left by an unconfirmed send.
    if (gap > 0) {
      LOG.error("GAP DETECTED: {} confirmed sequences were never delivered", gap);
      System.exit(1);
    }
    if (dups > 0) {
      LOG.error("DUPLICATES: {} duplicate sequences detected", dups);
      System.exit(1);
    }
    if (ooo > 0) {
      LOG.error("ORDERING VIOLATION: {} messages received out of order", ooo);
      System.exit(1);
    }

    LOG.info("Replay PASSED: all messages verified");
  }

  // Verifies a single consumer's view of the sequence: detects duplicates,
  // gaps, and ordering violations against the previously seen sequence. With a
  // single producer the confirmed sequences are strictly contiguous; with
  // multiple producers small interleaving inversions are tolerated. No confirmed
  // sequence may be missing or repeated within a consumer's full-stream replay.
  private void verifySequence(
      int consumerIdx,
      long seq,
      long[] lastSeq,
      ConfirmedSequences confirmed,
      AtomicLong duplicates,
      AtomicLong outOfOrder,
      AtomicLong gaps) {
    long prev = lastSeq[consumerIdx];
    if (prev >= 0) {
      if (seq == prev) {
        duplicates.incrementAndGet();
      } else if (seq < prev) {
        long backwards = prev - seq;
        if (numProducers == 1 || backwards > 10000) {
          outOfOrder.incrementAndGet();
        }
      } else if (numProducers == 1 && seq > prev + 1) {
        // Single producer: a forward jump means the consumer skipped over the
        // sequences in (prev, seq). The producer assigns a sequence to every
        // send but only confirmed sends reach the stream, so count only the
        // confirmed sequences in the skipped range as real gaps. Holes left by
        // unconfirmed sends are expected and ignored.
        long skipped = confirmed.countInRange(prev + 1, seq - 1);
        if (skipped > 0) {
          gaps.addAndGet(skipped);
        }
      }
    }
    lastSeq[consumerIdx] = seq;
  }

  private static byte[] encodeMessage(long sequence, byte[] padding) {
    byte[] body = new byte[8 + padding.length];
    ByteBuffer.wrap(body).putLong(sequence);
    return body;
  }

  private static long decodeSequence(byte[] body) {
    return ByteBuffer.wrap(body, 0, 8).getLong();
  }
}
