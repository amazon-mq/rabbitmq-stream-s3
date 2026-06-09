package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamStats;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "high-throughput",
    description =
        "Publish at high rate for a configurable duration, then replay from "
            + "'first' and verify all messages are readable.")
public class HighThroughputTest implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(HighThroughputTest.class);

  @CommandLine.Mixin private ClusterOptions cluster;

  @CommandLine.Option(
      names = "--duration",
      description = "Publishing duration in seconds",
      defaultValue = "2700")
  private int durationSeconds;

  @CommandLine.Option(
      names = "--producers",
      description = "Number of producers",
      defaultValue = "4")
  private int numProducers;

  @CommandLine.Option(
      names = "--consumers",
      description = "Number of head-tracking consumers during publish phase",
      defaultValue = "2")
  private int numConsumers;

  @CommandLine.Option(
      names = "--message-size",
      description = "Message body size in bytes",
      defaultValue = "1024")
  private int messageSize;

  @CommandLine.Option(
      names = "--max-length-bytes",
      description = "Stream max-length-bytes (remote tier retention)",
      defaultValue = "10000000000")
  private long maxLengthBytes;

  @CommandLine.Option(
      names = "--progress-interval",
      description = "Seconds between progress reports",
      defaultValue = "30")
  private int progressInterval;

  @CommandLine.Option(
      names = "--replay-timeout",
      description = "Maximum seconds for the replay phase",
      defaultValue = "3600")
  private int replayTimeoutSeconds;

  @CommandLine.Option(
      names = "--replay-consumers",
      description = "Number of parallel consumers for the replay phase",
      defaultValue = "8")
  private int replayConsumers;

  @CommandLine.Option(
      names = "--retention-stall-intervals",
      description =
          "Fail if S3 object count grows monotonically for this many consecutive intervals"
              + " (only when --s3-bucket is provided)",
      defaultValue = "10")
  private int retentionStallThreshold;

  @Override
  public void run() {
    LOG.info(
        "Starting high-throughput test: stream={} duration={}s producers={} consumers={} "
            + "msg-size={} max-length-bytes={}",
        cluster.stream,
        durationSeconds,
        numProducers,
        numConsumers,
        messageSize,
        maxLengthBytes);

    ManagementApi mgmt = new ManagementApi(cluster.mgmtUri);
    MetricsClient metrics = new MetricsClient(cluster.metricsUris);
    ClusterHealthMonitor health = new ClusterHealthMonitor(cluster.mgmtUri);
    S3Monitor s3Monitor = cluster.buildS3Monitor();

    boolean s3Populated = false;
    long published;
    try (Environment env = cluster.buildEnvironment()) {
      setupStream(env, mgmt, s3Monitor);
      published = publishPhase(env, mgmt, metrics, health, s3Monitor);
      LOG.info("Publish phase complete: confirmed={}", published);
    } catch (Exception e) {
      LOG.error("FAILED during publish phase", e);
      System.exit(1);
      return;
    }

    if (s3Monitor != null) {
      S3Monitor.Snapshot finalSnap = s3Monitor.snapshot();
      s3Populated = finalSnap.objectCount > 0;
      LOG.info("S3 objects at end of publish: {}", finalSnap.objectCount);
    }

    LOG.info("Waiting for management stats to stabilize...");
    long expectedMessages = mgmt.getStableMessageCount(cluster.stream, 12, 5000);
    if (expectedMessages <= 0) {
      expectedMessages = published;
    }
    LOG.info("Expected messages for replay: {} (published={})", expectedMessages, published);

    if (s3Populated && expectedMessages >= published) {
      LOG.error(
          "RETENTION NOT WORKING: message count ({}) equals published ({}) "
              + "— first_offset did not advance despite S3 being populated",
          expectedMessages,
          published);
      System.exit(1);
    }

    try (Environment replayEnv = cluster.buildEnvironment()) {
      replayPhase(replayEnv, expectedMessages, metrics, s3Populated);
      LOG.info("SUCCESS: high-throughput test passed");
    } catch (Exception e) {
      LOG.error("FAILED during replay phase", e);
      System.exit(1);
    } finally {
      if (s3Monitor != null) {
        s3Monitor.close();
      }
    }
  }

  private void setupStream(Environment env, ManagementApi mgmt, S3Monitor s3Monitor) {
    TestSetup.setupStream(env, mgmt, s3Monitor, cluster.stream, maxLengthBytes);
  }

  private long publishPhase(
      Environment env,
      ManagementApi mgmt,
      MetricsClient metrics,
      ClusterHealthMonitor health,
      S3Monitor s3Monitor)
      throws InterruptedException {
    byte[] body = new byte[messageSize];
    AtomicLong totalPublished = new AtomicLong(0);
    AtomicLong totalConsumed = new AtomicLong(0);
    AtomicLong headOffset = new AtomicLong(-1);
    CountDownLatch stop = new CountDownLatch(1);

    List<Consumer> consumers = new ArrayList<>();
    for (int i = 0; i < numConsumers; i++) {
      var builder = env.consumerBuilder().stream(cluster.stream).offset(OffsetSpecification.next());
      builder.flow().initialCredits(10);
      Consumer c =
          builder
              .messageHandler(
                  (context, message) -> {
                    totalConsumed.incrementAndGet();
                    headOffset.set(context.offset());
                  })
              .build();
      consumers.add(c);
    }

    List<Thread> publisherThreads = new ArrayList<>();
    List<Producer> producers = new ArrayList<>();
    for (int i = 0; i < numProducers; i++) {
      Producer producer = env.producerBuilder().stream(cluster.stream).build();
      producers.add(producer);
      int idx = i;
      Thread t =
          new Thread(
              () -> {
                try {
                  while (!stop.await(0, TimeUnit.MILLISECONDS)) {
                    producer.send(
                        producer.messageBuilder().addData(body).build(),
                        status -> {
                          if (status.isConfirmed()) {
                            totalPublished.incrementAndGet();
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
    int zeroSentIntervals = 0;
    int reportCount = 0;
    boolean retentionSeen = false;
    long cumulativeBytesSent = 0;

    LOG.info(
        "Publishing for {}s with {} head-tracking consumer(s)...", durationSeconds, numConsumers);

    while (System.currentTimeMillis() < deadline) {
      Thread.sleep(1000);
      long now = System.currentTimeMillis();
      if (now >= nextReport) {
        reportCount++;
        long elapsed = (now - startTime) / 1000;
        long pub = totalPublished.get();
        long cons = totalConsumed.get() / Math.max(1, numConsumers);
        double pubRate = pub * 1000.0 / (now - startTime);
        MetricsClient.Snapshot snap = metrics.snapshot();

        String s3ObjectInfo = "";
        long s3ObjectCount = 0;
        cumulativeBytesSent += snap.deltaBytesSent;
        if (s3Monitor != null) {
          S3Monitor.Snapshot s3Snap = s3Monitor.snapshot();
          s3ObjectCount = s3Snap.objectCount;
          s3ObjectInfo =
              String.format(" s3-objects=%d (delta=%+d)", s3Snap.objectCount, s3Snap.delta);
          if (s3Snap.retentionActive()) {
            retentionSeen = true;
          }
          // Only check for retention stalls after enough data has been sent to
          // S3 to exceed max-length-bytes — retention cannot fire until then.
          if (cumulativeBytesSent > maxLengthBytes
              && !retentionSeen
              && s3Snap.monotonicGrowthIntervals >= retentionStallThreshold) {
            LOG.error(
                "RETENTION STALLED: S3 object count has grown for {} consecutive"
                    + " intervals without any decrease (sent {} bytes > max-length-bytes {})"
                    + " — retention may not be working",
                s3Snap.monotonicGrowthIntervals,
                cumulativeBytesSent,
                maxLengthBytes);
            System.exit(1);
          }
        }

        ClusterHealthMonitor.Snapshot healthSnap = health.snapshot();
        LOG.info(
            "Publish [{}s]: published={} ({} msg/s) consumed={} head-offset={}"
                + " s3-recv={} MiB/s s3-sent={} MiB/s{}",
            elapsed,
            pub,
            String.format("%.0f", pubRate),
            cons,
            headOffset.get(),
            String.format("%.1f", snap.receivedMiBPerS(progressInterval)),
            String.format("%.1f", snap.sentMiBPerS(progressInterval)),
            s3ObjectInfo);
        LOG.info("  Health: {}", healthSnap.format());

        if (reportCount > 3 && s3ObjectCount > 0) {
          if (snap.deltaBytesSent > 0) {
            zeroSentIntervals = 0;
          } else {
            zeroSentIntervals++;
            if (zeroSentIntervals >= 3) {
              LOG.warn(
                  "S3 uploads stalled: zero bytes sent for {} consecutive intervals",
                  zeroSentIntervals);
            }
          }
        }

        if (healthSnap.hasAlarm()) {
          for (ClusterHealthMonitor.NodeSnapshot n : healthSnap.nodes) {
            if (n.memoryAlarm) {
              LOG.error("MEMORY ALARM on {} - aborting", n.shortName());
            }
            if (n.diskAlarm) {
              LOG.error("DISK ALARM on {} - aborting", n.shortName());
            }
          }
          System.exit(1);
        }

        nextReport = now + Duration.ofSeconds(progressInterval).toMillis();
      }
    }

    stop.countDown();
    for (Thread t : publisherThreads) {
      t.join(5000);
    }
    for (Producer p : producers) {
      p.close();
    }
    for (Consumer c : consumers) {
      c.close();
    }

    if (s3Monitor != null && !retentionSeen) {
      LOG.warn(
          "Retention was never observed during the publish phase (no S3 object count decrease)");
    }

    return totalPublished.get();
  }

  private void replayPhase(
      Environment env, long expectedMessages, MetricsClient metrics, boolean expectS3Reads)
      throws InterruptedException {
    long threshold = expectedMessages * 95 / 100;

    StreamStats stats = env.queryStreamStats(cluster.stream);
    long committedOffset = stats.committedOffset();
    // stats.firstOffset() returns the local-tier first offset, which does not
    // account for messages in the remote tier. Derive the actual first readable
    // offset from committedOffset and the management API message count.
    long firstOffset = committedOffset - expectedMessages;
    if (firstOffset < 0) {
      firstOffset = 0;
    }
    long offsetRange = committedOffset - firstOffset;
    long sliceSize = offsetRange / replayConsumers;

    LOG.info(
        "Replay: {} consumers, expecting >= {} messages (95% of {}), timeout={}s, expectS3={},"
            + " firstOffset={}, committedOffset={}",
        replayConsumers,
        threshold,
        expectedMessages,
        replayTimeoutSeconds,
        expectS3Reads,
        firstOffset,
        committedOffset);

    AtomicLong totalConsumed = new AtomicLong(0);
    CountDownLatch done = new CountDownLatch(1);

    List<Consumer> consumers = new ArrayList<>();
    for (int i = 0; i < replayConsumers; i++) {
      long startOffset = firstOffset + i * sliceSize;
      long endOffset =
          (i == replayConsumers - 1) ? Long.MAX_VALUE : firstOffset + (i + 1) * sliceSize;
      int consumerIdx = i;

      // Retry subscribe to work around issue #191: the rabbit_stream_reader
      // gen_statem can be blocked in send_chunks (synchronous S3 reads),
      // causing subscribe frames to time out. The server eventually processes
      // them, so a retry after a delay succeeds.
      Consumer c = null;
      for (int attempt = 1; attempt <= 5; attempt++) {
        try {
          var builder =
              env.consumerBuilder().stream(cluster.stream)
                  .offset(OffsetSpecification.offset(startOffset));
          builder.flow().initialCredits(10);
          c =
              builder
                  .messageHandler(
                      (context, message) -> {
                        if (context.offset() >= endOffset) {
                          return;
                        }
                        long count = totalConsumed.incrementAndGet();
                        if (count >= threshold) {
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
      LOG.info(
          "  Consumer {}: offset {} to {}",
          consumerIdx,
          startOffset,
          endOffset == Long.MAX_VALUE ? "end" : endOffset);
    }

    long startTime = System.currentTimeMillis();
    long nextReport = startTime + Duration.ofSeconds(progressInterval).toMillis();
    int zeroRecvIntervals = 0;
    int reportCount = 0;

    while (!done.await(1, TimeUnit.SECONDS)) {
      long elapsed = System.currentTimeMillis() - startTime;
      if (elapsed > Duration.ofSeconds(replayTimeoutSeconds).toMillis()) {
        for (Consumer c : consumers) {
          c.close();
        }
        LOG.error(
            "REPLAY TIMEOUT: consumed {} of {} expected in {}s",
            totalConsumed.get(),
            expectedMessages,
            replayTimeoutSeconds);
        System.exit(1);
      }
      long now = System.currentTimeMillis();
      if (now >= nextReport) {
        reportCount++;
        long current = totalConsumed.get();
        double rate = current * 1000.0 / (now - startTime);
        long elapsedSec = (now - startTime) / 1000;
        MetricsClient.Snapshot snap = metrics.snapshot();
        LOG.info(
            "Replay [{}s]: {}/{} ({} msg/s) s3-recv={} MiB/s",
            elapsedSec,
            current,
            expectedMessages,
            String.format("%.0f", rate),
            String.format("%.1f", snap.receivedMiBPerS(progressInterval)));

        if (expectS3Reads && reportCount > 2) {
          if (snap.deltaBytesReceived > 0) {
            zeroRecvIntervals = 0;
          } else {
            zeroRecvIntervals++;
            if (zeroRecvIntervals >= 3) {
              for (Consumer c : consumers) {
                c.close();
              }
              LOG.error(
                  "REPLAY FAILED: S3 bytes received was zero for {} consecutive intervals "
                      + "- consumers fell back to local tier",
                  zeroRecvIntervals);
              System.exit(1);
            }
          }
        }

        nextReport = now + Duration.ofSeconds(progressInterval).toMillis();
      }
    }

    for (Consumer c : consumers) {
      c.close();
    }
    long total = totalConsumed.get();
    long elapsedSec = (System.currentTimeMillis() - startTime) / 1000;

    LOG.info(
        "Replay complete: {} messages in {}s ({}% of {})",
        total,
        elapsedSec,
        expectedMessages > 0 ? total * 100 / expectedMessages : 0,
        expectedMessages);

    if (total < threshold) {
      LOG.error("REPLAY FAILED: {} < {} (95% of {})", total, threshold, expectedMessages);
      System.exit(1);
    }

    LOG.info("Replay PASSED");
  }
}
