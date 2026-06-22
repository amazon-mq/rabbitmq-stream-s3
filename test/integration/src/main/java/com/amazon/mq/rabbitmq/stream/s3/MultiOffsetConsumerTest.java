package com.amazon.mq.rabbitmq.stream.s3;

import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerFlowStrategy;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "multi-offset-consumer",
    description =
        "Run a head-tracking consumer and a rate-limited lagging consumer "
            + "(from 'first') simultaneously on the same stream while publishing. "
            + "Verifies both make progress, the lagging consumer reads from S3, "
            + "and no alarms fire.")
public class MultiOffsetConsumerTest implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MultiOffsetConsumerTest.class);

  @CommandLine.Mixin private ClusterOptions cluster;

  @CommandLine.Mixin private PublishOptions publish;

  @CommandLine.Option(
      names = "--consumer-rate",
      description = "Rate limit for lagging consumer in msg/s",
      defaultValue = "5000")
  private int consumerRate;

  @CommandLine.Option(
      names = "--progress-interval",
      description = "Seconds between progress reports",
      defaultValue = "30")
  private int progressInterval;

  @CommandLine.Option(
      names = "--warmup",
      description = "Seconds to publish before starting the lagging consumer",
      defaultValue = "120")
  private int warmupSeconds;

  @Override
  public void run() {
    LOG.info(
        "Starting multi-offset-consumer: stream={} publish-stop=[{}] consumer-rate={} msg/s"
            + " max-length-bytes={} warmup={}s",
        cluster.stream,
        publish.describeStop(),
        consumerRate,
        publish.maxLengthBytes,
        warmupSeconds);

    publish.validateStopConditions();

    ManagementApi mgmt = new ManagementApi(cluster.mgmtUri);
    MetricsClient metrics = new MetricsClient(cluster.metricsUris);
    ClusterHealthMonitor health = new ClusterHealthMonitor(cluster.mgmtUri);
    S3Monitor s3Monitor = cluster.buildS3Monitor();

    try (Environment env = cluster.buildEnvironment()) {
      TestSetup.setupStream(env, mgmt, s3Monitor, cluster.stream, publish.maxLengthBytes);

      AtomicLong totalPublished = new AtomicLong(0);
      AtomicLong headConsumed = new AtomicLong(0);
      AtomicLong laggingConsumed = new AtomicLong(0);
      AtomicLong headOffset = new AtomicLong(-1);
      AtomicLong laggingOffset = new AtomicLong(-1);
      CountDownLatch stop = new CountDownLatch(1);

      byte[] body = new byte[publish.messageSize];
      Producer producer = env.producerBuilder().stream(cluster.stream).build();

      Thread publisherThread =
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
              "publisher");
      publisherThread.start();

      var headBuilder =
          env.consumerBuilder().stream(cluster.stream).offset(OffsetSpecification.next());
      headBuilder.flow().initialCredits(10);
      Consumer headConsumer =
          headBuilder
              .messageHandler(
                  (context, message) -> {
                    headConsumed.incrementAndGet();
                    headOffset.set(context.offset());
                  })
              .build();

      LOG.info("Publishing for {}s warmup before starting lagging consumer...", warmupSeconds);
      long warmupDeadline =
          System.currentTimeMillis() + Duration.ofSeconds(warmupSeconds).toMillis();
      while (System.currentTimeMillis() < warmupDeadline) {
        Thread.sleep(5000);
        long pub = totalPublished.get();
        long s3Objects = s3Monitor != null ? s3Monitor.snapshot().objectCount : 0;
        LOG.info(
            "  Warmup: published={} head-offset={} s3-objects={}",
            pub,
            headOffset.get(),
            s3Objects);
      }

      RateLimiter rateLimiter = RateLimiter.create(consumerRate);
      // Retry subscribe to work around issue #191
      Consumer laggingConsumer = null;
      for (int attempt = 1; attempt <= 5; attempt++) {
        try {
          var laggingBuilder =
              env.consumerBuilder().stream(cluster.stream).offset(OffsetSpecification.first());
          // Use creditWhenHalfMessagesProcessed so the server only delivers new
          // chunks after the handler has processed half the current chunk. Combined
          // with the rate limiter, this throttles wire-level delivery and keeps the
          // consumer's offset genuinely behind the head.
          laggingBuilder.flow().strategy(ConsumerFlowStrategy.creditWhenHalfMessagesProcessed(1));
          laggingConsumer =
              laggingBuilder
                  .messageHandler(
                      (context, message) -> {
                        rateLimiter.acquire();
                        laggingConsumed.incrementAndGet();
                        laggingOffset.set(context.offset());
                        context.processed();
                      })
                  .build();
          break;
        } catch (StreamException e) {
          if (attempt == 5) {
            throw e;
          }
          LOG.warn(
              "Lagging consumer subscribe attempt {}/5 failed: {} — retrying in 15s",
              attempt,
              e.getMessage());
          Thread.sleep(15000);
        }
      }

      LOG.info("Lagging consumer started at 'first' with rate limit {} msg/s", consumerRate);

      // The publish stop conditions bound the post-warmup run. The duration is
      // measured from here (after warmup), matching the original behavior; the
      // message and byte limits count all confirmed messages, warmup included.
      long startTime = System.currentTimeMillis();
      long nextReport = startTime + Duration.ofSeconds(progressInterval).toMillis();
      boolean s3RecvSeen = false;

      while (!publish.publishStopReached(startTime, totalPublished.get())) {
        Thread.sleep(1000);
        long now = System.currentTimeMillis();
        if (now >= nextReport) {
          long elapsed = (now - startTime) / 1000;
          MetricsClient.Snapshot snap = metrics.snapshot();
          ClusterHealthMonitor.Snapshot healthSnap = health.snapshot();

          String s3ObjectInfo = "";
          if (s3Monitor != null) {
            S3Monitor.Snapshot s3Snap = s3Monitor.snapshot();
            s3ObjectInfo =
                String.format(" s3-objects=%d (delta=%+d)", s3Snap.objectCount, s3Snap.delta);
          }

          if (snap.deltaBytesReceived > 0) {
            s3RecvSeen = true;
          }

          LOG.info(
              "Progress [{}s]: published={} head={} (offset={}) lagging={} (offset={})"
                  + " s3-recv={} MiB/s{}",
              elapsed,
              totalPublished.get(),
              headConsumed.get(),
              headOffset.get(),
              laggingConsumed.get(),
              laggingOffset.get(),
              String.format("%.1f", snap.receivedMiBPerS(progressInterval)),
              s3ObjectInfo);
          LOG.info("  Health: {}", healthSnap.format());

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
      publisherThread.join(5000);
      producer.close();
      headConsumer.close();
      if (laggingConsumer != null) {
        laggingConsumer.close();
      }

      long finalHead = headConsumed.get();
      long finalLagging = laggingConsumed.get();
      long finalHeadOffset = headOffset.get();
      long finalLaggingOffset = laggingOffset.get();

      LOG.info(
          "Final: published={} head={} (offset={}) lagging={} (offset={})",
          totalPublished.get(),
          finalHead,
          finalHeadOffset,
          finalLagging,
          finalLaggingOffset);

      if (finalHead == 0) {
        LOG.error("FAIL: head consumer received zero messages");
        System.exit(1);
      }
      if (finalLagging == 0) {
        LOG.error("FAIL: lagging consumer received zero messages");
        System.exit(1);
      }
      if (finalLaggingOffset >= finalHeadOffset) {
        LOG.error(
            "FAIL: lagging consumer caught up to head (lagging={} >= head={})",
            finalLaggingOffset,
            finalHeadOffset);
        System.exit(1);
      }
      if (s3Monitor != null && !s3RecvSeen) {
        LOG.error(
            "FAIL: S3 bytes received was never observed — lagging consumer may not be reading from"
                + " remote tier");
        System.exit(1);
      }

      LOG.info("SUCCESS: multi-offset-consumer test passed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted");
      System.exit(1);
    } catch (Exception e) {
      LOG.error("Fatal error", e);
      System.exit(1);
    } finally {
      if (s3Monitor != null) {
        s3Monitor.close();
      }
    }
  }
}
