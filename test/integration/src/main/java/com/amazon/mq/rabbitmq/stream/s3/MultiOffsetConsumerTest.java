package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
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
        "Run a head-tracking consumer and a lagging consumer (from 'first') "
            + "simultaneously on the same stream while publishing. "
            + "Verifies both make progress without crashes.")
public class MultiOffsetConsumerTest implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(MultiOffsetConsumerTest.class);

  @CommandLine.Mixin private ClusterOptions cluster;

  @CommandLine.Option(
      names = "--duration",
      description = "How long to run in seconds",
      defaultValue = "300")
  private int durationSeconds;

  @CommandLine.Option(
      names = "--message-size",
      description = "Message body size in bytes",
      defaultValue = "1024")
  private int messageSize;

  @CommandLine.Option(
      names = "--progress-interval",
      description = "Seconds between progress reports",
      defaultValue = "10")
  private int progressInterval;

  @Override
  public void run() {
    LOG.info(
        "Starting multi-offset-consumer: stream={} duration={}s", cluster.stream, durationSeconds);

    AtomicLong headConsumed = new AtomicLong(0);
    AtomicLong laggingConsumed = new AtomicLong(0);
    AtomicLong published = new AtomicLong(0);
    AtomicLong headOffset = new AtomicLong(-1);
    AtomicLong laggingOffset = new AtomicLong(-1);

    try (Environment env = cluster.buildEnvironment()) {
      var headBuilder = env.consumerBuilder().stream(cluster.stream)
              .offset(OffsetSpecification.next());
      headBuilder.flow().initialCredits(10);
      Consumer headConsumer =
          headBuilder
              .messageHandler(
                  (context, message) -> {
                    headConsumed.incrementAndGet();
                    headOffset.set(context.offset());
                  })
              .build();

      var laggingBuilder = env.consumerBuilder().stream(cluster.stream)
              .offset(OffsetSpecification.first());
      laggingBuilder.flow().initialCredits(10);
      Consumer laggingConsumer =
          laggingBuilder
              .messageHandler(
                  (context, message) -> {
                    laggingConsumed.incrementAndGet();
                    laggingOffset.set(context.offset());
                  })
              .build();

      byte[] body = new byte[messageSize];
      Producer producer = env.producerBuilder().stream(cluster.stream).build();

      CountDownLatch stop = new CountDownLatch(1);
      long startTime = System.currentTimeMillis();
      long deadline = startTime + Duration.ofSeconds(durationSeconds).toMillis();
      long nextReport = startTime + Duration.ofSeconds(progressInterval).toMillis();

      Thread publisherThread =
          new Thread(
              () -> {
                try {
                  while (!stop.await(0, TimeUnit.MILLISECONDS)) {
                    producer.send(
                        producer.messageBuilder().addData(body).build(), confirmationStatus -> {});
                    published.incrementAndGet();
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              },
              "publisher");
      publisherThread.start();

      while (System.currentTimeMillis() < deadline) {
        Thread.sleep(1000);
        long now = System.currentTimeMillis();
        if (now >= nextReport) {
          long elapsed = (now - startTime) / 1000;
          LOG.info(
              "Progress [{}s]: published={} head={} (offset={}) lagging={} (offset={})",
              elapsed,
              published.get(),
              headConsumed.get(),
              headOffset.get(),
              laggingConsumed.get(),
              laggingOffset.get());
          nextReport = now + Duration.ofSeconds(progressInterval).toMillis();
        }
      }

      stop.countDown();
      publisherThread.join(5000);
      producer.close();
      headConsumer.close();
      laggingConsumer.close();

      long totalPublished = published.get();
      long totalHead = headConsumed.get();
      long totalLagging = laggingConsumed.get();

      LOG.info("Final: published={} head={} lagging={}", totalPublished, totalHead, totalLagging);

      if (totalHead == 0) {
        LOG.error("FAIL: head consumer received zero messages");
        System.exit(1);
      }
      if (totalLagging == 0) {
        LOG.error("FAIL: lagging consumer received zero messages");
        System.exit(1);
      }

      LOG.info("SUCCESS: both consumers made progress");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted");
      System.exit(1);
    } catch (Exception e) {
      LOG.error("Fatal error", e);
      System.exit(1);
    }
  }
}
