package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "remote-only-read",
    description =
        "Consume from offset 'first' and count exact messages. "
            + "Exits when the consumed count reaches --expected-messages "
            + "or --timeout elapses.")
public class RemoteOnlyReadTest implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteOnlyReadTest.class);

  @CommandLine.Mixin private ClusterOptions cluster;

  @CommandLine.Option(
      names = "--expected-messages",
      description = "Number of messages to consume before exiting successfully",
      required = true)
  private long expectedMessages;

  @CommandLine.Option(
      names = "--timeout",
      description = "Maximum seconds to wait for expected messages",
      defaultValue = "300")
  private int timeoutSeconds;

  @CommandLine.Option(
      names = "--progress-interval",
      description = "Seconds between progress reports",
      defaultValue = "10")
  private int progressInterval;

  @Override
  public void run() {
    LOG.info(
        "Starting remote-only-read: stream={} expected={} timeout={}s",
        cluster.stream,
        expectedMessages,
        timeoutSeconds);

    AtomicLong consumed = new AtomicLong(0);
    AtomicLong lastOffset = new AtomicLong(-1);
    CountDownLatch done = new CountDownLatch(1);

    try (Environment env = cluster.buildEnvironment()) {
      var consumerBuilder =
          env.consumerBuilder().stream(cluster.stream).offset(OffsetSpecification.first());
      consumerBuilder.flow().initialCredits(10);
      Consumer consumer =
          consumerBuilder
              .messageHandler(
                  (context, message) -> {
                    long count = consumed.incrementAndGet();
                    lastOffset.set(context.offset());
                    if (count >= expectedMessages) {
                      done.countDown();
                    }
                  })
              .build();

      long startTime = System.currentTimeMillis();
      long nextReport = startTime + Duration.ofSeconds(progressInterval).toMillis();

      while (!done.await(1, TimeUnit.SECONDS)) {
        long elapsed = System.currentTimeMillis() - startTime;
        if (elapsed > Duration.ofSeconds(timeoutSeconds).toMillis()) {
          LOG.error(
              "TIMEOUT: consumed {} of {} expected in {}s (last offset={})",
              consumed.get(),
              expectedMessages,
              timeoutSeconds,
              lastOffset.get());
          consumer.close();
          System.exit(1);
        }
        if (System.currentTimeMillis() >= nextReport) {
          long current = consumed.get();
          double rate = current * 1000.0 / elapsed;
          LOG.info(
              "Progress: {}/{} messages ({} msg/s, offset={}, elapsed={}s)",
              current,
              expectedMessages,
              String.format("%.0f", rate),
              lastOffset.get(),
              elapsed / 1000);
          nextReport = System.currentTimeMillis() + Duration.ofSeconds(progressInterval).toMillis();
        }
      }

      consumer.close();
      long elapsedMs = System.currentTimeMillis() - startTime;

      LOG.info(
          "SUCCESS: consumed {} messages in {}s (last offset={})",
          consumed.get(),
          elapsedMs / 1000,
          lastOffset.get());
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
