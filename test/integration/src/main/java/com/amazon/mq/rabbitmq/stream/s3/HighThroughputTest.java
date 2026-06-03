package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.StreamException;
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

    try (Environment env = cluster.buildEnvironment()) {
      setupStream(env, mgmt);
      long published = publishPhase(env, mgmt);

      LOG.info("Publish phase complete: confirmed={}", published);
      LOG.info("Waiting for management stats to stabilize...");
      long expectedMessages = mgmt.getStableMessageCount(cluster.stream, 12, 5000);
      if (expectedMessages <= 0) {
        expectedMessages = published;
      }
      LOG.info("Expected messages for replay: {}", expectedMessages);

      replayPhase(env, expectedMessages);
      LOG.info("SUCCESS: high-throughput test passed");
    } catch (Exception e) {
      LOG.error("FAILED", e);
      System.exit(1);
    }
  }

  private void setupStream(Environment env, ManagementApi mgmt) {
    LOG.info("Deleting stream if it exists: {}", cluster.stream);
    mgmt.deleteStream(cluster.stream);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    LOG.info("Creating stream: {} (max-length-bytes={})", cluster.stream, maxLengthBytes);
    try {
      env.streamCreator()
          .stream(cluster.stream)
          .maxLengthBytes(ByteCapacity.B(maxLengthBytes))
          .create();
    } catch (StreamException e) {
      if (e.getMessage() != null && e.getMessage().contains("precondition")) {
        LOG.info("Stream already exists with compatible settings");
      } else {
        throw e;
      }
    }
    LOG.info("Stream ready");
  }

  private long publishPhase(Environment env, ManagementApi mgmt) throws InterruptedException {
    byte[] body = new byte[messageSize];
    AtomicLong totalPublished = new AtomicLong(0);
    AtomicLong totalConsumed = new AtomicLong(0);
    AtomicLong headOffset = new AtomicLong(-1);
    CountDownLatch stop = new CountDownLatch(1);

    List<Consumer> consumers = new ArrayList<>();
    for (int i = 0; i < numConsumers; i++) {
      var builder =
          env.consumerBuilder().stream(cluster.stream).offset(OffsetSpecification.next());
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

    LOG.info("Publishing for {}s...", durationSeconds);

    while (System.currentTimeMillis() < deadline) {
      Thread.sleep(1000);
      long now = System.currentTimeMillis();
      if (now >= nextReport) {
        long elapsed = (now - startTime) / 1000;
        long pub = totalPublished.get();
        long cons = totalConsumed.get();
        double pubRate = pub * 1000.0 / (now - startTime);
        LOG.info(
            "Publish [{}s]: published={} ({} msg/s) consumed={} head-offset={}",
            elapsed,
            pub,
            String.format("%.0f", pubRate),
            cons,
            headOffset.get());

        if (mgmt.hasMemoryAlarm()) {
          LOG.error("MEMORY ALARM detected - aborting");
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

    return totalPublished.get();
  }

  private void replayPhase(Environment env, long expectedMessages) throws InterruptedException {
    long threshold = expectedMessages * 95 / 100;
    LOG.info(
        "Replay: consuming from 'first', expecting >= {} messages (95% of {}), timeout={}s",
        threshold,
        expectedMessages,
        replayTimeoutSeconds);

    AtomicLong consumed = new AtomicLong(0);
    AtomicLong lastOffset = new AtomicLong(-1);
    CountDownLatch done = new CountDownLatch(1);

    var builder =
        env.consumerBuilder().stream(cluster.stream).offset(OffsetSpecification.first());
    builder.flow().initialCredits(10);
    Consumer consumer =
        builder
            .messageHandler(
                (context, message) -> {
                  long count = consumed.incrementAndGet();
                  lastOffset.set(context.offset());
                  if (count >= threshold) {
                    done.countDown();
                  }
                })
            .build();

    long startTime = System.currentTimeMillis();
    long nextReport = startTime + Duration.ofSeconds(progressInterval).toMillis();

    while (!done.await(1, TimeUnit.SECONDS)) {
      long elapsed = System.currentTimeMillis() - startTime;
      if (elapsed > Duration.ofSeconds(replayTimeoutSeconds).toMillis()) {
        consumer.close();
        LOG.error(
            "REPLAY TIMEOUT: consumed {} of {} expected in {}s (last offset={})",
            consumed.get(),
            expectedMessages,
            replayTimeoutSeconds,
            lastOffset.get());
        System.exit(1);
      }
      long now = System.currentTimeMillis();
      if (now >= nextReport) {
        long current = consumed.get();
        double rate = current * 1000.0 / (now - startTime);
        long elapsedSec = (now - startTime) / 1000;
        LOG.info(
            "Replay [{}s]: {}/{} ({} msg/s, offset={})",
            elapsedSec,
            current,
            expectedMessages,
            String.format("%.0f", rate),
            lastOffset.get());
        nextReport = now + Duration.ofSeconds(progressInterval).toMillis();
      }
    }

    consumer.close();
    long total = consumed.get();
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
