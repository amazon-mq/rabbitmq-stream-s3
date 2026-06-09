package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "stream-deletion",
    description =
        "Publish until S3 is populated, delete the stream, and verify "
            + "all S3 objects are removed by the Khepri trigger.")
public class StreamDeletionTest implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamDeletionTest.class);

  @CommandLine.Mixin private ClusterOptions cluster;

  @CommandLine.Option(
      names = "--duration",
      description = "Max seconds to publish waiting for S3 to populate",
      defaultValue = "120")
  private int durationSeconds;

  @CommandLine.Option(
      names = "--delete-timeout",
      description = "Max seconds to wait for S3 cleanup after deletion",
      defaultValue = "60")
  private int deleteTimeoutSeconds;

  @CommandLine.Option(
      names = "--max-length-bytes",
      description = "Stream max-length-bytes (small to force S3 spill quickly)",
      defaultValue = "500000000")
  private long maxLengthBytes;

  @CommandLine.Option(
      names = "--message-size",
      description = "Message body size in bytes",
      defaultValue = "1024")
  private int messageSize;

  @Override
  public void run() {
    LOG.info(
        "Starting stream-deletion test: stream={} max-length-bytes={} duration={}s"
            + " delete-timeout={}s",
        cluster.stream,
        maxLengthBytes,
        durationSeconds,
        deleteTimeoutSeconds);

    ManagementApi mgmt = new ManagementApi(cluster.mgmtUri);
    S3Monitor s3Monitor = cluster.buildS3Monitor();
    if (s3Monitor == null) {
      LOG.error("FAIL: --s3-bucket is required for this test");
      System.exit(1);
    }

    try (Environment env = cluster.buildEnvironment()) {
      TestSetup.setupStream(env, mgmt, s3Monitor, cluster.stream, maxLengthBytes);

      AtomicLong totalPublished = new AtomicLong(0);
      CountDownLatch stop = new CountDownLatch(1);
      byte[] body = new byte[messageSize];

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

      LOG.info("Publishing until S3 is populated (max {}s)...", durationSeconds);
      long deadline = System.currentTimeMillis() + Duration.ofSeconds(durationSeconds).toMillis();
      long s3ObjectCount = 0;
      while (System.currentTimeMillis() < deadline) {
        Thread.sleep(5000);
        S3Monitor.Snapshot snap = s3Monitor.snapshot();
        s3ObjectCount = snap.objectCount;
        LOG.info("  published={} s3-objects={}", totalPublished.get(), s3ObjectCount);
        if (s3ObjectCount > 0) {
          break;
        }
      }

      stop.countDown();
      publisherThread.join(5000);
      producer.close();

      if (s3ObjectCount == 0) {
        LOG.error("FAIL: S3 was never populated within {}s", durationSeconds);
        System.exit(1);
      }

      LOG.info("S3 populated: {} objects. Deleting stream: {}", s3ObjectCount, cluster.stream);

      mgmt.deleteStream(cluster.stream);

      LOG.info("Waiting for S3 cleanup (max {}s)...", deleteTimeoutSeconds);
      long deleteDeadline =
          System.currentTimeMillis() + Duration.ofSeconds(deleteTimeoutSeconds).toMillis();
      long remainingObjects = s3ObjectCount;
      while (System.currentTimeMillis() < deleteDeadline) {
        Thread.sleep(2000);
        S3Monitor.Snapshot snap = s3Monitor.snapshot();
        remainingObjects = snap.objectCount;
        LOG.info("  s3-objects={}", remainingObjects);
        if (remainingObjects == 0) {
          break;
        }
      }

      if (remainingObjects > 0) {
        LOG.error(
            "FAIL: {} S3 objects remain after {}s — Khepri trigger did not clean up",
            remainingObjects,
            deleteTimeoutSeconds);
        System.exit(1);
      }

      long messageCount = mgmt.getMessageCount(cluster.stream);
      if (messageCount >= 0) {
        LOG.error("FAIL: stream still exists in management API after deletion");
        System.exit(1);
      }

      LOG.info("SUCCESS: stream-deletion test passed");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted");
      System.exit(1);
    } catch (Exception e) {
      LOG.error("Fatal error", e);
      System.exit(1);
    } finally {
      s3Monitor.close();
    }
  }
}
