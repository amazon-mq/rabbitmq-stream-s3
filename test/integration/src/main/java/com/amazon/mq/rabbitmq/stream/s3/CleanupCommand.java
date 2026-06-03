package com.amazon.mq.rabbitmq.stream.s3;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

@CommandLine.Command(
    name = "cleanup",
    description =
        "Delete all streams, close all connections, and optionally "
            + "clean the S3 bucket. Run before a test to ensure a clean environment.")
public class CleanupCommand implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(CleanupCommand.class);
  private static final String S3_PREFIX = "rabbitmq/stream/";
  private static final int DELETE_BATCH_SIZE = 1000;

  @CommandLine.Option(
      names = {"--mgmt-uri"},
      description = "Management API base URI",
      defaultValue = "http://localhost:15672")
  private String mgmtUri;

  @CommandLine.Option(
      names = {"--s3-bucket"},
      description = "S3 bucket to clean (omit to skip S3 cleanup)")
  private String s3Bucket;

  @CommandLine.Option(
      names = {"--s3-region"},
      description = "AWS region for the S3 bucket",
      defaultValue = "us-west-2")
  private String s3Region;

  @Override
  public void run() {
    LOG.info("Cleaning environment via {}", mgmtUri);
    ManagementApi mgmt = new ManagementApi(mgmtUri);

    List<String> streams = mgmt.listStreams();
    if (streams.isEmpty()) {
      LOG.info("No streams to delete");
    } else {
      LOG.info("Deleting {} stream(s)...", streams.size());
      for (String stream : streams) {
        if (mgmt.deleteStream(stream)) {
          LOG.info("  Deleted: {}", stream);
        } else {
          LOG.error("  Failed to delete: {}", stream);
        }
      }
    }

    int closed = mgmt.closeAllConnections();
    if (closed > 0) {
      LOG.info("Closed {} connection(s)", closed);
    } else {
      LOG.info("No connections to close");
    }

    if (s3Bucket != null && !s3Bucket.isEmpty()) {
      cleanS3();
    } else {
      LOG.info("S3 cleanup skipped (no --s3-bucket specified)");
    }

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    List<String> remaining = mgmt.listStreams();
    if (remaining.isEmpty()) {
      LOG.info("Cleanup complete: environment is clean");
    } else {
      LOG.error("Cleanup incomplete: {} stream(s) still exist", remaining.size());
      System.exit(1);
    }
  }

  private void cleanS3() {
    LOG.info("Cleaning S3 bucket: {} prefix: {} region: {}", s3Bucket, S3_PREFIX, s3Region);

    try (S3Client s3 = S3Client.builder().region(Region.of(s3Region)).build()) {
      int totalDeleted = 0;
      String continuationToken = null;

      do {
        ListObjectsV2Request.Builder listBuilder =
            ListObjectsV2Request.builder()
                .bucket(s3Bucket)
                .prefix(S3_PREFIX)
                .maxKeys(DELETE_BATCH_SIZE);
        if (continuationToken != null) {
          listBuilder.continuationToken(continuationToken);
        }

        ListObjectsV2Response response = s3.listObjectsV2(listBuilder.build());
        List<S3Object> objects = response.contents();

        if (objects.isEmpty()) {
          break;
        }

        List<ObjectIdentifier> keys = new ArrayList<>(objects.size());
        for (S3Object obj : objects) {
          keys.add(ObjectIdentifier.builder().key(obj.key()).build());
        }

        s3.deleteObjects(
            DeleteObjectsRequest.builder()
                .bucket(s3Bucket)
                .delete(Delete.builder().objects(keys).quiet(true).build())
                .build());

        totalDeleted += keys.size();
        continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;

      } while (continuationToken != null);

      LOG.info("Deleted {} S3 object(s)", totalDeleted);
    } catch (Exception e) {
      LOG.error("S3 cleanup failed: {}", e.getMessage());
      System.exit(1);
    }
  }
}
