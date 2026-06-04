package com.amazon.mq.rabbitmq.stream.s3;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3Monitor implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(S3Monitor.class);

  private final S3Client s3;
  private final String bucket;
  private final String prefix;
  private long previousCount = -1;
  private int monotonicGrowthIntervals = 0;

  S3Monitor(String bucket, String region, String streamName) {
    this.s3 = S3Client.builder().region(Region.of(region)).build();
    this.bucket = bucket;
    this.prefix = "rabbitmq/stream/" + streamName + "/";
  }

  static class Snapshot {
    final long objectCount;
    final long delta;
    final int monotonicGrowthIntervals;

    Snapshot(long objectCount, long delta, int monotonicGrowthIntervals) {
      this.objectCount = objectCount;
      this.delta = delta;
      this.monotonicGrowthIntervals = monotonicGrowthIntervals;
    }

    boolean retentionActive() {
      return delta < 0;
    }
  }

  Snapshot snapshot() {
    long count = countObjects();
    long delta = previousCount >= 0 ? count - previousCount : 0;

    if (previousCount >= 0) {
      if (delta > 0) {
        monotonicGrowthIntervals++;
      } else if (delta < 0) {
        monotonicGrowthIntervals = 0;
      }
    }

    previousCount = count;
    return new Snapshot(count, delta, monotonicGrowthIntervals);
  }

  private long countObjects() {
    long total = 0;
    String continuationToken = null;
    try {
      do {
        ListObjectsV2Request.Builder builder =
            ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(1000);
        if (continuationToken != null) {
          builder.continuationToken(continuationToken);
        }
        ListObjectsV2Response response = s3.listObjectsV2(builder.build());
        total += response.keyCount();
        continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
      } while (continuationToken != null);
    } catch (Exception e) {
      LOG.debug("S3 object count failed: {}", e.getMessage());
      return previousCount >= 0 ? previousCount : 0;
    }
    return total;
  }

  int deleteAll() {
    LOG.info("Deleting all S3 objects under prefix: {}", prefix);
    int totalDeleted = 0;
    String continuationToken = null;
    try {
      do {
        ListObjectsV2Request.Builder listBuilder =
            ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(1000);
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
                .bucket(bucket)
                .delete(Delete.builder().objects(keys).quiet(true).build())
                .build());
        totalDeleted += keys.size();
        continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
      } while (continuationToken != null);
    } catch (Exception e) {
      LOG.error("S3 deleteAll failed: {}", e.getMessage());
    }
    LOG.info("Deleted {} S3 object(s) under prefix: {}", totalDeleted, prefix);
    return totalDeleted;
  }

  @Override
  public void close() {
    s3.close();
  }
}
