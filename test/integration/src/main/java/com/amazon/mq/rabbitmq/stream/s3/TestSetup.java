package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.StreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestSetup {

  private static final Logger LOG = LoggerFactory.getLogger(TestSetup.class);

  static void setupStream(
      Environment env,
      ManagementApi mgmt,
      S3Monitor s3Monitor,
      String stream,
      long maxLengthBytes) {
    if (s3Monitor != null) {
      s3Monitor.deleteAll();
    }

    LOG.info("Deleting stream if it exists: {}", stream);
    mgmt.deleteStream(stream);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    LOG.info("Creating stream: {} (max-length-bytes={})", stream, maxLengthBytes);
    try {
      env.streamCreator().stream(stream).maxLengthBytes(ByteCapacity.B(maxLengthBytes)).create();
    } catch (StreamException e) {
      if (e.getMessage() != null && e.getMessage().contains("precondition")) {
        LOG.info("Stream already exists with compatible settings");
      } else {
        throw e;
      }
    }
    LOG.info("Stream ready");
  }
}
