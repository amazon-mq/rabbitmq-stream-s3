package com.amazon.mq.rabbitmq.stream.s3;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

// Publish-phase options shared by every test harness that produces messages.
// Consolidates the duration, message-size, and max-length-bytes options that
// were previously duplicated per command, and adds amount-based publish limits.
// Publishing stops on whichever of the duration, message, or byte limit is
// reached first; each is independently disabled with 0 (the duration aside,
// which defaults to a finite value).
public class PublishOptions {

  private static final Logger LOG = LoggerFactory.getLogger(PublishOptions.class);

  @CommandLine.Option(
      names = "--publish-duration",
      description = "Publishing duration in seconds (0 for no time limit)",
      defaultValue = "300")
  int publishDurationSeconds;

  @CommandLine.Option(
      names = "--publish-messages",
      description = "Stop publishing after this many confirmed messages (0 for no limit)",
      defaultValue = "0")
  long publishMessages;

  @CommandLine.Option(
      names = "--publish-bytes",
      description =
          "Stop publishing after this many confirmed message-body bytes (0 for no limit). "
              + "Counts message bodies only, not on-disk framing, so it is a lower bound on the "
              + "segment bytes that max-length-bytes measures",
      defaultValue = "0")
  long publishBytes;

  @CommandLine.Option(
      names = "--message-size",
      description = "Message body size in bytes (minimum 8 for the sequence header)",
      defaultValue = "1024")
  int messageSize;

  @CommandLine.Option(
      names = "--max-length-bytes",
      description = "Stream max-length-bytes (remote tier retention)",
      defaultValue = "500000000")
  long maxLengthBytes;

  // At least one publish stop condition must be set, or publishing would never
  // terminate. Logs and exits on a misconfiguration rather than hanging.
  void validateStopConditions() {
    if (messageSize < 8) {
      LOG.error("FAIL: --message-size must be >= 8 (need 8 bytes for the sequence header)");
      System.exit(1);
    }
    if (publishDurationSeconds <= 0 && publishMessages <= 0 && publishBytes <= 0) {
      LOG.error(
          "FAIL: set at least one of --publish-duration, --publish-messages, --publish-bytes");
      System.exit(1);
    }
  }

  // True once any active publish limit has been reached. Confirmed body bytes
  // are derived from the confirmed count, since every message body is
  // messageSize bytes.
  boolean publishStopReached(long startTimeMillis, long confirmedMessages) {
    if (publishDurationSeconds > 0
        && System.currentTimeMillis() - startTimeMillis
            >= Duration.ofSeconds(publishDurationSeconds).toMillis()) {
      return true;
    }
    if (publishMessages > 0 && confirmedMessages >= publishMessages) {
      return true;
    }
    return publishBytes > 0 && confirmedMessages * messageSize >= publishBytes;
  }

  // Describes the active stop conditions for the startup log.
  String describeStop() {
    List<String> parts = new ArrayList<>();
    if (publishDurationSeconds > 0) {
      parts.add(publishDurationSeconds + "s elapse");
    }
    if (publishMessages > 0) {
      parts.add(publishMessages + " confirmed messages");
    }
    if (publishBytes > 0) {
      parts.add(publishBytes + " confirmed body bytes");
    }
    return String.join(" or ", parts) + " (whichever first)";
  }
}
