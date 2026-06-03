package com.amazon.mq.rabbitmq.stream.s3;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetricsClient {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsClient.class);
  private static final Pattern BYTES_RECEIVED_PATTERN =
      Pattern.compile("^rabbitmq_stream_s3_bytes_received\\s+(\\d+)", Pattern.MULTILINE);
  private static final Pattern BYTES_SENT_PATTERN =
      Pattern.compile("^rabbitmq_stream_s3_bytes_sent\\s+(\\d+)", Pattern.MULTILINE);

  private final List<String> endpoints;
  private final HttpClient client;
  private long previousBytesReceived = -1;
  private long previousBytesSent = -1;

  MetricsClient(List<String> endpoints) {
    this.endpoints = endpoints;
    this.client = HttpClient.newHttpClient();
  }

  static class Snapshot {
    final long bytesReceived;
    final long bytesSent;
    final long deltaBytesReceived;
    final long deltaBytesSent;

    Snapshot(long bytesReceived, long bytesSent, long deltaReceived, long deltaSent) {
      this.bytesReceived = bytesReceived;
      this.bytesSent = bytesSent;
      this.deltaBytesReceived = deltaReceived;
      this.deltaBytesSent = deltaSent;
    }

    double receivedMiBPerS(long intervalSeconds) {
      if (intervalSeconds <= 0) return 0;
      return deltaBytesReceived / (1048576.0 * intervalSeconds);
    }

    double sentMiBPerS(long intervalSeconds) {
      if (intervalSeconds <= 0) return 0;
      return deltaBytesSent / (1048576.0 * intervalSeconds);
    }
  }

  Snapshot snapshot() {
    long totalReceived = 0;
    long totalSent = 0;

    for (String endpoint : endpoints) {
      try {
        HttpRequest request =
            HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/metrics"))
                .timeout(Duration.ofSeconds(5))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
          String body = response.body();
          totalReceived += extractMetric(body, BYTES_RECEIVED_PATTERN);
          totalSent += extractMetric(body, BYTES_SENT_PATTERN);
        }
      } catch (Exception e) {
        LOG.debug("Failed to scrape {}: {}", endpoint, e.getMessage());
      }
    }

    long deltaReceived = previousBytesReceived >= 0 ? totalReceived - previousBytesReceived : 0;
    long deltaSent = previousBytesSent >= 0 ? totalSent - previousBytesSent : 0;
    previousBytesReceived = totalReceived;
    previousBytesSent = totalSent;

    return new Snapshot(totalReceived, totalSent, deltaReceived, deltaSent);
  }

  private long extractMetric(String body, Pattern pattern) {
    long total = 0;
    Matcher m = pattern.matcher(body);
    while (m.find()) {
      total += Long.parseLong(m.group(1));
    }
    return total;
  }
}
