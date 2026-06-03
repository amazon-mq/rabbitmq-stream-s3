package com.amazon.mq.rabbitmq.stream.s3;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManagementApi {

  private static final Logger LOG = LoggerFactory.getLogger(ManagementApi.class);

  private final String baseUri;
  private final HttpClient client;
  private final String authHeader;

  ManagementApi(String baseUri) {
    this.baseUri = baseUri;
    this.client = HttpClient.newHttpClient();
    this.authHeader =
        "Basic " + Base64.getEncoder().encodeToString("guest:guest".getBytes());
  }

  long getMessageCount(String stream) {
    try {
      String body = get("/api/queues/%2F/" + stream);
      if (body == null) return -1;
      int idx = body.indexOf("\"messages\":");
      if (idx < 0) return -1;
      String sub = body.substring(idx + 11);
      int end = sub.indexOf(',');
      if (end < 0) end = sub.indexOf('}');
      return Long.parseLong(sub.substring(0, end).trim());
    } catch (Exception e) {
      LOG.debug("getMessageCount failed: {}", e.getMessage());
      return -1;
    }
  }

  boolean hasMemoryAlarm() {
    try {
      String body = get("/api/nodes");
      return body != null && body.contains("\"mem_alarm\":true");
    } catch (Exception e) {
      LOG.debug("hasMemoryAlarm check failed: {}", e.getMessage());
      return false;
    }
  }

  List<String> listStreams() {
    List<String> streams = new ArrayList<>();
    try {
      String body = get("/api/queues/%2F");
      if (body == null) return streams;
      Pattern p = Pattern.compile("\"name\":\"([^\"]+)\"[^}]*\"type\":\"stream\"");
      Matcher m = p.matcher(body);
      while (m.find()) {
        streams.add(m.group(1));
      }
    } catch (Exception e) {
      LOG.debug("listStreams failed: {}", e.getMessage());
    }
    return streams;
  }

  boolean deleteStream(String stream) {
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(baseUri + "/api/queues/%2F/" + stream))
              .header("Authorization", authHeader)
              .timeout(Duration.ofSeconds(10))
              .DELETE()
              .build();
      HttpResponse<String> response =
          client.send(request, HttpResponse.BodyHandlers.ofString());
      return response.statusCode() == 204 || response.statusCode() == 404;
    } catch (Exception e) {
      LOG.debug("deleteStream failed: {}", e.getMessage());
      return false;
    }
  }

  int closeAllConnections() {
    int closed = 0;
    try {
      String body = get("/api/connections");
      if (body == null) return 0;
      Pattern p = Pattern.compile("\"name\":\"([^\"]+)\"");
      Matcher m = p.matcher(body);
      List<String> names = new ArrayList<>();
      while (m.find()) {
        names.add(m.group(1));
      }
      for (String name : names) {
        try {
          HttpRequest request =
              HttpRequest.newBuilder()
                  .uri(URI.create(baseUri + "/api/connections/" + urlEncode(name)))
                  .header("Authorization", authHeader)
                  .timeout(Duration.ofSeconds(10))
                  .DELETE()
                  .build();
          client.send(request, HttpResponse.BodyHandlers.ofString());
          closed++;
        } catch (Exception e) {
          LOG.debug("Failed to close connection {}: {}", name, e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.debug("closeAllConnections failed: {}", e.getMessage());
    }
    return closed;
  }

  private String get(String path) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUri + path))
            .header("Authorization", authHeader)
            .timeout(Duration.ofSeconds(10))
            .build();
    HttpResponse<String> response =
        client.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() == 200) {
      return response.body();
    }
    return null;
  }

  private static String urlEncode(String value) {
    return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8);
  }
}
