package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.ClientParameters;
import com.rabbitmq.http.client.domain.ConnectionInfo;
import com.rabbitmq.http.client.domain.NodeInfo;
import com.rabbitmq.http.client.domain.QueueInfo;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManagementApi {

  private static final Logger LOG = LoggerFactory.getLogger(ManagementApi.class);
  private static final String VHOST = "/";

  private final Client client;

  ManagementApi(String baseUri) {
    try {
      this.client =
          new Client(
              new ClientParameters().url(baseUri + "/api/").username("guest").password("guest"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create management API client", e);
    }
  }

  long getMessageCount(String stream) {
    try {
      QueueInfo info = client.getQueue(VHOST, stream);
      if (info == null) return -1;
      return info.getMessagesReady();
    } catch (Exception e) {
      LOG.debug("getMessageCount failed: {}", e.getMessage());
      return -1;
    }
  }

  long getStableMessageCount(String stream, int maxAttempts, long intervalMs) {
    long previous = -1;
    for (int i = 0; i < maxAttempts; i++) {
      long current = getMessageCount(stream);
      if (current > 0 && current == previous) {
        return current;
      }
      previous = current;
      try {
        Thread.sleep(intervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return current;
      }
    }
    LOG.warn(
        "Message count did not stabilize after {} attempts, using last value: {}",
        maxAttempts,
        previous);
    return previous;
  }

  boolean hasMemoryAlarm() {
    try {
      List<NodeInfo> nodes = client.getNodes();
      for (NodeInfo node : nodes) {
        if (node.isMemoryAlarmActive()) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      LOG.debug("hasMemoryAlarm check failed: {}", e.getMessage());
      return false;
    }
  }

  List<String> listStreams() {
    List<String> streams = new ArrayList<>();
    try {
      List<QueueInfo> queues = client.getQueues(VHOST);
      if (queues == null) return streams;
      for (QueueInfo q : queues) {
        if ("stream".equals(q.getType())) {
          streams.add(q.getName());
        }
      }
    } catch (Exception e) {
      LOG.debug("listStreams failed: {}", e.getMessage());
    }
    return streams;
  }

  boolean deleteStream(String stream) {
    try {
      client.deleteQueue(VHOST, stream);
      return true;
    } catch (Exception e) {
      if (e.getMessage() != null && e.getMessage().contains("404")) {
        return true;
      }
      LOG.debug("deleteStream failed: {}", e.getMessage());
      return false;
    }
  }

  int closeAllConnections() {
    int closed = 0;
    try {
      List<ConnectionInfo> connections = client.getConnections();
      if (connections == null) return 0;
      for (ConnectionInfo conn : connections) {
        try {
          client.closeConnection(conn.getName());
          closed++;
        } catch (Exception e) {
          LOG.debug("Failed to close connection {}: {}", conn.getName(), e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.debug("closeAllConnections failed: {}", e.getMessage());
    }
    return closed;
  }
}
