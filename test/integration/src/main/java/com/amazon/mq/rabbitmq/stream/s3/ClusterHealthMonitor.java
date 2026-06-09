package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.ClientParameters;
import com.rabbitmq.http.client.domain.NodeInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClusterHealthMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterHealthMonitor.class);

  private final Client client;

  ClusterHealthMonitor(String baseUri) {
    try {
      this.client =
          new Client(
              new ClientParameters().url(baseUri + "/api/").username("guest").password("guest"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create health monitor client", e);
    }
  }

  static class NodeSnapshot {
    final String name;
    final long memoryUsedBytes;
    final long diskFreeBytes;
    final long fileDescriptorsUsed;
    final boolean memoryAlarm;
    final boolean diskAlarm;

    NodeSnapshot(
        String name,
        long memoryUsedBytes,
        long diskFreeBytes,
        long fileDescriptorsUsed,
        boolean memoryAlarm,
        boolean diskAlarm) {
      this.name = name;
      this.memoryUsedBytes = memoryUsedBytes;
      this.diskFreeBytes = diskFreeBytes;
      this.fileDescriptorsUsed = fileDescriptorsUsed;
      this.memoryAlarm = memoryAlarm;
      this.diskAlarm = diskAlarm;
    }

    double memoryUsedMiB() {
      return memoryUsedBytes / (1024.0 * 1024.0);
    }

    double diskFreeGiB() {
      return diskFreeBytes / (1024.0 * 1024.0 * 1024.0);
    }

    boolean hasAlarm() {
      return memoryAlarm || diskAlarm;
    }

    String shortName() {
      int atIdx = name.indexOf('@');
      return atIdx >= 0 ? name.substring(atIdx + 1) : name;
    }
  }

  static class Snapshot {
    final List<NodeSnapshot> nodes;

    Snapshot(List<NodeSnapshot> nodes) {
      this.nodes = nodes;
    }

    boolean hasAlarm() {
      for (NodeSnapshot n : nodes) {
        if (n.hasAlarm()) {
          return true;
        }
      }
      return false;
    }

    String format() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < nodes.size(); i++) {
        if (i > 0) {
          sb.append(" ");
        }
        NodeSnapshot n = nodes.get(i);
        sb.append(
            String.format(
                "%s[mem=%.0f MiB disk=%.1f GiB fd=%d]",
                n.shortName(), n.memoryUsedMiB(), n.diskFreeGiB(), n.fileDescriptorsUsed));
      }
      return sb.toString();
    }
  }

  Snapshot snapshot() {
    try {
      List<NodeInfo> nodeInfos = client.getNodes();
      if (nodeInfos == null || nodeInfos.isEmpty()) {
        return new Snapshot(Collections.emptyList());
      }

      List<NodeSnapshot> nodes = new ArrayList<>(nodeInfos.size());
      for (NodeInfo node : nodeInfos) {
        nodes.add(
            new NodeSnapshot(
                node.getName(),
                node.getMemoryUsed(),
                node.getDiskFree(),
                node.getFileDescriptorsUsed(),
                node.isMemoryAlarmActive(),
                node.isDiskAlarmActive()));
      }
      return new Snapshot(nodes);
    } catch (Exception e) {
      LOG.debug("Health snapshot failed: {}", e.getMessage());
      return new Snapshot(Collections.emptyList());
    }
  }
}
