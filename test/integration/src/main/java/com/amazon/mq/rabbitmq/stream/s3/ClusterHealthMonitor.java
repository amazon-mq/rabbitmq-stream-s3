package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.ClientParameters;
import com.rabbitmq.http.client.domain.NodeInfo;
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

  static class Snapshot {
    final long totalMemoryUsedBytes;
    final long totalDiskFreeBytes;
    final long totalFileDescriptorsUsed;
    final int nodeCount;
    final boolean memoryAlarm;
    final boolean diskAlarm;

    Snapshot(
        long totalMemoryUsedBytes,
        long totalDiskFreeBytes,
        long totalFileDescriptorsUsed,
        int nodeCount,
        boolean memoryAlarm,
        boolean diskAlarm) {
      this.totalMemoryUsedBytes = totalMemoryUsedBytes;
      this.totalDiskFreeBytes = totalDiskFreeBytes;
      this.totalFileDescriptorsUsed = totalFileDescriptorsUsed;
      this.nodeCount = nodeCount;
      this.memoryAlarm = memoryAlarm;
      this.diskAlarm = diskAlarm;
    }

    double totalMemoryUsedMiB() {
      return totalMemoryUsedBytes / (1024.0 * 1024.0);
    }

    double totalDiskFreeGiB() {
      return totalDiskFreeBytes / (1024.0 * 1024.0 * 1024.0);
    }

    boolean hasAlarm() {
      return memoryAlarm || diskAlarm;
    }
  }

  Snapshot snapshot() {
    try {
      List<NodeInfo> nodes = client.getNodes();
      if (nodes == null || nodes.isEmpty()) {
        return new Snapshot(0, 0, 0, 0, false, false);
      }

      long totalMem = 0;
      long totalDisk = 0;
      long totalFd = 0;
      boolean memAlarm = false;
      boolean diskAlarm = false;

      for (NodeInfo node : nodes) {
        totalMem += node.getMemoryUsed();
        totalDisk += node.getDiskFree();
        totalFd += node.getFileDescriptorsUsed();
        if (node.isMemoryAlarmActive()) {
          memAlarm = true;
        }
        if (node.isDiskAlarmActive()) {
          diskAlarm = true;
        }
      }

      return new Snapshot(totalMem, totalDisk, totalFd, nodes.size(), memAlarm, diskAlarm);
    } catch (Exception e) {
      LOG.debug("Health snapshot failed: {}", e.getMessage());
      return new Snapshot(0, 0, 0, 0, false, false);
    }
  }
}
