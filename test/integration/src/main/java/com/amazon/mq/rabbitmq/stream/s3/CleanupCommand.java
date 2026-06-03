package com.amazon.mq.rabbitmq.stream.s3;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "cleanup",
    description =
        "Delete all streams and close all connections. "
            + "Run before a test to ensure a clean environment.")
public class CleanupCommand implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(CleanupCommand.class);

  @CommandLine.Option(
      names = {"--mgmt-uri"},
      description = "Management API base URI",
      defaultValue = "http://localhost:15672")
  private String mgmtUri;

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
}
