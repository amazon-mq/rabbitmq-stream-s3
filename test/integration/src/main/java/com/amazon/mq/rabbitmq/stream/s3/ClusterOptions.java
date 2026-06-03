package com.amazon.mq.rabbitmq.stream.s3;

import com.rabbitmq.stream.Environment;
import java.util.List;
import picocli.CommandLine;

public class ClusterOptions {

  @CommandLine.Option(
      names = {"--uris", "-u"},
      description = "Stream protocol URIs, comma-separated",
      defaultValue = "rabbitmq-stream://localhost:5552",
      split = ",")
  List<String> uris;

  @CommandLine.Option(
      names = {"--stream", "-s"},
      description = "Stream name",
      required = true)
  String stream;

  @CommandLine.Option(
      names = {"--mgmt-uri"},
      description = "Management API base URI",
      defaultValue = "http://localhost:15672")
  String mgmtUri;

  @CommandLine.Option(
      names = {"--metrics-uris"},
      description = "Prometheus metrics endpoints, comma-separated",
      defaultValue = "http://localhost:15692",
      split = ",")
  List<String> metricsUris;

  Environment buildEnvironment() {
    return Environment.builder().uris(uris).build();
  }
}
