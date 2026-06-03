package com.amazon.mq.rabbitmq.stream.s3;

import picocli.CommandLine;

@CommandLine.Command(
    name = "stream-s3-test",
    description = "Integration test harness for rabbitmq_stream_s3",
    subcommands = {
      RemoteOnlyReadTest.class,
      MultiOffsetConsumerTest.class,
      CommandLine.HelpCommand.class
    })
public class Main implements Runnable {

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Main()).execute(args);
    System.exit(exitCode);
  }
}
