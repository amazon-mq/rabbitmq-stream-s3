package com.amazon.mq.rabbitmq.stream.s3;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Helpers for tracking independent per-consumer progress during a fan-out replay, where every
 * consumer reads the whole stream on its own. The slowest consumer gates completion; the sum is
 * reported for an aggregate throughput view.
 */
final class ReplayProgress {

  private ReplayProgress() {}

  /** The slowest consumer's count. Used to gate completion and pass/fail. */
  static long min(AtomicLong[] counts) {
    long min = Long.MAX_VALUE;
    for (AtomicLong c : counts) {
      min = Math.min(min, c.get());
    }
    return min;
  }

  /** The fastest consumer's count. Used to check fan-out agreement against min. */
  static long max(AtomicLong[] counts) {
    long max = 0;
    for (AtomicLong c : counts) {
      max = Math.max(max, c.get());
    }
    return max;
  }

  /** The combined count across all consumers. Used for aggregate throughput. */
  static long sum(AtomicLong[] counts) {
    long sum = 0;
    for (AtomicLong c : counts) {
      sum += c.get();
    }
    return sum;
  }

  /** A compact per-consumer breakdown, e.g. {@code c0=123 c1=456}. */
  static String format(AtomicLong[] counts) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < counts.length; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append('c').append(i).append('=').append(counts[i].get());
    }
    return sb.toString();
  }
}
