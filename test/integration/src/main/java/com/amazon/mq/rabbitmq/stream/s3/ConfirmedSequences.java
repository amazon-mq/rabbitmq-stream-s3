package com.amazon.mq.rabbitmq.stream.s3;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A thread-safe, lazily-grown bitset over message sequence numbers recording which sequences the
 * broker confirmed.
 *
 * <p>The producer assigns a sequence to every send but only some are confirmed; unconfirmed sends
 * leave holes in the sequence space. Replay verification uses this to distinguish a real gap (a
 * confirmed sequence that was never delivered) from an expected hole (an unconfirmed sequence).
 *
 * <p>Storage is paged and allocated on demand, so memory tracks the sequence range actually used
 * rather than a preallocated maximum, keeping it bounded regardless of run length.
 */
final class ConfirmedSequences {

  private static final int BITS_PER_WORD = 64;
  // 2^20 bits per page: 16384 longs == 128 KiB per page.
  private static final int PAGE_BITS = 1 << 20;
  private static final int WORDS_PER_PAGE = PAGE_BITS / BITS_PER_WORD;

  private final ConcurrentHashMap<Long, AtomicLongArray> pages = new ConcurrentHashMap<>();

  /** Marks a sequence as confirmed. Thread-safe. */
  void set(long seq) {
    if (seq < 0) {
      return;
    }
    long pageIdx = seq / PAGE_BITS;
    int bitInPage = (int) (seq % PAGE_BITS);
    int wordIdx = bitInPage / BITS_PER_WORD;
    long mask = 1L << (bitInPage % BITS_PER_WORD);
    AtomicLongArray page = pages.computeIfAbsent(pageIdx, k -> new AtomicLongArray(WORDS_PER_PAGE));
    long prev;
    long next;
    do {
      prev = page.get(wordIdx);
      next = prev | mask;
      if (prev == next) {
        return;
      }
    } while (!page.compareAndSet(wordIdx, prev, next));
  }

  /** Returns whether a sequence was confirmed. */
  boolean isSet(long seq) {
    if (seq < 0) {
      return false;
    }
    long pageIdx = seq / PAGE_BITS;
    AtomicLongArray page = pages.get(pageIdx);
    if (page == null) {
      return false;
    }
    int bitInPage = (int) (seq % PAGE_BITS);
    int wordIdx = bitInPage / BITS_PER_WORD;
    long mask = 1L << (bitInPage % BITS_PER_WORD);
    return (page.get(wordIdx) & mask) != 0;
  }

  /**
   * Counts confirmed sequences in the inclusive range [from, to]. Used to size a gap: the number of
   * confirmed sequences a consumer skipped over. Gap ranges are small, so a per-bit scan is
   * adequate.
   */
  long countInRange(long from, long to) {
    long lo = Math.max(0, from);
    long count = 0;
    for (long s = lo; s <= to; s++) {
      if (isSet(s)) {
        count++;
      }
    }
    return count;
  }
}
