/* Khepri: the committed truth plus a lagging local replica cache.

   committedAnchor / committedQueue are the durable, quorum-agreed state. A write
   updates the committed state immediately but does NOT update the local cache;
   eReplicate models the replica catching up. A consistent read returns the
   committed anchor (what a quorum read sees); a local read returns the cached
   anchor, which can lag.

   The anchor is keep_while'd to the queue: deleting the queue removes the anchor
   in the SAME committed transaction, which is why "anchor absent" is produced
   atomically with deletion and is permanent. */
machine Khepri {
  var committedQueue: bool;
  var committedAnchor: bool;
  var cachedAnchor: bool;

  start state Serving {
    entry {
      committedQueue = true;
      committedAnchor = false;
      cachedAnchor = false;
    }

    on eWriteAnchor do (p: (from: machine)) {
      /* The anchor exists only while the queue does (keep_while). The local cache
         is intentionally NOT updated here, so a local read lags until eReplicate. */
      if (committedQueue) {
        committedAnchor = true;
      }
      send p.from, eKAck;
    }

    on eReplicate do (p: (from: machine)) {
      cachedAnchor = committedAnchor;
      send p.from, eKAck;
    }

    on eDeleteQueue do (p: (from: machine)) {
      /* keep_while removes the anchor in the same transaction as the queue. */
      committedQueue = false;
      committedAnchor = false;
      announce eQueueDeleted;
      send p.from, eKAck;
    }

    on eReadAnchor do (p: (from: machine, consistent: bool)) {
      var present: bool;
      if (p.consistent) {
        present = committedAnchor;
      } else {
        present = cachedAnchor;
      }
      send p.from, eAnchorResult, present;
    }
  }
}
