/* Test drivers and declarations for the anchor-before-first-fragment design.

   The model deliberately has NO manifest: objects are bare fragments and the
   anchor is the only Khepri record, so every scenario is the never-committed case
   - which the anchor design must handle by construction.

   The validation gates are the two load-bearing failures:
     - tcAnchorStaleReadReapsLive MUST FAIL: a local (stale) anchor read reaps a
       LIVE stream whose anchor just committed
     - tcAnchorOrderingViolated MUST FAIL: a fragment that exists before the anchor
       commits is reaped while the stream is live, even with a consistent read
   The correct design (consistent reads + anchor-before-fragment) holds, including
   across a crash mid-sweep (tcAnchorReclaimsAcrossCrash, tcAnchorExplore). */

machine DriverReclaimAcrossCrash {
  start state Init {
    entry {
      var s3: machine;
      var khepri: machine;
      var writer: machine;
      var gc: machine;

      s3 = new S3Store();
      khepri = new Khepri();
      writer = new Writer((s3 = s3, khepri = khepri));
      gc = new GC((s3 = s3, khepri = khepri));

      /* Live stream: anchor committed before the first fragment, then replicated. */
      send writer, eEnsureAnchorThenPut, (from = this, key = (offset = 10, uid = 1), anchorBeforeFragment = true);
      receive { case eWriterDone: { } }
      send khepri, eReplicate, (from = this,);
      receive { case eKAck: { } }

      /* Delete the queue: keep_while removes the anchor in the same transaction. */
      send khepri, eDeleteQueue, (from = this,);
      receive { case eKAck: { } }

      /* A sweep decides to reap but crashes before deleting. */
      send gc, eSweep, (from = this, consistent = true, crashBeforeDelete = true);
      receive { case eSweepDone: { } }
      /* The anchor's absence is permanent, so the next sweep still reclaims. */
      send gc, eSweep, (from = this, consistent = true, crashBeforeDelete = false);
      receive { case eSweepDone: { } }

      announce eSweepQuiesced;
    }
  }
}

machine DriverStaleReadReapsLive {
  start state Init {
    entry {
      var s3: machine;
      var khepri: machine;
      var writer: machine;
      var gc: machine;

      s3 = new S3Store();
      khepri = new Khepri();
      writer = new Writer((s3 = s3, khepri = khepri));
      gc = new GC((s3 = s3, khepri = khepri));

      /* Anchor committed before the fragment, but NOT replicated: the local cache
         still reports the anchor absent. */
      send writer, eEnsureAnchorThenPut, (from = this, key = (offset = 10, uid = 1), anchorBeforeFragment = true);
      receive { case eWriterDone: { } }

      /* The stream is LIVE (queue not deleted). A local (stale) read sees no
         anchor and the sweep reaps the live stream's first fragment. */
      send gc, eSweep, (from = this, consistent = false, crashBeforeDelete = false);
      receive { case eSweepDone: { } }
    }
  }
}

machine DriverOrderingViolated {
  start state Init {
    entry {
      var s3: machine;
      var khepri: machine;
      var gc: machine;

      s3 = new S3Store();
      khepri = new Khepri();
      gc = new GC((s3 = s3, khepri = khepri));

      /* Ordering VIOLATED: the fragment is PUT before the anchor is written. */
      send s3, eS3Put, (from = this, key = (offset = 10, uid = 1));
      receive { case eS3Ack: { } }

      /* A sweep runs in the window before the anchor commits. Even a consistent
         read sees no anchor, so it reaps the live stream's fragment. */
      send gc, eSweep, (from = this, consistent = true, crashBeforeDelete = false);
      receive { case eSweepDone: { } }

      /* The anchor is written too late. */
      send khepri, eWriteAnchor, (from = this,);
      receive { case eKAck: { } }
    }
  }
}

machine DriverExplore {
  start state Init {
    entry {
      var s3: machine;
      var khepri: machine;
      var writer: machine;
      var gc: machine;

      s3 = new S3Store();
      khepri = new Khepri();
      writer = new Writer((s3 = s3, khepri = khepri));
      gc = new GC((s3 = s3, khepri = khepri));

      send writer, eEnsureAnchorThenPut, (from = this, key = (offset = 10, uid = 1), anchorBeforeFragment = true);
      receive { case eWriterDone: { } }

      /* A live-stream sweep (consistent) must NOT reap: the anchor is committed. */
      send gc, eSweep, (from = this, consistent = true, crashBeforeDelete = false);
      receive { case eSweepDone: { } }

      /* Replication may or may not have caught up; consistent reads ignore it. */
      if ($) {
        send khepri, eReplicate, (from = this,);
        receive { case eKAck: { } }
      }

      send khepri, eDeleteQueue, (from = this,);
      receive { case eKAck: { } }

      /* The cleanup sweep may crash before deleting; a recovery sweep follows. */
      if ($) {
        send gc, eSweep, (from = this, consistent = true, crashBeforeDelete = true);
        receive { case eSweepDone: { } }
      }
      send gc, eSweep, (from = this, consistent = true, crashBeforeDelete = false);
      receive { case eSweepDone: { } }

      announce eSweepQuiesced;
    }
  }
}

/* The correct design - consistent reads, anchor before fragment - reclaims junk
   even when a sweep crashes mid-reap. MUST HOLD. */
test tcAnchorReclaimsAcrossCrash [main = DriverReclaimAcrossCrash]:
  assert NoReapLive, EventuallyEmpty in
  { DriverReclaimAcrossCrash, S3Store, Khepri, Writer, GC };

/* GATE - a stale local anchor read reaps a LIVE stream: MUST FAIL. Proves the
   consistent-read requirement is load-bearing. */
test tcAnchorStaleReadReapsLive [main = DriverStaleReadReapsLive]:
  assert NoReapLive, EventuallyEmpty in
  { DriverStaleReadReapsLive, S3Store, Khepri, Writer, GC };

/* GATE - a fragment that exists before the anchor commits is reaped while live:
   MUST FAIL. Proves the anchor-before-fragment ordering is load-bearing, even
   with a consistent read. */
test tcAnchorOrderingViolated [main = DriverOrderingViolated]:
  assert NoReapLive, EventuallyEmpty in
  { DriverOrderingViolated, S3Store, Khepri, GC };

/* Correct design across nondeterministic replication timing and a crash mid-sweep:
   MUST HOLD. */
test tcAnchorExplore [main = DriverExplore]:
  assert NoReapLive, EventuallyEmpty in
  { DriverExplore, S3Store, Khepri, Writer, GC };
