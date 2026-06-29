/* The GC sweep / reaper. It lists the prefix and reads the anchor, then reaps the
   whole prefix iff objects are present AND the anchor is absent (the "should be
   empty" classification). consistent selects the anchor read mode: a consistent
   read sees the committed anchor, a local read can see a stale cache.
   crashBeforeDelete drops the reap to model a crash after the decision; because
   the anchor's absence is permanent, a later sweep is still correct. */
machine GC {
  var s3: machine;
  var khepri: machine;

  start state Serving {
    entry (init: (s3: machine, khepri: machine)) {
      s3 = init.s3;
      khepri = init.khepri;
    }
    on eSweep do (p: (from: machine, consistent: bool, crashBeforeDelete: bool)) {
      var n: int;
      var anchorPresent: bool;
      send s3, eS3List, (from = this,);
      receive { case eS3ListResult: (c: int) { n = c; } }
      send khepri, eReadAnchor, (from = this, consistent = p.consistent);
      receive { case eAnchorResult: (b: bool) { anchorPresent = b; } }
      if (n > 0 && !anchorPresent && !p.crashBeforeDelete) {
        send s3, eS3DeleteAll, (from = this,);
        receive { case eS3Ack: { } }
      }
      send p.from, eSweepDone;
    }
  }
}
