/* The writer / replica reader. Before the first fragment it ensures the anchor is
   committed, then PUTs the fragment. anchorBeforeFragment honors the load-bearing
   ordering; with it false the writer PUTs first and writes the anchor late, which
   the ordering-violation test uses to expose a live-stream reap. */
machine Writer {
  var s3: machine;
  var khepri: machine;

  start state Idle {
    entry (init: (s3: machine, khepri: machine)) {
      s3 = init.s3;
      khepri = init.khepri;
    }
    on eEnsureAnchorThenPut do (p: (from: machine, key: ObjKey, anchorBeforeFragment: bool)) {
      if (p.anchorBeforeFragment) {
        send khepri, eWriteAnchor, (from = this,);
        receive { case eKAck: { } }
        send s3, eS3Put, (from = this, key = p.key);
        receive { case eS3Ack: { } }
      } else {
        send s3, eS3Put, (from = this, key = p.key);
        receive { case eS3Ack: { } }
        send khepri, eWriteAnchor, (from = this,);
        receive { case eKAck: { } }
      }
      send p.from, eWriterDone;
    }
  }
}
