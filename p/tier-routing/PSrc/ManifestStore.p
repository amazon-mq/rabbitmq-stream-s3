/* The node's manifest CACHE for one stream: a lifecycle state machine, not a
   static oracle. Cold (no row) -> Pending (member-init marker) -> Serving
   (resolved extent). The true remote extent is fixed at construction but is
   only REPORTED once Serving, mirroring the real cache: the remote tier can
   hold data the cache does not yet describe.

   mark_pending is insert-if-absent in the real code, so it never downgrades
   Serving; resolution is idempotent. */
machine ManifestStore {
  var nonEmpty: bool;
  var remoteFirst: int;
  var remoteNext: int;

  start state Cold {
    entry (init: (nonEmpty: bool, remoteFirst: int, remoteNext: int)) {
      nonEmpty = init.nonEmpty;
      remoteFirst = init.remoteFirst;
      remoteNext = init.remoteNext;
    }
    on eGetManifest do (p: (from: machine)) {
      send p.from, eManifestResult,
        (reply = ABSENT, nonEmpty = false, remoteFirst = 0, remoteNext = 0);
    }
    on eMarkPending do (p: (from: machine)) {
      send p.from, eMSAck;
      goto Pending;
    }
    /* A persist can seed the row without a marker (put_manifest). */
    on eResolveManifest do (p: (from: machine)) {
      send p.from, eMSAck;
      goto Serving;
    }
  }

  state Pending {
    on eGetManifest do (p: (from: machine)) {
      send p.from, eManifestResult,
        (reply = PENDING, nonEmpty = false, remoteFirst = 0, remoteNext = 0);
    }
    on eMarkPending do (p: (from: machine)) {
      /* Idempotent: a re-registration re-marks an already pending row. */
      send p.from, eMSAck;
    }
    on eResolveManifest do (p: (from: machine)) {
      send p.from, eMSAck;
      goto Serving;
    }
  }

  state Serving {
    on eGetManifest do (p: (from: machine)) {
      send p.from, eManifestResult,
        (reply = RESOLVED, nonEmpty = nonEmpty, remoteFirst = remoteFirst, remoteNext = remoteNext);
    }
    on eMarkPending do (p: (from: machine)) {
      /* Insert-if-absent: never downgrade a resolved row. */
      send p.from, eMSAck;
    }
    on eResolveManifest do (p: (from: machine)) {
      send p.from, eMSAck;
    }
  }
}
