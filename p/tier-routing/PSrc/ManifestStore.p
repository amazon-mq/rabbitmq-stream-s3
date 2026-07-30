/* The node's manifest CACHE for one stream: a lifecycle state machine, not a
   static oracle. Cold (no row) -> Pending (member-init marker) -> Serving
   (resolved extent). The true remote extent is fixed at construction but is
   only REPORTED once Serving, mirroring the real cache: the remote tier can
   hold data the cache does not yet describe.

   Cold answers PENDING by default: a missing row must fail closed, exactly
   like an explicitly-marked-pending row, because "no row" never legitimately
   means "un-tiered stream" (tiering is unconditional and plugin-wide) or
   "brand-new stream" (the replica reader's eager empty-manifest resolve
   already seeds a RESOLVED row for that case). bugColdReportsAbsent restores
   the retired ABSENT classification, for regression coverage only; it must
   stay off in every non-regression scenario.

   mark_pending is insert-if-absent in the real code, so it never downgrades
   Serving; resolution is idempotent. */
machine ManifestStore {
  var nonEmpty: bool;
  var remoteFirst: int;
  var remoteNext: int;
  var bugColdReportsAbsent: bool;

  start state Cold {
    entry (init: (nonEmpty: bool, remoteFirst: int, remoteNext: int, bugColdReportsAbsent: bool)) {
      nonEmpty = init.nonEmpty;
      remoteFirst = init.remoteFirst;
      remoteNext = init.remoteNext;
      bugColdReportsAbsent = init.bugColdReportsAbsent;
    }
    on eGetManifest do (p: (from: machine)) {
      if (bugColdReportsAbsent) {
        /* Retired classification: a missing row reported ABSENT, and the
           reader (wrongly) took that as "un-tiered stream, local is the
           whole stream". */
        send p.from, eManifestResult,
          (reply = ABSENT, nonEmpty = false, remoteFirst = 0, remoteNext = 0);
      } else {
        /* Default: no row yet is PENDING (fail closed), never ABSENT. */
        send p.from, eManifestResult,
          (reply = PENDING, nonEmpty = false, remoteFirst = 0, remoteNext = 0);
      }
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
