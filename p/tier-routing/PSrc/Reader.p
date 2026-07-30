/* The reader routing an integer offset to a tier (resolve_remote_location/2).

   Two injectable bugs live here, each a shipped defect this model gates on:
   - bugNoMinusOneGuard drops the `first_chunk_id =/= -1` guard from the
     local-tier check, reproducing the empty-local-log silent remote skip.
   - bugMissFallsLocal collapses a PENDING cache reply into the local fallback
     ({local, first}), reproducing the boot-window silent remote skip that
     shipped when the cache had no pending state and a miss meant "no remote
     tier".

   A third, retired bug -- classifying a missing cache row as ABSENT instead
   of defaulting it to PENDING -- lives on ManifestStore's
   bugColdReportsAbsent, not here: the defect is in what the cache reports
   for a missing row, not in how the Reader routes a given reply. The
   Reader's ABSENT branch below is unconditional LOCAL and unchanged; it is
   only ever reached when that toggle is on.

   The Reader never sees the ground-truth extent: it decides from the cache
   reply alone, exactly like the real code. The monitor judges the decision
   against the truth the driver announced. */
machine Reader {
  var bugNoMinusOneGuard: bool;
  var bugMissFallsLocal: bool;
  var manifest: machine;
  var driver: machine;

  start state Idle {
    entry (init: (bugNoMinusOneGuard: bool, bugMissFallsLocal: bool, manifest: machine, driver: machine)) {
      bugNoMinusOneGuard = init.bugNoMinusOneGuard;
      bugMissFallsLocal = init.bugMissFallsLocal;
      manifest = init.manifest;
      driver = init.driver;
    }
    on eResolve do (p: (from: machine, firstChunkId: int, offset: int)) {
      var m: (reply: CacheReply, nonEmpty: bool, remoteFirst: int, remoteNext: int);
      var outcome: Outcome;
      var localCheck: bool;
      send manifest, eGetManifest, (from = this,);
      receive {
        case eManifestResult: (r: (reply: CacheReply, nonEmpty: bool, remoteFirst: int, remoteNext: int)) { m = r; }
      }
      if (bugNoMinusOneGuard) {
        localCheck = p.offset >= p.firstChunkId;
      } else {
        localCheck = (p.firstChunkId != -1) && (p.offset >= p.firstChunkId);
      }
      if (localCheck) {
        outcome = LOCAL;
      } else if (m.reply == RESOLVED) {
        if (!m.nonEmpty) {
          /* Resolved and empty: no remote tier, attach locally. */
          outcome = LOCAL;
        } else if (p.firstChunkId == -1 && p.offset >= m.remoteNext) {
          /* Local empty and the offset is beyond the remote tail: local tail
             wait. */
          outcome = LOCAL;
        } else {
          outcome = REMOTE;
        }
      } else if (bugMissFallsLocal) {
        /* Pre-fix behavior: any miss is treated as "no remote tier". */
        outcome = LOCAL;
      } else if (m.reply == PENDING) {
        /* Attached but unresolved: fail closed, the consumer retries. */
        outcome = RETRY;
      } else {
        /* ABSENT only occurs when ManifestStore's bugColdReportsAbsent
           regression toggle is on; the default cold-cache classification is
           PENDING (handled above). This branch stays LOCAL on purpose: it
           reproduces the retired "missing row = un-tiered stream"
           classification bug for tcTierRoutingBuggyColdAbsent. */
        outcome = LOCAL;
      }
      announce eResolution, (outcome = outcome, offset = p.offset, firstChunkId = p.firstChunkId);
      send p.from, eResolveDone, outcome;
    }
  }
}
