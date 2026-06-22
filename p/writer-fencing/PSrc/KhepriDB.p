/* The Khepri metadata store with the optimistic-lock CAS from do_put/5. A put
   commits only if the expected revision matches the current one AND (when the
   fence is enabled) the new epoch is at least the stored epoch. On success the
   revision advances and the (uid, epoch) is replaced. */
machine KhepriDB {
  var uid: int;
  var epoch: int;
  var revision: int;
  var fence: bool;

  start state Serving {
    entry (init: (uid: int, epoch: int, revision: int, fence: bool)) {
      uid = init.uid;
      epoch = init.epoch;
      revision = init.revision;
      fence = init.fence;
    }
    on eGet do (p: (from: machine)) {
      send p.from, eGetResult, (uid = uid, epoch = epoch, revision = revision);
    }
    on ePut do (p: (from: machine, expectedRev: int, epoch: int, uid: int)) {
      var epochOk: bool;
      epochOk = (!fence) || (p.epoch >= epoch);
      if (p.expectedRev == revision && epochOk) {
        revision = revision + 1;
        epoch = p.epoch;
        uid = p.uid;
        announce eCommitted, epoch;
        send p.from, ePutOk, revision;
      } else {
        send p.from, ePutConflict, (uid = uid, epoch = epoch, revision = revision);
      }
    }
  }
}
