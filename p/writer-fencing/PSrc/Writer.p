/* A writer attempting to commit a manifest root: it reads the current revision,
   then CASes its own (epoch, uid) against that revision. A deposed writer keeps
   its old (lower) epoch. */
machine Writer {
  var myEpoch: int;
  var myUid: int;
  var db: machine;
  var driver: machine;

  start state Idle {
    entry (init: (epoch: int, uid: int, db: machine, driver: machine)) {
      myEpoch = init.epoch;
      myUid = init.uid;
      db = init.db;
      driver = init.driver;
    }
    on eAttemptCommit do {
      var rev: int;
      var ok: bool;
      send db, eGet, (from = this,);
      receive { case eGetResult: (r: (uid: int, epoch: int, revision: int)) { rev = r.revision; } }
      send db, ePut, (from = this, expectedRev = rev, epoch = myEpoch, uid = myUid);
      receive {
        case ePutOk: (nr: int) { ok = true; }
        case ePutConflict: (c: (uid: int, epoch: int, revision: int)) { ok = false; }
      }
      send driver, eCommitDone, ok;
    }
  }
}
