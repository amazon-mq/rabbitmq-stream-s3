/* Test drivers and declarations for the writer/epoch fencing seam.

   Two writers contend: W2 is the new writer (epoch 2), W1 is a deposed writer
   (epoch 1) that does not yet know it lost leadership. The gate contrasts
   tcFencingGuarded (the epoch fence rejects W1's stale commit, must hold) with
   tcFencingUnguarded (fence removed: W1 overwrites W2's commit at a lower epoch,
   must fail with the split-brain epoch regression). */

fun RunFencing(self: machine, fence: bool) {
  var db: machine;
  var w1: machine;
  var w2: machine;
  var ok: bool;

  db = new KhepriDB((uid = 0, epoch = 0, revision = 1, fence = fence));
  w1 = new Writer((epoch = 1, uid = 1, db = db, driver = self));
  w2 = new Writer((epoch = 2, uid = 2, db = db, driver = self));

  /* The new writer commits first. */
  send w2, eAttemptCommit;
  receive { case eCommitDone: (o: bool) { ok = o; } }

  /* The deposed writer then reads the current revision and tries to commit. */
  send w1, eAttemptCommit;
  receive { case eCommitDone: (o: bool) { ok = o; } }
}

machine DriverGuarded {
  start state Init {
    entry { RunFencing(this, true); }
  }
}

machine DriverUnguarded {
  start state Init {
    entry { RunFencing(this, false); }
  }
}

/* Exploration: both writers contend with their reads and commits interleaved
   freely (fence on). No interleaving may regress the committed epoch. */
machine DriverExplore {
  start state Init {
    entry {
      var db: machine;
      var w1: machine;
      var w2: machine;
      var done: int;
      db = new KhepriDB((uid = 0, epoch = 0, revision = 1, fence = true));
      w1 = new Writer((epoch = 1, uid = 1, db = db, driver = this));
      w2 = new Writer((epoch = 2, uid = 2, db = db, driver = this));
      send w1, eAttemptCommit;
      send w2, eAttemptCommit;
      done = 0;
      while (done < 2) {
        receive { case eCommitDone: (o: bool) { done = done + 1; } }
      }
    }
  }
}

/* Current code: the epoch fence rejects the deposed writer. Must hold. */
test tcFencingGuarded [main = DriverGuarded]:
  assert NoEpochRegression in { DriverGuarded, KhepriDB, Writer };

/* Fence removed: the deposed writer's stale-epoch commit succeeds and overwrites
   the newer writer. MUST fail with the split-brain epoch regression. This
   failing run is the gate. */
test tcFencingUnguarded [main = DriverUnguarded]:
  assert NoEpochRegression in { DriverUnguarded, KhepriDB, Writer };

/* Fence on, both writers' reads/commits interleaved. Must hold. */
test tcFencingExplore [main = DriverExplore]:
  assert NoEpochRegression in { DriverExplore, KhepriDB, Writer };
