/* Test drivers for the multi-node GC x reset durability seam.

   Fixture (parallels ../gc-reset so the contrast is clear):
     - committed epoch starts at 1; the lagging replica cache starts at
       (floor = 1000, epoch = 1)
     - S3 holds a live (1000, 10), a pre-existing stale orphan (850, 1), and a
       genuine deep orphan (500, 0)
     - a COMMITTED remote-tier-ahead reset bumps the epoch to 2, lowers the floor
       to 850, and re-tiers a live (850, 2)

   The sweep targets the lagging replica. Whether that cache learns of the reset
   (eMRSync delivered) is the variable.

   Validation gate: tcMultiNodeStaleUnguarded (epoch guard off, sync dropped)
   MUST fail with the INV#2 dangling reference on (850, 2) - and crucially the
   shipped still_dangling/1 guard is ON, proving it is insufficient cross-node.
   tcMultiNodeStaleGuarded (epoch guard on) MUST hold by failing closed. */

fun S3Put(self: machine, s3: machine, k: ObjKey) {
  send s3, eS3Put, (from = self, key = k);
  receive { case eS3Ack: { } }
}

fun S3Has(self: machine, s3: machine, k: ObjKey): bool {
  var present: bool;
  send s3, eS3Has, (from = self, key = k);
  receive { case eS3HasResult: (b: bool) { present = b; } }
  return present;
}

/* Build the common fixture: returns the handles via the caller's variables by
   convention (P has no multiple return, so this is inlined per driver). */

/* Deterministic gate: the reset commits, its sync to the target is DROPPED, then
   the sweep runs fully sequenced so the dangerous stale-cache read is forced
   every run. epochGuardEnabled toggles the fix; stillDanglingEnabled stays true. */
fun RunStaleScenario(self: machine, epochGuardEnabled: bool) {
  var db: machine;
  var s3: machine;
  var target: machine;
  var writer: machine;
  var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  target = new ManifestReplica((floor = 1000, epoch = 1));

  /* Authoritative initial state announced to the monitors. */
  announce eFloorChanged, (newFloor = 1000, isReset = false);
  announce eObjectReferenced, (offset = 1000, uid = 10);

  S3Put(self, s3, (offset = 1000, uid = 10));
  S3Put(self, s3, (offset = 850, uid = 1));
  S3Put(self, s3, (offset = 500, uid = 0));

  writer = new Writer((db = db, s3 = s3, driver = self));
  gc = new GC((stillDanglingEnabled = true, epochGuardEnabled = epochGuardEnabled,
               db = db, target = target, s3 = s3, driver = self));

  /* Reset commits (epoch 2, floor 850, re-tier (850, 2)). Sync is NOT delivered:
     the target cache keeps its stale-high floor 1000 at epoch 1. */
  send writer, eDoReset, (from = self, newFloor = 850, newUid = 2, newEpoch = 2);
  receive { case eResetDone: { } }

  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }
  send gc, eGcListClassify;
  receive { case eGcClassifyDone: { } }
  send gc, eGcExecute;
  receive { case eGcExecuteDone: { } }
}

/* Anti-vacuity: the reset commits AND its sync is delivered, so the cache is now
   at the committed epoch. The guarded sweep must proceed (not skip), preserve the
   live re-tiered (850, 2), and still reclaim the genuine deep orphan (500, 0). */
fun RunSyncedScenario(self: machine) {
  var db: machine;
  var s3: machine;
  var target: machine;
  var writer: machine;
  var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  target = new ManifestReplica((floor = 1000, epoch = 1));

  announce eFloorChanged, (newFloor = 1000, isReset = false);
  announce eObjectReferenced, (offset = 1000, uid = 10);

  S3Put(self, s3, (offset = 1000, uid = 10));
  S3Put(self, s3, (offset = 850, uid = 1));
  S3Put(self, s3, (offset = 500, uid = 0));

  writer = new Writer((db = db, s3 = s3, driver = self));
  gc = new GC((stillDanglingEnabled = true, epochGuardEnabled = true,
               db = db, target = target, s3 = s3, driver = self));

  send writer, eDoReset, (from = self, newFloor = 850, newUid = 2, newEpoch = 2);
  receive { case eResetDone: { } }

  /* Deliver the reset's sync to the cache before the sweep reads it. */
  send target, eMRSync, (from = self, floor = 850, epoch = 2);

  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }
  send gc, eGcListClassify;
  receive { case eGcClassifyDone: { } }
  send gc, eGcExecute;
  receive { case eGcExecuteDone: { } }

  assert !S3Has(self, s3, (offset = 500, uid = 0)),
    "guard over-suppressed: genuine deep orphan (500, 0) was not reclaimed after the cache caught up";
  assert S3Has(self, s3, (offset = 850, uid = 2)),
    "guard failed to preserve the live re-tiered object (850, 2)";
}

/* Exploration: the reset commits, then the cache sync is RACED against the sweep
   (fire-and-forget, not sequenced). Guard on. No interleaving may violate safety:
   either the snapshot sees the stale epoch and skips, or it sees the caught-up
   epoch and sweeps against the correct floor. */
fun RunExploreScenario(self: machine) {
  var db: machine;
  var s3: machine;
  var target: machine;
  var writer: machine;
  var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  target = new ManifestReplica((floor = 1000, epoch = 1));

  announce eFloorChanged, (newFloor = 1000, isReset = false);
  announce eObjectReferenced, (offset = 1000, uid = 10);

  S3Put(self, s3, (offset = 1000, uid = 10));
  S3Put(self, s3, (offset = 850, uid = 1));
  S3Put(self, s3, (offset = 500, uid = 0));

  writer = new Writer((db = db, s3 = s3, driver = self));
  gc = new GC((stillDanglingEnabled = true, epochGuardEnabled = true,
               db = db, target = target, s3 = s3, driver = self));

  send writer, eDoReset, (from = self, newFloor = 850, newUid = 2, newEpoch = 2);
  receive { case eResetDone: { } }

  /* Fire the sync without waiting: the scheduler may deliver it before, during,
     or after the sweep's reads. */
  send target, eMRSync, (from = self, floor = 850, epoch = 2);

  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }
  send gc, eGcListClassify;
  receive { case eGcClassifyDone: { } }
  send gc, eGcExecute;
  receive { case eGcExecuteDone: { } }
}

machine DriverStaleUnguarded {
  start state Init {
    entry { RunStaleScenario(this, false); }
  }
}

machine DriverStaleGuarded {
  start state Init {
    entry { RunStaleScenario(this, true); }
  }
}

machine DriverSynced {
  start state Init {
    entry { RunSyncedScenario(this); }
  }
}

machine DriverExplore {
  start state Init {
    entry { RunExploreScenario(this); }
  }
}

/* GATE (MUST fail): epoch guard off, reset sync dropped. The shipped
   still_dangling/1 guard is ON but re-reads the same stale cache, so the
   re-tiered live (850, 2) is deleted - INV#2. This failing run proves the model
   reproduces the cross-node bug, so the guarded result is meaningful. */
test tcMultiNodeStaleUnguarded [main = DriverStaleUnguarded]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverStaleUnguarded, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* The fix (MUST hold): epoch guard on, sync dropped. The cache epoch is behind
   the committed epoch, so the sweep fails closed and deletes nothing. */
test tcMultiNodeStaleGuarded [main = DriverStaleGuarded]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverStaleGuarded, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* Anti-vacuity (MUST hold): epoch guard on, sync delivered. The caught-up cache
   lets the sweep proceed, preserve the live (850, 2), and reclaim the deep
   orphan (500, 0). Proves the guard does not simply suppress all sweeps. */
test tcMultiNodeSyncedGuarded [main = DriverSynced]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverSynced, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* Exploration (MUST hold): epoch guard on, sync raced against the sweep. */
test tcMultiNodeExplore [main = DriverExplore]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverExplore, KhepriDB, S3Store, ManifestReplica, Writer, GC };
