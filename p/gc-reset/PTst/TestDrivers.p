/* Test drivers and declarations for the GC x reset durability seam.

   Fixture offsets/uids (shared across scenarios):
     - floor starts at 1000; a live object (1000, uid 10) covers it
     - (850, uid 1): a pre-existing stale orphan at the eventual reset offset
     - (500, uid 0): a genuine deep orphan, below even the reset floor
     - the reset lowers the floor to 850 and re-tiers (850, uid 2) as live

   The validation gate is the contrast between tcGcResetGuarded (guard on, must
   hold) and tcGcResetUnguarded (guard off, must fail with the INV#2 dangling
   reference on (850, uid 2)). */

fun PutObj(self: machine, s3: machine, o: Obj) {
  send s3, eS3Put, (from = self, obj = o);
  receive { case eS3Ack: { } }
}

fun AddEntry(self: machine, mr: machine, k: ObjKey) {
  send mr, eMRAddEntry, (from = self, key = k);
  receive { case eMRAck: { } }
}

fun S3Has(self: machine, s3: machine, k: ObjKey): bool {
  var present: bool;
  send s3, eS3Has, (from = self, key = k);
  receive { case eS3HasResult: (b: bool) { present = b; } }
  return present;
}

/* Build the common fixture and return the machine handles via the caller's own
   variables. P has no multiple return, so this is inlined per driver below. */

/* Deterministic gate: snapshot, then reset, then list/classify/execute, each
   fully sequenced so the dangerous interleaving is forced every run. */
fun RunGcResetScenario(self: machine, guardEnabled: bool) {
  var db: machine;
  var s3: machine;
  var mr: machine;
  var writer: machine;
  var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  mr = new ManifestReplica((floor = 1000, nextOffset = 1001));

  PutObj(self, s3, (offset = 1000, uid = 10, epoch = 1));
  AddEntry(self, mr, (offset = 1000, uid = 10));
  PutObj(self, s3, (offset = 850, uid = 1, epoch = 1));
  PutObj(self, s3, (offset = 500, uid = 0, epoch = 1));

  writer = new Writer((epoch = 1, manifest = mr, s3 = s3, driver = self));
  gc = new GC((guardEnabled = guardEnabled, db = db, manifest = mr, s3 = s3, driver = self));

  /* 1. Sweep captures the snapshot floor (1000) and epoch (1). */
  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }

  /* 2. Remote-tier-ahead reset: floor -> 850, re-tier (850, uid 2) live. */
  send writer, eDoReset, (localFloor = 850, newUid = 2);
  receive { case eResetDone: { } }

  /* 3. Sweep lists + classifies against the stale snapshot floor (1000). */
  send gc, eGcListClassify;
  receive { case eGcClassifyDone: { } }

  /* 4. Sweep executes deletes (still_dangling toggled by guardEnabled). */
  send gc, eGcExecute;
  receive { case eGcExecuteDone: { } }

  /* Anti-vacuity for the guarded run: the guard must reclaim the genuine deep
     orphan (500, 0) AND preserve the live re-tiered object (850, 2). Without
     this, a guard that simply skips everything would also pass. */
  if (guardEnabled) {
    assert !S3Has(self, s3, (offset = 500, uid = 0)),
      "guard over-suppressed: genuine deep orphan (500, 0) was not reclaimed";
    assert S3Has(self, s3, (offset = 850, uid = 2)),
      "guard failed to preserve the live re-tiered object (850, 2)";
  }
}

/* Exploration: snapshot first, then race the reset against list/classify/execute
   and let the checker interleave them. Guard on; no schedule may violate safety. */
fun RunGcResetExplore(self: machine) {
  var db: machine;
  var s3: machine;
  var mr: machine;
  var writer: machine;
  var gc: machine;
  var done: int;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  mr = new ManifestReplica((floor = 1000, nextOffset = 1001));

  PutObj(self, s3, (offset = 1000, uid = 10, epoch = 1));
  AddEntry(self, mr, (offset = 1000, uid = 10));
  PutObj(self, s3, (offset = 850, uid = 1, epoch = 1));
  PutObj(self, s3, (offset = 500, uid = 0, epoch = 1));

  writer = new Writer((epoch = 1, manifest = mr, s3 = s3, driver = self));
  gc = new GC((guardEnabled = true, db = db, manifest = mr, s3 = s3, driver = self));

  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }

  /* Fire the reset and the rest of the sweep without sequencing: the scheduler
     interleaves the reset's manifest/S3 effects with the LIST and the guard's
     live-floor re-read. */
  send writer, eDoReset, (localFloor = 850, newUid = 2);
  send gc, eGcListClassify;
  send gc, eGcExecute;

  done = 0;
  while (done < 3) {
    receive {
      case eResetDone: { done = done + 1; }
      case eGcClassifyDone: { done = done + 1; }
      case eGcExecuteDone: { done = done + 1; }
    }
  }
}

/* Epoch-axis safety: a stale-epoch object above the live floor is deleted
   unconditionally (no guard on that axis). Because epoch is monotonic the
   object is genuinely dead, so no safety monitor may fire. This is the
   asymmetry that proves the guard is only needed on the offset axis. */
fun RunEpochAxisScenario(self: machine) {
  var db: machine;
  var s3: machine;
  var mr: machine;
  var gc: machine;

  db = new KhepriDB((epoch = 2,));
  s3 = new S3Store();
  mr = new ManifestReplica((floor = 500, nextOffset = 1001));

  PutObj(self, s3, (offset = 1000, uid = 10, epoch = 2));
  AddEntry(self, mr, (offset = 1000, uid = 10));
  /* Stale-epoch orphan above the floor: not below_first_offset, but epoch 1 < 2. */
  PutObj(self, s3, (offset = 800, uid = 8, epoch = 1));

  gc = new GC((guardEnabled = true, db = db, manifest = mr, s3 = s3, driver = self));

  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }
  send gc, eGcListClassify;
  receive { case eGcClassifyDone: { } }
  send gc, eGcExecute;
  receive { case eGcExecuteDone: { } }
}

machine DriverGuarded {
  start state Init {
    entry { RunGcResetScenario(this, true); }
  }
}

machine DriverUnguarded {
  start state Init {
    entry { RunGcResetScenario(this, false); }
  }
}

machine DriverExplore {
  start state Init {
    entry { RunGcResetExplore(this); }
  }
}

machine DriverEpochAxis {
  start state Init {
    entry { RunEpochAxisScenario(this); }
  }
}

/* Guard on: the seam is safe. Must hold. */
test tcGcResetGuarded [main = DriverGuarded]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverGuarded, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* Guard off (still_dangling collapsed on the offset axis): MUST fail with the
   INV#2 dangling reference on the re-tiered (850, uid 2). This failing run is
   the validation gate. */
test tcGcResetUnguarded [main = DriverUnguarded]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverUnguarded, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* Guard on, all interleavings of reset vs sweep explored. Must hold. */
test tcGcResetExplore [main = DriverExplore]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverExplore, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* Epoch-axis deletions are safe without a re-check. Must hold. */
test tcEpochAxisSafe [main = DriverEpochAxis]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverEpochAxis, KhepriDB, S3Store, ManifestReplica, GC };
