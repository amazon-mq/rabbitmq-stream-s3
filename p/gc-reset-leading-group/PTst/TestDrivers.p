/* Test drivers and declarations for the GC x reset x leading-group seam.

   Fixture (shared across scenarios):
     - floor starts at 1000; a live fragment (1000, uid 10) covers it
     - the snapshot leading group is (900, uid 1): a GROUP straddling the floor
       (partial expiry), referenced - the carve-out target at snapshot time
     - (500, uid 0): a genuine deep orphan GROUP, not referenced
     - the reset lowers the floor to 850 and installs a fresh leading GROUP
       (850, uid 2); retention then advances the floor to 870, so (850, uid 2) is
       a leading group BELOW the live floor while still referenced

   The validation gate is the contrast between tcLeadingGroupResetBug (the shipped
   offset-only re-check, which MUST fail with INV#2 on the live leading group
   (850, uid 2)) and tcLeadingGroupResetFixed (still_dangling re-validates the
   carve-out against the live manifest, which MUST hold). */

fun PutObj(self: machine, s3: machine, o: Obj) {
  send s3, eS3Put, (from = self, obj = o);
  receive { case eS3Ack: { } }
}

fun AddEntry(self: machine, mr: machine, k: ObjKey) {
  send mr, eMRAddEntry, (from = self, key = k);
  receive { case eMRAck: { } }
}

fun RemoveEntry(self: machine, mr: machine, k: ObjKey) {
  send mr, eMRRemoveEntry, (from = self, key = k);
  receive { case eMRAck: { } }
}

fun S3Has(self: machine, s3: machine, k: ObjKey): bool {
  var present: bool;
  send s3, eS3Has, (from = self, key = k);
  receive { case eS3HasResult: (b: bool) { present = b; } }
  return present;
}

/* Build the common fixture: durable consensus, S3, and a manifest whose leading
   group is (900, 1). Returns nothing; the caller threads the handles. */

/* Deterministic gate: snapshot, then reset + forward retention, then
   list/classify/execute, each fully sequenced so the dangerous interleaving is
   forced every run. */
fun RunResetScenario(self: machine, recheckCarveOut: bool) {
  var db: machine;
  var s3: machine;
  var mr: machine;
  var writer: machine;
  var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  mr = new ManifestReplica((floor = 1000, nextOffset = 2000,
        leadingKey = (offset = 900, uid = 1), hasLeading = true, skipGroups = false));

  PutObj(self, s3, (offset = 1000, uid = 10, kind = FRAGMENT, epoch = 1));
  AddEntry(self, mr, (offset = 1000, uid = 10));
  PutObj(self, s3, (offset = 900, uid = 1, kind = GROUP, epoch = 1));
  AddEntry(self, mr, (offset = 900, uid = 1));
  PutObj(self, s3, (offset = 500, uid = 0, kind = GROUP, epoch = 1));

  writer = new Writer((epoch = 1, manifest = mr, s3 = s3));
  gc = new GC((recheckCarveOut = recheckCarveOut, db = db, manifest = mr, s3 = s3, driver = self));

  /* 1. Sweep captures the snapshot floor (1000) and leading group (900, 1). */
  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }

  /* 2. Reset lowers floor to 850 and installs the fresh leading group (850, 2). */
  send writer, eDoReset, (from = self, newFloor = 850, newUid = 2, oldLeading = (offset = 900, uid = 1));
  receive { case eResetDone: { } }

  /* 3. Retention advances the floor to 870; (850, 2) now straddles it. */
  send writer, eAdvanceFloor, (from = self, newFloor = 870);
  receive { case eAdvanceDone: { } }

  /* 4. Sweep lists + classifies against the stale snapshot (floor 1000, leading
     group (900, 1)). The live leading group (850, 2) is not the snapshot one. */
  send gc, eGcListClassify;
  receive { case eGcClassifyDone: { } }

  /* 5. Sweep executes deletes. still_dangling re-reads the live floor (870). */
  send gc, eGcExecute;
  receive { case eGcExecuteDone: { } }

  /* Anti-vacuity for the fixed run: the genuine deep orphan (500, 0) must be
     reclaimed AND the live leading group (850, 2) preserved. Without this a fix
     that simply skips every group would also pass. */
  if (recheckCarveOut) {
    assert !S3Has(self, s3, (offset = 500, uid = 0)),
      "fix over-suppressed: genuine deep orphan group (500, 0) was not reclaimed";
    assert S3Has(self, s3, (offset = 850, uid = 2)),
      "fix failed to preserve the live leading group (850, 2)";
  }
}

/* Exploration: snapshot first, then race the reset + retention against the
   list/classify/execute and let the checker interleave them. Fix on; no schedule
   may violate safety. */
fun RunResetExplore(self: machine) {
  var db: machine;
  var s3: machine;
  var mr: machine;
  var writer: machine;
  var gc: machine;
  var done: int;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  mr = new ManifestReplica((floor = 1000, nextOffset = 2000,
        leadingKey = (offset = 900, uid = 1), hasLeading = true, skipGroups = false));

  PutObj(self, s3, (offset = 1000, uid = 10, kind = FRAGMENT, epoch = 1));
  AddEntry(self, mr, (offset = 1000, uid = 10));
  PutObj(self, s3, (offset = 900, uid = 1, kind = GROUP, epoch = 1));
  AddEntry(self, mr, (offset = 900, uid = 1));
  PutObj(self, s3, (offset = 500, uid = 0, kind = GROUP, epoch = 1));

  writer = new Writer((epoch = 1, manifest = mr, s3 = s3));
  gc = new GC((recheckCarveOut = true, db = db, manifest = mr, s3 = s3, driver = self));

  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }

  /* FIFO per machine keeps the writer's reset before its retention; the scheduler
     interleaves those with the sweep's LIST and the guard's live-floor re-read. */
  send writer, eDoReset, (from = self, newFloor = 850, newUid = 2, oldLeading = (offset = 900, uid = 1));
  send writer, eAdvanceFloor, (from = self, newFloor = 870);
  send gc, eGcListClassify;
  send gc, eGcExecute;

  done = 0;
  while (done < 4) {
    receive {
      case eResetDone: { done = done + 1; }
      case eAdvanceDone: { done = done + 1; }
      case eGcClassifyDone: { done = done + 1; }
      case eGcExecuteDone: { done = done + 1; }
    }
  }
}

/* Control: pure FORWARD retention, no reset. The floor only advances (1000 ->
   1100); the old leading group (900, 1) fully expires (unreferenced) and a new
   leading group forms at (1050, 3), ABOVE the snapshot floor, so it is never
   classified. The shipped offset-only re-check (recheckCarveOut = false) is safe
   here: this is the asymmetry that proves the DOWNWARD reset is the necessary
   ingredient of the seam. Must hold. */
fun RunRetentionOnly(self: machine) {
  var db: machine;
  var s3: machine;
  var mr: machine;
  var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  mr = new ManifestReplica((floor = 1000, nextOffset = 2000,
        leadingKey = (offset = 900, uid = 1), hasLeading = true, skipGroups = false));

  PutObj(self, s3, (offset = 1000, uid = 10, kind = FRAGMENT, epoch = 1));
  AddEntry(self, mr, (offset = 1000, uid = 10));
  PutObj(self, s3, (offset = 900, uid = 1, kind = GROUP, epoch = 1));
  AddEntry(self, mr, (offset = 900, uid = 1));
  PutObj(self, s3, (offset = 500, uid = 0, kind = GROUP, epoch = 1));

  gc = new GC((recheckCarveOut = false, db = db, manifest = mr, s3 = s3, driver = self));

  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }

  /* Forward retention: floor 1000 -> 1100, old leading group expires, a new
     leading group forms above the snapshot floor. */
  send mr, eMRSetFloor, (from = self, floor = 1100, isReset = false);
  receive { case eMRAck: { } }
  RemoveEntry(self, mr, (offset = 900, uid = 1));
  PutObj(self, s3, (offset = 1050, uid = 3, kind = GROUP, epoch = 1));
  AddEntry(self, mr, (offset = 1050, uid = 3));
  send mr, eMRSetLeading, (from = self, leadingKey = (offset = 1050, uid = 3), hasLeading = true, skipGroups = false);
  receive { case eMRAck: { } }

  send gc, eGcListClassify;
  receive { case eGcClassifyDone: { } }
  send gc, eGcExecute;
  receive { case eGcExecuteDone: { } }

  /* The genuine orphan (500, 0) is reclaimed; the live fragment and the new
     leading group survive. */
  assert !S3Has(self, s3, (offset = 500, uid = 0)),
    "retention control: genuine orphan group (500, 0) was not reclaimed";
  assert S3Has(self, s3, (offset = 1050, uid = 3)),
    "retention control: live leading group (1050, 3) was deleted";
}

machine DriverResetBug {
  start state Init {
    entry { RunResetScenario(this, false); }
  }
}

machine DriverResetFixed {
  start state Init {
    entry { RunResetScenario(this, true); }
  }
}

machine DriverResetExplore {
  start state Init {
    entry { RunResetExplore(this); }
  }
}

machine DriverRetentionOnly {
  start state Init {
    entry { RunRetentionOnly(this); }
  }
}

/* Shipped offset-only re-check: MUST fail with INV#2 on the live leading group
   (850, uid 2). This failing run is the validation gate - the proof the seam is
   real and the fix is meaningful. */
test tcLeadingGroupResetBug [main = DriverResetBug]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverResetBug, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* still_dangling re-validates the carve-out against the live manifest: MUST hold,
   and the anti-vacuity asserts the orphan is still reclaimed. */
test tcLeadingGroupResetFixed [main = DriverResetFixed]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverResetFixed, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* Fix on, reset + retention raced against the sweep across all interleavings.
   Must hold. */
test tcLeadingGroupResetExplore [main = DriverResetExplore]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverResetExplore, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* Forward-retention-only control (shipped offset-only re-check): must hold.
   Proves the downward reset, not retention, is what defeats the offset-only
   re-check. */
test tcLeadingGroupRetentionOnly [main = DriverRetentionOnly]:
  assert NoDanglingReference, NoLostAckedData, MonotonicFrontier in
  { DriverRetentionOnly, KhepriDB, S3Store, ManifestReplica, GC };
