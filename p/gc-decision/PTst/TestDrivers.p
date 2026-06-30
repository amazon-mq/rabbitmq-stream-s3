/* Test drivers for the integrated GC reap-decision model.

   The holds exercise the whole decision with every guard on; each gate turns one
   guard off and must reproduce a live deletion, proving that guard load-bearing
   inside the composed model rather than in isolation:

     - tcGcBasicReclaim        - data + manifest orphans reclaimed, live preserved
     - tcGcCrossNodeStaleGuarded / ...Unguarded - GUARD A (build_lookup epoch gate)
     - tcGcNoReread            - GUARD B (still_dangling live-floor re-read)
     - tcGcNoLeadingReread     - GUARD C (still_dangling live carve-out re-derivation)

   tcGcResetAfterSnapshotStale and tcGcExplore probe an interaction no single-axis
   model covers: a reset that commits AFTER build_lookup's get_consistent. The
   epoch gate samples the committed epoch once, at snapshot; still_dangling never
   re-checks it. If a lagging node's sweep snapshots epoch N, a reset to N+1 then
   commits and re-tiers, and the cache has not applied the sync, the live re-read
   still sees the stale-high floor. These two tests are OBSERVATIONS: they assert
   the same safety property and report whether that window is reachable. */

fun dataObj(off: int, uid: int): Obj { return (kind = DATA, offset = off, uid = uid, epoch = 0); }
fun groupObj(off: int, uid: int): Obj { return (kind = GROUP, offset = off, uid = uid, epoch = 0); }
fun manifestObj(ep: int, uid: int): Obj { return (kind = MANIFEST, offset = 0, uid = uid, epoch = ep); }

fun mkView(present: bool, floor: int, epoch: int,
           leadOff: int, leadUid: int, leadPresent: bool, skip: bool): ManifestView {
  return (present = present, floor = floor, epoch = epoch,
          leadOff = leadOff, leadUid = leadUid, leadPresent = leadPresent, skipGroups = skip);
}

fun S3Put(self: machine, s3: machine, o: Obj) {
  send s3, eS3Put, (from = self, obj = o);
  receive { case eS3Ack: { } }
}

fun S3Has(self: machine, s3: machine, o: Obj): bool {
  var present: bool;
  send s3, eS3Has, (from = self, obj = o);
  receive { case eS3HasResult: (b: bool) { present = b; } }
  return present;
}

fun GcSnapshot(self: machine, gc: machine) {
  send gc, eGcSnapshot;
  receive { case eGcSnapshotDone: { } }
}
fun GcClassify(self: machine, gc: machine) {
  send gc, eGcClassify;
  receive { case eGcClassifyDone: { } }
}
fun GcExecute(self: machine, gc: machine) {
  send gc, eGcExecute;
  receive { case eGcExecuteDone: { } }
}
fun Sweep3(self: machine, gc: machine) {
  GcSnapshot(self, gc);
  GcClassify(self, gc);
  GcExecute(self, gc);
}

/* Single node, cache in sync. A data orphan below the floor and a stale-epoch
   manifest are reclaimed; the live fragment above the floor and the current
   manifest are preserved. Exercises the data and manifest reasons together. */
fun ScenarioBasic(self: machine) {
  var db: machine; var s3: machine; var cache: machine; var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  cache = new ManifestReplica((view = mkView(true, 100, 1, 0, 0, false, false),));
  gc = new GC((epochGate = true, reread = true, leadingReread = true, epochRecheck = false,
               db = db, cache = cache, s3 = s3, driver = self));

  announce eObjectReferenced, dataObj(150, 2);
  announce eObjectReferenced, manifestObj(1, 8);

  S3Put(self, s3, dataObj(50, 1));
  S3Put(self, s3, dataObj(150, 2));
  S3Put(self, s3, manifestObj(0, 9));
  S3Put(self, s3, manifestObj(1, 8));

  Sweep3(self, gc);

  assert !S3Has(self, s3, dataObj(50, 1)),
    "basic: data orphan below the floor was not reclaimed";
  assert !S3Has(self, s3, manifestObj(0, 9)),
    "basic: stale-epoch manifest was not reclaimed";
  assert S3Has(self, s3, dataObj(150, 2)),
    "basic: live fragment above the floor was deleted";
  assert S3Has(self, s3, manifestObj(1, 8)),
    "basic: current manifest was deleted";
}

/* Cross-node (../gc-reset-multinode integrated): a committed reset lowers the
   floor and re-tiers a live fragment below the lagging cache's stale-high floor.
   The reset commits BEFORE the sweep snapshots, so get_consistent returns the new
   epoch and the epoch gate can catch the lag. epochGate off + sync dropped reaps
   the live (850, 2); epochGate on fails closed; sync delivered reclaims the deep
   orphan and preserves the live re-tier. */
fun ScenarioCrossNode(self: machine, epochGate: bool, deliverSync: bool) {
  var db: machine; var s3: machine; var cache: machine; var writer: machine; var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  cache = new ManifestReplica((view = mkView(true, 1000, 1, 0, 0, false, false),));
  writer = new Writer((db = db, s3 = s3, driver = self));
  gc = new GC((epochGate = epochGate, reread = true, leadingReread = true, epochRecheck = false,
               db = db, cache = cache, s3 = s3, driver = self));

  announce eObjectReferenced, dataObj(1000, 10);
  S3Put(self, s3, dataObj(1000, 10));
  S3Put(self, s3, dataObj(850, 1));
  S3Put(self, s3, dataObj(500, 0));

  /* Reset commits (epoch 2, floor 850, re-tier (850, 2)) before the snapshot. */
  send writer, eDoReset, (from = self, newFloor = 850, newEpoch = 2, retierOffset = 850, retierUid = 2);
  receive { case eResetDone: { } }

  if (deliverSync) {
    send cache, eMRSync, (from = self, view = mkView(true, 850, 2, 0, 0, false, false));
  }

  Sweep3(self, gc);

  if (deliverSync) {
    assert !S3Has(self, s3, dataObj(500, 0)),
      "cross-node synced: genuine deep orphan (500, 0) was not reclaimed";
    assert S3Has(self, s3, dataObj(850, 2)),
      "cross-node synced: live re-tiered (850, 2) was deleted";
    assert S3Has(self, s3, dataObj(1000, 10)),
      "cross-node synced: live (1000, 10) was deleted";
  }
}

/* Single node, snapshot-then-reset (../gc-reset integrated): the sweep snapshots
   the high floor, then a reset lowers the floor and re-tiers a live fragment, and
   the cache catches up before execute. GUARD B (live-floor re-read) preserves the
   re-tier; without it the stale-high snapshot floor reaps the live (80, 3). */
fun ScenarioResetReread(self: machine, reread: bool) {
  var db: machine; var s3: machine; var cache: machine; var writer: machine; var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  cache = new ManifestReplica((view = mkView(true, 100, 1, 0, 0, false, false),));
  writer = new Writer((db = db, s3 = s3, driver = self));
  gc = new GC((epochGate = true, reread = reread, leadingReread = true, epochRecheck = false,
               db = db, cache = cache, s3 = s3, driver = self));

  announce eObjectReferenced, dataObj(150, 2);
  S3Put(self, s3, dataObj(150, 2));
  S3Put(self, s3, dataObj(40, 1));

  /* Snapshot the high floor (100), THEN the reset lands and the cache catches up. */
  GcSnapshot(self, gc);
  send writer, eDoReset, (from = self, newFloor = 60, newEpoch = 2, retierOffset = 80, retierUid = 3);
  receive { case eResetDone: { } }
  send cache, eMRSync, (from = self, view = mkView(true, 60, 2, 0, 0, false, false));

  GcClassify(self, gc);
  GcExecute(self, gc);

  if (reread) {
    assert !S3Has(self, s3, dataObj(40, 1)),
      "reset-reread: genuine orphan (40, 1) was not reclaimed";
    assert S3Has(self, s3, dataObj(80, 3)),
      "reset-reread: live re-tiered (80, 3) was deleted";
    assert S3Has(self, s3, dataObj(150, 2)),
      "reset-reread: live (150, 2) was deleted";
  }
}

/* Single node, snapshot-then-reset-plus-retention (../gc-reset-leading-group
   integrated): the snapshot carve-out protects the OLD leading group (50, 510);
   then retention advances the floor and promotes a NEW referenced leading group
   (80, 820) below the live floor. GUARD C (live carve-out re-derivation) preserves
   it; without it the stale snapshot carve-out reaps the live leading group. */
fun ScenarioLeadingGroup(self: machine, leadingReread: bool) {
  var db: machine; var s3: machine; var cache: machine; var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  cache = new ManifestReplica((view = mkView(true, 100, 1, 50, 510, true, false),));
  gc = new GC((epochGate = true, reread = true, leadingReread = leadingReread, epochRecheck = false,
               db = db, cache = cache, s3 = s3, driver = self));

  announce eObjectReferenced, groupObj(50, 510);
  announce eObjectReferenced, dataObj(150, 2);
  S3Put(self, s3, groupObj(50, 510));
  S3Put(self, s3, groupObj(80, 820));
  S3Put(self, s3, groupObj(30, 300));
  S3Put(self, s3, dataObj(150, 2));

  /* Snapshot the carve-out at (50, 510), THEN retention advances the floor and
     promotes (80, 820) to the referenced leading group; the cache catches up. */
  GcSnapshot(self, gc);
  announce eObjectUnreferenced, groupObj(50, 510);
  announce eObjectReferenced, groupObj(80, 820);
  send cache, eMRSync, (from = self, view = mkView(true, 120, 1, 80, 820, true, false));

  GcClassify(self, gc);
  GcExecute(self, gc);

  if (leadingReread) {
    assert !S3Has(self, s3, groupObj(30, 300)),
      "leading-group: deep orphan group (30, 300) was not reclaimed";
    assert S3Has(self, s3, groupObj(80, 820)),
      "leading-group: live referenced leading group (80, 820) was deleted";
    assert S3Has(self, s3, dataObj(150, 2)),
      "leading-group: live (150, 2) was deleted";
  }
}

/* A reset that commits AFTER the sweep snapshots, on a node whose cache never
   applies the sync. GUARDS A/B/C are on: get_consistent returned the old epoch,
   so the epoch gate passed; the reset then lowers the floor and re-tiers (80, 3);
   the live re-read still sees the stale-high cached floor. epochRecheck toggles
   the proposed GUARD D. Off (shipped) reaps the live re-tier; on closes it. */
fun ScenarioResetAfterSnapshotStale(self: machine, epochRecheck: bool) {
  var db: machine; var s3: machine; var cache: machine; var writer: machine; var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  cache = new ManifestReplica((view = mkView(true, 100, 1, 0, 0, false, false),));
  writer = new Writer((db = db, s3 = s3, driver = self));
  gc = new GC((epochGate = true, reread = true, leadingReread = true, epochRecheck = epochRecheck,
               db = db, cache = cache, s3 = s3, driver = self));

  announce eObjectReferenced, dataObj(150, 2);
  S3Put(self, s3, dataObj(150, 2));
  S3Put(self, s3, dataObj(40, 1));

  /* Snapshot first: get_consistent sees epoch 1, cache epoch 1, gate passes. */
  GcSnapshot(self, gc);

  /* Reset commits epoch 2, lowers the floor to 60, re-tiers a live (80, 3). The
     sync is NOT delivered: the cache keeps its stale-high floor 100 at epoch 1. */
  send writer, eDoReset, (from = self, newFloor = 60, newEpoch = 2, retierOffset = 80, retierUid = 3);
  receive { case eResetDone: { } }

  GcClassify(self, gc);
  GcExecute(self, gc);

  if (epochRecheck) {
    assert S3Has(self, s3, dataObj(80, 3)),
      "reset-after-snapshot guarded: GUARD D failed to preserve the live re-tier (80, 3)";
  }
}

/* PCT exploration. A reset may commit before or after the snapshot, and its sync
   may or may not be delivered. epochRecheck toggles the proposed GUARD D: with it
   off (shipped) the reset-after-snapshot branch reaps live; with it on no
   interleaving violates safety. */
fun ScenarioExplore(self: machine, epochRecheck: bool) {
  var db: machine; var s3: machine; var cache: machine; var writer: machine; var gc: machine;

  db = new KhepriDB((epoch = 1,));
  s3 = new S3Store();
  cache = new ManifestReplica((view = mkView(true, 100, 1, 0, 0, false, false),));
  writer = new Writer((db = db, s3 = s3, driver = self));
  gc = new GC((epochGate = true, reread = true, leadingReread = true, epochRecheck = epochRecheck,
               db = db, cache = cache, s3 = s3, driver = self));

  announce eObjectReferenced, dataObj(150, 2);
  S3Put(self, s3, dataObj(150, 2));
  S3Put(self, s3, dataObj(40, 1));

  if ($) {
    /* Reset before the snapshot: get_consistent sees the new epoch. */
    send writer, eDoReset, (from = self, newFloor = 60, newEpoch = 2, retierOffset = 80, retierUid = 3);
    receive { case eResetDone: { } }
    if ($) {
      send cache, eMRSync, (from = self, view = mkView(true, 60, 2, 0, 0, false, false));
    }
    Sweep3(self, gc);
  } else {
    /* Reset after the snapshot: get_consistent saw the old epoch. */
    GcSnapshot(self, gc);
    send writer, eDoReset, (from = self, newFloor = 60, newEpoch = 2, retierOffset = 80, retierUid = 3);
    receive { case eResetDone: { } }
    if ($) {
      send cache, eMRSync, (from = self, view = mkView(true, 60, 2, 0, 0, false, false));
    }
    GcClassify(self, gc);
    GcExecute(self, gc);
  }
}

machine DriverBasic { start state Init { entry { ScenarioBasic(this); } } }
machine DriverCrossNodeStaleUnguarded { start state Init { entry { ScenarioCrossNode(this, false, false); } } }
machine DriverCrossNodeStaleGuarded { start state Init { entry { ScenarioCrossNode(this, true, false); } } }
machine DriverCrossNodeSynced { start state Init { entry { ScenarioCrossNode(this, true, true); } } }
machine DriverResetRereadHolds { start state Init { entry { ScenarioResetReread(this, true); } } }
machine DriverNoReread { start state Init { entry { ScenarioResetReread(this, false); } } }
machine DriverLeadingGroupHolds { start state Init { entry { ScenarioLeadingGroup(this, true); } } }
machine DriverNoLeadingReread { start state Init { entry { ScenarioLeadingGroup(this, false); } } }
machine DriverResetAfterSnapshotStale { start state Init { entry { ScenarioResetAfterSnapshotStale(this, false); } } }
machine DriverResetAfterSnapshotGuarded { start state Init { entry { ScenarioResetAfterSnapshotStale(this, true); } } }
machine DriverExploreShipped { start state Init { entry { ScenarioExplore(this, false); } } }
machine DriverExploreGuarded { start state Init { entry { ScenarioExplore(this, true); } } }

/* HOLD: data + manifest reasons reclaimed, live preserved, every guard on. */
test tcGcBasicReclaim [main = DriverBasic]:
  assert NoDanglingReference in
  { DriverBasic, KhepriDB, S3Store, ManifestReplica, GC };

/* GATE (MUST fail): GUARD A off, sync dropped. The shipped live re-read re-reads
   the same stale cache, so the re-tiered live (850, 2) is deleted. */
test tcGcCrossNodeStaleUnguarded [main = DriverCrossNodeStaleUnguarded]:
  assert NoDanglingReference in
  { DriverCrossNodeStaleUnguarded, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* HOLD: GUARD A on, sync dropped. The cache epoch lags the committed epoch, so
   build_lookup fails closed and deletes nothing. */
test tcGcCrossNodeStaleGuarded [main = DriverCrossNodeStaleGuarded]:
  assert NoDanglingReference in
  { DriverCrossNodeStaleGuarded, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* HOLD + anti-vacuity: GUARD A on, sync delivered. The caught-up cache lets the
   sweep proceed, reclaim the deep orphan, and preserve the live re-tier. */
test tcGcCrossNodeSynced [main = DriverCrossNodeSynced]:
  assert NoDanglingReference in
  { DriverCrossNodeSynced, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* HOLD + anti-vacuity: GUARD B on. The live-floor re-read preserves the re-tier
   and still reclaims the genuine orphan. */
test tcGcResetRereadHolds [main = DriverResetRereadHolds]:
  assert NoDanglingReference in
  { DriverResetRereadHolds, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* GATE (MUST fail): GUARD B off. The stale-high snapshot floor reaps the live
   re-tiered (80, 3). */
test tcGcNoReread [main = DriverNoReread]:
  assert NoDanglingReference in
  { DriverNoReread, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* HOLD + anti-vacuity: GUARD C on. The live carve-out re-derivation preserves the
   newly promoted leading group and still reclaims the deep orphan group. */
test tcGcLeadingGroupHolds [main = DriverLeadingGroupHolds]:
  assert NoDanglingReference in
  { DriverLeadingGroupHolds, KhepriDB, S3Store, ManifestReplica, GC };

/* GATE (MUST fail): GUARD C off. The stale snapshot carve-out reaps the live
   referenced leading group (80, 820). */
test tcGcNoLeadingReread [main = DriverNoLeadingReread]:
  assert NoDanglingReference in
  { DriverNoLeadingReread, KhepriDB, S3Store, ManifestReplica, GC };

/* GATE for GUARD D (MUST fail): the three shipped guards are on, but a reset
   commits after the snapshot and the cache never applies the sync. The epoch gate
   was already sampled, the live re-read sees the stale-high floor, and the live
   re-tier (80, 3) is deleted. This is the gap the integrated model surfaces that
   no single-axis model covers. */
test tcGcResetAfterSnapshotStale [main = DriverResetAfterSnapshotStale]:
  assert NoDanglingReference in
  { DriverResetAfterSnapshotStale, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* HOLD + anti-vacuity: the proposed GUARD D re-validates the committed epoch at
   execute time and fails closed, preserving the live re-tier. */
test tcGcResetAfterSnapshotGuarded [main = DriverResetAfterSnapshotGuarded]:
  assert NoDanglingReference in
  { DriverResetAfterSnapshotGuarded, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* GATE (MUST fail): PCT exploration with shipped guards only reaches the
   reset-after-snapshot deletion. */
test tcGcExploreShipped [main = DriverExploreShipped]:
  assert NoDanglingReference in
  { DriverExploreShipped, KhepriDB, S3Store, ManifestReplica, Writer, GC };

/* HOLD: PCT exploration with GUARD D on. No interleaving of reset timing and sync
   delivery violates safety. */
test tcGcExploreGuarded [main = DriverExploreGuarded]:
  assert NoDanglingReference in
  { DriverExploreGuarded, KhepriDB, S3Store, ManifestReplica, Writer, GC };
