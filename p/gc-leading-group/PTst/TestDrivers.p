/* Test drivers and declarations for the GC leading-group carve-out.

   Fixture (floor = 100):
     - (50, uid 1) GROUP    : a genuinely expired orphan group, deletable
     - (60, uid 0) FRAGMENT : an orphan fragment, deletable
     - (80, uid 2) GROUP    : the leading group, straddling the floor, REFERENCED
     - (100, uid 10) FRAGMENT: a live fragment at the floor, referenced

   The gate contrasts tcLeadingGroupGuarded (the carve-out protects the leading
   group, must hold) with tcLeadingGroupUnguarded (carve-out removed: the leading
   group is deleted, must fail with the INV#2 dangling reference on (80, uid 2)). */

fun PutObj(self: machine, s3: machine, o: Obj) {
  send s3, eS3Put, (from = self, obj = o);
  receive { case eS3Ack: { } }
}

fun AddEntry(self: machine, mr: machine, k: ObjKey) {
  send mr, eMRAddEntry, (from = self, key = k);
  receive { case eMRAck: { } }
}

fun SetLeadingGroup(self: machine, mr: machine, k: ObjKey) {
  send mr, eMRSetLeadingGroup, (from = self, key = k);
  receive { case eMRAck: { } }
}

fun S3Has(self: machine, s3: machine, k: ObjKey): bool {
  var present: bool;
  send s3, eS3Has, (from = self, key = k);
  receive { case eS3HasResult: (b: bool) { present = b; } }
  return present;
}

fun RunLeadingGroup(self: machine, guard: bool) {
  var mr: machine;
  var s3: machine;
  var gc: machine;

  mr = new ManifestReplica((floor = 100,));
  s3 = new S3Store();
  gc = new GC((guardLeadingGroup = guard, manifest = mr, s3 = s3, driver = self));

  PutObj(self, s3, (offset = 50, uid = 1, kind = GROUP));
  PutObj(self, s3, (offset = 60, uid = 0, kind = FRAGMENT));
  PutObj(self, s3, (offset = 80, uid = 2, kind = GROUP));
  PutObj(self, s3, (offset = 100, uid = 10, kind = FRAGMENT));

  /* The leading group and the live fragment are referenced; the leading group
     is also recorded as the protected leading group key. */
  AddEntry(self, mr, (offset = 80, uid = 2));
  AddEntry(self, mr, (offset = 100, uid = 10));
  SetLeadingGroup(self, mr, (offset = 80, uid = 2));

  send gc, eGcSweep;
  receive { case eGcSweepDone: { } }

  /* Anti-vacuity for the guarded run: the genuine orphans are reclaimed and the
     leading group is preserved, so a carve-out that skips everything would not
     pass. */
  if (guard) {
    assert !S3Has(self, s3, (offset = 50, uid = 1)),
      "orphan group (50, 1) was not reclaimed";
    assert !S3Has(self, s3, (offset = 60, uid = 0)),
      "orphan fragment (60, 0) was not reclaimed";
    assert S3Has(self, s3, (offset = 80, uid = 2)),
      "leading group (80, 2) was wrongly deleted";
  }
}

machine DriverGuarded {
  start state Init {
    entry { RunLeadingGroup(this, true); }
  }
}

machine DriverUnguarded {
  start state Init {
    entry { RunLeadingGroup(this, false); }
  }
}

/* Current code: the carve-out protects the leading group. Must hold. */
test tcLeadingGroupGuarded [main = DriverGuarded]:
  assert NoDanglingReference in { DriverGuarded, ManifestReplica, S3Store, GC };

/* Carve-out removed: the leading group below the floor is deleted. MUST fail
   with the INV#2 dangling reference on (80, uid 2). This failing run is the gate. */
test tcLeadingGroupUnguarded [main = DriverUnguarded]:
  assert NoDanglingReference in { DriverUnguarded, ManifestReplica, S3Store, GC };
