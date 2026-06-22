/* Test drivers and declarations for the orphan-leak seam.

   The gate contrasts tcOrphanGuarded (GC re-sweeps until clean: a transiently
   failed delete is reclaimed on a later pass, liveness holds) with
   tcOrphanBuggy (a single pass: the failed delete leaks and the Dirty hot state
   never clears). */

fun RunOrphanScenario(self: machine, reSweep: bool, faults: int, numOrphans: int) {
  var s3: machine;
  var gc: machine;
  var i: int;
  s3 = new S3Store((faultsRemaining = faults,));
  gc = new GC((reSweep = reSweep, s3 = s3, driver = self));
  i = 0;
  while (i < numOrphans) {
    send s3, eAddOrphan, (from = self, id = i + 1);
    receive { case eAddAck: { } }
    i = i + 1;
  }
  send gc, eSweep;
  receive { case eSweepFinished: { } }
}

machine DriverGuarded {
  start state Init {
    entry { RunOrphanScenario(this, true, 1, 1); }
  }
}

machine DriverBuggy {
  start state Init {
    entry { RunOrphanScenario(this, false, 1, 1); }
  }
}

/* Guard on, more orphans and a larger transient-fault budget: every orphan is
   still reclaimed across the re-sweeps. */
machine DriverExplore {
  start state Init {
    entry { RunOrphanScenario(this, true, 2, 2); }
  }
}

/* Current code: GC re-sweeps, so a transiently failed delete is reclaimed. Holds. */
test tcOrphanGuarded [main = DriverGuarded]:
  assert OrphanEventuallyReclaimed in { DriverGuarded, S3Store, GC };

/* Bug (no re-sweep): a single pass leaves a transiently failed delete leaked.
   MUST fail the liveness obligation. This failing run is the gate. */
test tcOrphanBuggy [main = DriverBuggy]:
  assert OrphanEventuallyReclaimed in { DriverBuggy, S3Store, GC };

/* Guard on, multiple orphans and faults. Holds. */
test tcOrphanExplore [main = DriverExplore]:
  assert OrphanEventuallyReclaimed in { DriverExplore, S3Store, GC };
