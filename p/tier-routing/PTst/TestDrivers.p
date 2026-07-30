/* Test drivers and declarations for the offset -> tier routing seam.

   The remote tier covers [0, 100) in every tiered scenario. Guarded tests must
   hold; each buggy test either removes one load-bearing piece or restores a
   retired classification, and MUST fail with the INV#4 silent remote skip,
   proving the spec has discriminating power against that exact defect (the
   validation gate) -- except tcTierRoutingBuggyNoMarker, which now holds
   (see below):

   - tcTierRoutingGuardedWarm: full lifecycle (mark pending, resolve), then the
     probe grid. Holds.
   - tcTierRoutingGuardedColdStart: readers attach at every point of the
     lifecycle (before resolution completes); the checker interleaves probes
     with the resolution. Pending attaches RETRY; nothing routes LOCAL. Holds,
     and a probe after the acknowledged resolution must not RETRY.
   - tcTierRoutingBuggyNoMinusOneGuard: the =/= -1 floor guard removed on a
     warm cache. Must fail.
   - tcTierRoutingBuggyMissFallsLocal: the pre-fix miss collapse (PENDING ->
     local first) with an unresolved cache. Must fail: this is the boot-window
     bug, a consumer attaching below the local floor after a node restart and
     before resolution.
   - tcTierRoutingBuggyNoMarker: the environment weakened instead of the code:
     no eMarkPending, reader attaches while the cache row is missing (Cold) on
     a tiered stream (models the code before the member-init marker existed).
     Now HOLDS: Cold defaults to PENDING (fail closed), so a missing marker no
     longer causes a silent remote skip by itself -- marker placement is no
     longer load-bearing for INV#4. Kept as a regression test: it proves the
     fix does not regress if marker-writing breaks again (register_replica_
     context's gen_server:call silently failing).
   - tcTierRoutingBuggyColdAbsent: the retired classification restored
     directly via ManifestStore's bugColdReportsAbsent, independent of marker
     timing (no eMarkPending here either, same setup as BuggyNoMarker, but
     the variable under test is the cache's classification of a missing row,
     not the marker). Must fail: this is the historical "no row = un-tiered
     stream" bug this task retires. */

fun Probe(self: machine, reader: machine, fcid: int, off: int) : Outcome {
  var o: Outcome;
  send reader, eResolve, (from = self, firstChunkId = fcid, offset = off);
  receive { case eResolveDone: (oo: Outcome) { o = oo; } }
  return o;
}

/* Probe every combination of a local floor (including -1 = empty local) and an
   offset spanning below / within / beyond the remote extent. Deterministic:
   one run covers the whole grid, so totality does not rely on random
   sampling. */
fun ProbeGrid(self: machine, reader: machine) {
  var fcids: seq[int];
  var fi: int;
  var oi: int;
  var o: Outcome;

  fcids += (0, -1);
  fcids += (1, 0);
  fcids += (2, 30);

  fi = 0;
  while (fi < sizeof(fcids)) {
    oi = 0;
    while (oi < 8) {
      o = Probe(self, reader, fcids[fi], oi * 25);
      oi = oi + 1;
    }
    fi = fi + 1;
  }
}

fun MarkPending(self: machine, store: machine) {
  send store, eMarkPending, (from = self,);
  receive { case eMSAck: { } }
}

fun ResolveManifest(self: machine, store: machine) {
  send store, eResolveManifest, (from = self,);
  receive { case eMSAck: { } }
}

/* Full lifecycle before any reader exists, then the grid. */
machine DriverGuardedWarm {
  start state Init {
    entry {
      var store: machine;
      var reader: machine;
      announce eGroundTruth, (nonEmpty = true, remoteFirst = 0, remoteNext = 100);
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100, bugColdReportsAbsent = false));
      MarkPending(this, store);
      ResolveManifest(this, store);
      reader = new Reader((bugNoMinusOneGuard = false, bugMissFallsLocal = false, manifest = store, driver = this));
      ProbeGrid(this, reader);
    }
  }
}

/* Cold start: the marker is placed (as member init does, before readers can
   attach), then probes and the resolution race. Probes that land before the
   resolution see PENDING and must RETRY, never LOCAL; a probe after the
   acknowledged resolution must be served. */
machine DriverGuardedColdStart {
  start state Init {
    entry {
      var store: machine;
      var reader: machine;
      var o: Outcome;
      var i: int;
      announce eGroundTruth, (nonEmpty = true, remoteFirst = 0, remoteNext = 100);
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100, bugColdReportsAbsent = false));
      MarkPending(this, store);
      reader = new Reader((bugNoMinusOneGuard = false, bugMissFallsLocal = false, manifest = store, driver = this));
      i = 0;
      while (i < 4) {
        /* Below the local floor (30), inside the remote extent: the offset the
           boot-window bug used to skip. */
        o = Probe(this, reader, 30, 5);
        assert o != LOCAL, "cold-start probe below the floor routed LOCAL";
        if ($) {
          ResolveManifest(this, store);
        }
        i = i + 1;
      }
      ResolveManifest(this, store);
      /* Resolution acknowledged: the read must now be served remotely. */
      o = Probe(this, reader, 30, 5);
      assert o == REMOTE, "probe after acknowledged resolution was not served remotely";
      ProbeGrid(this, reader);
    }
  }
}

/* The =/= -1 guard removed on a warm cache: an empty local log routes a
   remote-only offset to the local tier. */
machine DriverBuggyNoMinusOneGuard {
  start state Init {
    entry {
      var store: machine;
      var reader: machine;
      var o: Outcome;
      announce eGroundTruth, (nonEmpty = true, remoteFirst = 0, remoteNext = 100);
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100, bugColdReportsAbsent = false));
      MarkPending(this, store);
      ResolveManifest(this, store);
      reader = new Reader((bugNoMinusOneGuard = true, bugMissFallsLocal = false, manifest = store, driver = this));
      o = Probe(this, reader, -1, 50);
    }
  }
}

/* The pre-fix miss collapse: the cache row is pending (marked, unresolved) and
   the reader treats the miss as "no remote tier -> local first". */
machine DriverBuggyMissFallsLocal {
  start state Init {
    entry {
      var store: machine;
      var reader: machine;
      var o: Outcome;
      announce eGroundTruth, (nonEmpty = true, remoteFirst = 0, remoteNext = 100);
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100, bugColdReportsAbsent = false));
      MarkPending(this, store);
      reader = new Reader((bugNoMinusOneGuard = false, bugMissFallsLocal = true, manifest = store, driver = this));
      o = Probe(this, reader, 30, 5);
    }
  }
}

/* The environment weakened: no pending marker at all (the code before the
   member-init marker existed). The reader is fully guarded and ManifestStore
   uses its safe default (Cold answers PENDING, bugColdReportsAbsent off), so
   a reader attaching while the row is merely missing (no marker written yet)
   correctly RETRYs instead of routing LOCAL. This now HOLDS: proves marker
   placement is no longer load-bearing for INV#4 -- only the cold-cache
   default is (see DriverBuggyColdAbsent for that axis in isolation). */
machine DriverBuggyNoMarker {
  start state Init {
    entry {
      var store: machine;
      var reader: machine;
      var o: Outcome;
      announce eGroundTruth, (nonEmpty = true, remoteFirst = 0, remoteNext = 100);
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100, bugColdReportsAbsent = false));
      reader = new Reader((bugNoMinusOneGuard = false, bugMissFallsLocal = false, manifest = store, driver = this));
      o = Probe(this, reader, 30, 5);
    }
  }
}

/* The retired classification bug, reproduced directly: the cache row is
   truly missing (Cold, no marker -- same setup as DriverBuggyNoMarker), but
   Cold is toggled to answer ABSENT instead of the safe PENDING default. This
   isolates the classification axis from the marker-timing axis: unlike
   DriverBuggyNoMarker (which now holds), this must still fail, because the
   bug here is what the cache reports, not whether a marker was written. */
machine DriverBuggyColdAbsent {
  start state Init {
    entry {
      var store: machine;
      var reader: machine;
      var o: Outcome;
      announce eGroundTruth, (nonEmpty = true, remoteFirst = 0, remoteNext = 100);
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100, bugColdReportsAbsent = true));
      reader = new Reader((bugNoMinusOneGuard = false, bugMissFallsLocal = false, manifest = store, driver = this));
      o = Probe(this, reader, 30, 5);
    }
  }
}

/* Current code, warm cache: must hold. */
test tcTierRoutingGuardedWarm [main = DriverGuardedWarm]:
  assert TierRoutingCorrect in { DriverGuardedWarm, ManifestStore, Reader };

/* Current code, readers racing the resolution: must hold. */
test tcTierRoutingGuardedColdStart [main = DriverGuardedColdStart]:
  assert TierRoutingCorrect in { DriverGuardedColdStart, ManifestStore, Reader };

/* Guard removed: MUST fail with the INV#4 silent remote skip. */
test tcTierRoutingBuggyNoMinusOneGuard [main = DriverBuggyNoMinusOneGuard]:
  assert TierRoutingCorrect in { DriverBuggyNoMinusOneGuard, ManifestStore, Reader };

/* Pre-fix miss collapse: MUST fail with the INV#4 silent remote skip. */
test tcTierRoutingBuggyMissFallsLocal [main = DriverBuggyMissFallsLocal]:
  assert TierRoutingCorrect in { DriverBuggyMissFallsLocal, ManifestStore, Reader };

/* No marker in the lifecycle, Cold defaults to PENDING: now HOLDS. Marker
   placement is no longer load-bearing for INV#4 (see
   tcTierRoutingBuggyColdAbsent for the classification axis, which still
   fails). */
test tcTierRoutingBuggyNoMarker [main = DriverBuggyNoMarker]:
  assert TierRoutingCorrect in { DriverBuggyNoMarker, ManifestStore, Reader };

/* Retired ABSENT classification restored directly: MUST fail with the INV#4
   silent remote skip, independent of marker timing. */
test tcTierRoutingBuggyColdAbsent [main = DriverBuggyColdAbsent]:
  assert TierRoutingCorrect in { DriverBuggyColdAbsent, ManifestStore, Reader };
