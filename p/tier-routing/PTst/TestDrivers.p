/* Test drivers and declarations for the offset -> tier routing seam.

   The remote tier covers [0, 100) in every tiered scenario. Guarded tests must
   hold; each buggy test removes one load-bearing piece and MUST fail with the
   INV#4 silent remote skip, proving the spec has discriminating power against
   that exact defect (the validation gate):

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
     no eMarkPending, reader attaches while the cache row is absent on a tiered
     stream (models the code before the member-init marker existed). Must
     fail, proving the marker's placement (before readers can attach) is
     load-bearing, not just the reader's PENDING branch. */

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
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100));
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
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100));
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
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100));
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
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100));
      MarkPending(this, store);
      reader = new Reader((bugNoMinusOneGuard = false, bugMissFallsLocal = true, manifest = store, driver = this));
      o = Probe(this, reader, 30, 5);
    }
  }
}

/* The environment weakened: no pending marker at all (the code before the
   member-init marker existed). The reader is fully guarded, yet a reader
   attaching while the row is absent on a tiered stream still routes LOCAL,
   because ABSENT is indistinguishable from an un-tiered stream. */
machine DriverBuggyNoMarker {
  start state Init {
    entry {
      var store: machine;
      var reader: machine;
      var o: Outcome;
      announce eGroundTruth, (nonEmpty = true, remoteFirst = 0, remoteNext = 100);
      store = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100));
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

/* No marker in the lifecycle: MUST fail with the INV#4 silent remote skip. */
test tcTierRoutingBuggyNoMarker [main = DriverBuggyNoMarker]:
  assert TierRoutingCorrect in { DriverBuggyNoMarker, ManifestStore, Reader };
