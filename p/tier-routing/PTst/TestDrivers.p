/* Test drivers and declarations for the offset -> tier routing seam.

   The remote tier covers [0, 100). The gate contrasts tcTierRoutingGuarded
   (the first_chunk_id =/= -1 guard present: routing is correct for every probed
   offset and local floor, must hold) with tcTierRoutingBuggy (the guard removed:
   an empty local log routes a remote-only offset to the local tier, must fail
   with the INV#4 silent remote skip). */

/* Deterministically probe every combination of a local floor (including -1 =
   empty local) and an offset spanning below / within / beyond the remote extent.
   One run covers the whole grid, so totality does not rely on random sampling. */
fun RunRouteAll(self: machine, bug: bool) {
  var manifest: machine;
  var reader: machine;
  var fcids: seq[int];
  var fi: int;
  var oi: int;
  var t: Tier;

  manifest = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100));
  reader = new Reader((bug = bug, manifest = manifest, driver = self));

  fcids += (0, -1);
  fcids += (1, 0);
  fcids += (2, 30);

  fi = 0;
  while (fi < sizeof(fcids)) {
    oi = 0;
    while (oi < 8) {
      send reader, eResolve, (from = self, firstChunkId = fcids[fi], offset = oi * 25);
      receive { case eResolveDone: (tt: Tier) { t = tt; } }
      oi = oi + 1;
    }
    fi = fi + 1;
  }
}

/* Deterministic empty-local case with the offset inside the remote extent. */
fun RunRouteFixed(self: machine, bug: bool, fcid: int, off: int) {
  var manifest: machine;
  var reader: machine;
  var t: Tier;

  manifest = new ManifestStore((nonEmpty = true, remoteFirst = 0, remoteNext = 100));
  reader = new Reader((bug = bug, manifest = manifest, driver = self));

  send reader, eResolve, (from = self, firstChunkId = fcid, offset = off);
  receive { case eResolveDone: (tt: Tier) { t = tt; } }
}

machine DriverGuarded {
  start state Init {
    entry { RunRouteAll(this, false); }
  }
}

machine DriverBuggy {
  start state Init {
    entry { RunRouteFixed(this, true, -1, 50); }
  }
}

/* Current code: routing is correct for every local floor and offset. Must hold. */
test tcTierRoutingGuarded [main = DriverGuarded]:
  assert TierRoutingCorrect in { DriverGuarded, ManifestStore, Reader };

/* Guard removed: an empty local log (first_chunk_id = -1) routes a remote-only
   offset to the local tier. MUST fail with the INV#4 silent remote skip. This
   failing run is the gate. */
test tcTierRoutingBuggy [main = DriverBuggy]:
  assert TierRoutingCorrect in { DriverBuggy, ManifestStore, Reader };
