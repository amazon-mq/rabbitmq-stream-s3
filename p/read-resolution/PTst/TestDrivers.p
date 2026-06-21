/* Test drivers and declarations for the read / tier-resolution seam.

   The validation gate contrasts tcReadResolveGuarded (the current exhaustive
   resolve_first_lookup/1: a transient group fetch surfaces as RETRY, must hold)
   with tcReadResolveBuggy (the pre-e3f931b catch-all collapsing group_fetch_failed
   to {local, first}: must fail with the INV#4 silent remote skip). */

/* Deterministic: remote tier non-empty, the leading group fetch always fails
   transiently, no retention. The only correct outcome is RETRY. */
fun RunResolve(self: machine, bugCatchAll: bool, alwaysFail: bool) {
  var ms: machine;
  var gs: machine;
  var reader: machine;
  var res: Resolution;

  gs = new GroupStore((present = true, alwaysFail = alwaysFail));
  ms = new ManifestStore((nonEmpty = true, remoteFirst = 0, localFloor = 100, groups = gs));
  reader = new Reader((bugCatchAll = bugCatchAll, manifest = ms, groups = gs, driver = self));

  send reader, eResolveFirst, (from = self,);
  receive { case eResolveDone: (r: Resolution) { res = r; } }

  /* Anti-vacuity for the guarded run: a transient fetch must actually surface as
     RETRY, not merely "not local". */
  if (!bugCatchAll && alwaysFail) {
    assert res == RETRY,
      "guarded resolve_first must surface RETRY on a transient group fetch failure";
  }
}

/* Exploration: remote tier non-empty, group fetch fails nondeterministically,
   and a retention advance races the resolution. Guard on; no schedule may
   resolve to the local tier while the remote tier is still non-empty. */
fun RunResolveExplore(self: machine) {
  var ms: machine;
  var gs: machine;
  var reader: machine;
  var done: int;

  gs = new GroupStore((present = true, alwaysFail = false));
  ms = new ManifestStore((nonEmpty = true, remoteFirst = 0, localFloor = 100, groups = gs));
  reader = new Reader((bugCatchAll = false, manifest = ms, groups = gs, driver = self));

  send ms, eAdvanceRetention, (from = self,);
  send reader, eResolveFirst, (from = self,);

  done = 0;
  while (done < 2) {
    receive {
      case eMSAck: { done = done + 1; }
      case eResolveDone: (r: Resolution) { done = done + 1; }
    }
  }
}

machine DriverGuarded {
  start state Init {
    entry { RunResolve(this, false, true); }
  }
}

machine DriverBuggy {
  start state Init {
    entry { RunResolve(this, true, true); }
  }
}

machine DriverExplore {
  start state Init {
    entry { RunResolveExplore(this); }
  }
}

/* Current code: a transient group fetch surfaces as RETRY. Must hold. */
test tcReadResolveGuarded [main = DriverGuarded]:
  assert NoSilentRemoteSkip in
  { DriverGuarded, ManifestStore, GroupStore, Reader };

/* Pre-e3f931b catch-all: group_fetch_failed collapses to local fallback. MUST
   fail with the INV#4 silent remote skip. This failing run is the gate. */
test tcReadResolveBuggy [main = DriverBuggy]:
  assert NoSilentRemoteSkip in
  { DriverBuggy, ManifestStore, GroupStore, Reader };

/* Guard on, nondeterministic transient fetch raced against retention. Must hold. */
test tcReadResolveExplore [main = DriverExplore]:
  assert NoSilentRemoteSkip in
  { DriverExplore, ManifestStore, GroupStore, Reader };
