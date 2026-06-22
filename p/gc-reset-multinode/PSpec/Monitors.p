/* Global safety monitors for the multi-node durability seam. Identical in intent
   to ../gc-reset: they observe the AUTHORITATIVE manifest state announced by the
   writer/driver and assert GC never deletes a live (offset, uid). The lagging
   cache is never a source of these events, so "live" always means what the
   committed manifest references, regardless of which node's cache the sweep
   read. */

/* INV#2: GC must never delete an object the live (committed) manifest references. */
spec NoDanglingReference observes eObjectReferenced, eObjectUnreferenced, eGcDelete {
  var live: set[ObjKey];

  start state Watching {
    on eObjectReferenced do (k: ObjKey) {
      live += (k);
    }
    on eObjectUnreferenced do (k: ObjKey) {
      if (k in live) {
        live -= (k);
      }
    }
    on eGcDelete do (k: ObjKey) {
      assert !(k in live),
        format("INV#2 violated: GC deleted live object (offset={0}, uid={1}) still referenced by the committed manifest (stale-cache sweep)",
          k.offset, k.uid);
    }
  }
}

/* INV#1: no lost acked data. The object currently covering an offset at or above
   the live floor must not be deleted. Keyed by (offset, uid) so a stale-UID
   delete is not mistaken for losing the live cover. */
spec NoLostAckedData observes eObjectReferenced, eObjectUnreferenced, eFloorChanged, eGcDelete {
  var floor: int;
  var liveByOffset: map[int, int];

  start state Watching {
    on eFloorChanged do (p: (newFloor: int, isReset: bool)) {
      floor = p.newFloor;
    }
    on eObjectReferenced do (k: ObjKey) {
      if (k.offset in liveByOffset) {
        liveByOffset[k.offset] = k.uid;
      } else {
        liveByOffset += (k.offset, k.uid);
      }
    }
    on eObjectUnreferenced do (k: ObjKey) {
      if (k.offset in liveByOffset) {
        if (liveByOffset[k.offset] == k.uid) {
          liveByOffset -= (k.offset);
        }
      }
    }
    on eGcDelete do (k: ObjKey) {
      if (k.offset >= floor) {
        if (k.offset in liveByOffset) {
          if (liveByOffset[k.offset] == k.uid) {
            assert false,
              format("INV#1 violated: GC deleted the live cover of acked offset {0} (uid={1}) at or above the live floor {2}",
                k.offset, k.uid, floor);
          }
        }
      }
    }
  }
}

/* INV#5: the committed first_offset frontier is monotonically non-decreasing
   except across an explicitly labeled reset. Only the authoritative writer
   announces floor changes. */
spec MonotonicFrontier observes eFloorChanged {
  var floor: int;
  var started: bool;

  start state Watching {
    on eFloorChanged do (p: (newFloor: int, isReset: bool)) {
      if (!started) {
        floor = p.newFloor;
        started = true;
      } else {
        if (!p.isReset) {
          assert p.newFloor >= floor,
            format("INV#5 violated: first_offset moved backward from {0} to {1} without a labeled reset",
              floor, p.newFloor);
        }
        floor = p.newFloor;
      }
    }
  }
}
