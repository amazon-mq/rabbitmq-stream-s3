/* Global safety monitors (the spec yardstick) for the durability seam.

   They observe announced events only and never participate in the protocol. The
   headline is NoDanglingReference (INV#2): it must key protection on
   (offset, uid), not offset alone, so it can tell a legal stale-UID delete from
   the illegal fresh-UID delete at the same offset. */

/* INV#2: GC must never delete an object that the live manifest references. */
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
        format("INV#2 violated: GC deleted live object (offset={0}, uid={1}) still referenced by the manifest",
          k.offset, k.uid);
    }
  }
}

/* INV#1: no lost acked data. An offset in the durable live range [floor, next)
   must keep its covering object. Deleting the object that currently covers an
   in-range offset destroys acked data. Keyed by (offset, uid) so a stale-UID
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

/* INV#5: the first_offset frontier is monotonically non-decreasing except across
   an explicitly labeled reset. */
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
