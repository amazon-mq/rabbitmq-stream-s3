/* The per-node manifest replica (rabbitmq_stream_s3_manifest_replica): the
   serialized owner of the live first_offset and the set of live (offset, uid)
   entries. It is the single source GC's still_dangling/1 re-reads. Every state
   change is announced so the monitors track the live manifest. */
machine ManifestReplica {
  var floor: int;
  var nextOffset: int;
  var entries: set[ObjKey];

  start state Serving {
    entry (init: (floor: int, nextOffset: int)) {
      floor = init.floor;
      nextOffset = init.nextOffset;
      /* Establish the initial floor for the frontier monitor. */
      announce eFloorChanged, (newFloor = floor, isReset = false);
    }
    on eMRSetFloor do (p: (from: machine, floor: int, isReset: bool)) {
      floor = p.floor;
      announce eFloorChanged, (newFloor = p.floor, isReset = p.isReset);
      send p.from, eMRAck;
    }
    on eMRAddEntry do (p: (from: machine, key: ObjKey)) {
      entries += (p.key);
      announce eObjectReferenced, p.key;
      send p.from, eMRAck;
    }
    on eMRRemoveEntry do (p: (from: machine, key: ObjKey)) {
      if (p.key in entries) {
        entries -= (p.key);
      }
      announce eObjectUnreferenced, p.key;
      send p.from, eMRAck;
    }
    on eMRGetFloor do (p: (from: machine)) {
      send p.from, eMRFloorResult, floor;
    }
  }
}
