/* The per-node manifest replica (rabbitmq_stream_s3_manifest_replica): the
   serialized owner of the live first_offset, the live (offset, uid) entries, and
   the leading-group carve-out info that leading_group_info/2 derives from the
   manifest's first entry (referenced_group_key + skip_groups). It is the single
   source both GC's snapshot (build_lookup) and still_dangling/1 read. Every state
   change is announced so the monitors track the live manifest. */
machine ManifestReplica {
  var floor: int;
  var nextOffset: int;
  var entries: set[ObjKey];
  /* Live leading-group carve-out, as leading_group_info/2 would return it. */
  var leadingKey: ObjKey;
  var hasLeading: bool;
  var skipGroups: bool;

  start state Serving {
    entry (init: (floor: int, nextOffset: int, leadingKey: ObjKey, hasLeading: bool, skipGroups: bool)) {
      floor = init.floor;
      nextOffset = init.nextOffset;
      leadingKey = init.leadingKey;
      hasLeading = init.hasLeading;
      skipGroups = init.skipGroups;
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
    on eMRSetLeading do (p: (from: machine, leadingKey: ObjKey, hasLeading: bool, skipGroups: bool)) {
      leadingKey = p.leadingKey;
      hasLeading = p.hasLeading;
      skipGroups = p.skipGroups;
      send p.from, eMRAck;
    }
    on eMRGetFloor do (p: (from: machine)) {
      send p.from, eMRFloorResult, floor;
    }
    on eMRGetLeading do (p: (from: machine)) {
      send p.from, eMRLeadingResult, (leadingKey = leadingKey, hasLeading = hasLeading, skipGroups = skipGroups);
    }
  }
}
