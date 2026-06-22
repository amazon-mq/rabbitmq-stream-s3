/* The manifest replica: owns the live floor, the set of referenced (offset, uid)
   entries, and the leading group key (leading_group_info/2's referenced_group_key).
   The leading group is referenced even though it sits below the floor. */
machine ManifestReplica {
  var floor: int;
  var entries: set[ObjKey];
  var hasLeadingGroup: bool;
  var leadingGroup: ObjKey;

  start state Serving {
    entry (init: (floor: int)) {
      floor = init.floor;
    }
    on eMRAddEntry do (p: (from: machine, key: ObjKey)) {
      entries += (p.key);
      announce eObjectReferenced, p.key;
      send p.from, eMRAck;
    }
    on eMRSetLeadingGroup do (p: (from: machine, key: ObjKey)) {
      hasLeadingGroup = true;
      leadingGroup = p.key;
      send p.from, eMRAck;
    }
    on eMRGetFloor do (p: (from: machine)) {
      send p.from, eMRFloorResult, floor;
    }
    on eMRGetLeadingGroup do (p: (from: machine)) {
      send p.from, eMRLeadingGroupResult, (has = hasLeadingGroup, key = leadingGroup);
    }
  }
}
