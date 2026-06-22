/* The orphan GC sweep, modeling classify/classify_group. It snapshots the floor
   and the leading group key (build_lookup), lists S3, and deletes objects below
   the floor. A group below the floor is deletable UNLESS it is the referenced
   leading group; guardLeadingGroup toggles that carve-out. */
machine GC {
  var guardLeadingGroup: bool;
  var manifest: machine;
  var s3: machine;
  var driver: machine;

  start state Idle {
    entry (init: (guardLeadingGroup: bool, manifest: machine, s3: machine, driver: machine)) {
      guardLeadingGroup = init.guardLeadingGroup;
      manifest = init.manifest;
      s3 = init.s3;
      driver = init.driver;
    }
    on eGcSweep do {
      var floor: int;
      var lg: (has: bool, key: ObjKey);
      var objs: seq[Obj];
      var i: int;
      var o: Obj;
      var del: bool;
      send manifest, eMRGetFloor, (from = this,);
      receive { case eMRFloorResult: (f: int) { floor = f; } }
      send manifest, eMRGetLeadingGroup, (from = this,);
      receive { case eMRLeadingGroupResult: (r: (has: bool, key: ObjKey)) { lg = r; } }
      send s3, eS3List, (from = this,);
      receive { case eS3ListResult: (l: seq[Obj]) { objs = l; } }
      i = 0;
      while (i < sizeof(objs)) {
        o = objs[i];
        del = false;
        if (o.offset < floor) {
          if (o.kind == GROUP) {
            /* classify_group: the leading group below the floor is still
               referenced and must be protected. */
            if (guardLeadingGroup && lg.has && o.offset == lg.key.offset && o.uid == lg.key.uid) {
              del = false;
            } else {
              del = true;
            }
          } else {
            del = true;
          }
        }
        if (del) {
          send s3, eS3Delete, (from = this, key = (offset = o.offset, uid = o.uid));
          receive { case eS3Ack: { } }
          announce eGcDelete, (offset = o.offset, uid = o.uid);
        }
        i = i + 1;
      }
      send driver, eGcSweepDone;
    }
  }
}
