/* The orphan GC sweep (rabbitmq_stream_s3_gc) on the operator CLI path, split
   into the three steps that matter for the decide-then-act window:

     1. eGcSnapshot     - build_lookup / lookup_entry: capture the committed
                          epoch, the snapshot first_offset, AND the snapshot
                          leading-group carve-out (referenced_group_key +
                          skip_groups) via leading_group_info/2. This carve-out is
                          computed ONCE and passed by value through classify.
     2. eGcListClassify - LIST S3 and classify each object against the SNAPSHOT
                          floor and SNAPSHOT carve-out. Groups go through
                          classify_group/3; fragments through the offset axis.
     3. eGcExecute      - for each candidate, apply still_dangling/1 (re-read the
                          LIVE floor) and delete.

   The shipped still_dangling/1, for a group finding, re-reads only the live
   FLOOR (Offset < first_offset). It does NOT re-run leading_group_info/2, so it
   never re-validates the carve-out against the LIVE manifest.

   recheckCarveOut models the FIX: in still_dangling, re-fetch the LIVE leading
   group and skip a group that is now the live referenced leading group (or when
   the live manifest is in conservative skip_groups mode). With it false the
   shipped offset-only re-check stands, and the seam is reproduced. */
machine GC {
  var recheckCarveOut: bool;
  var db: machine;
  var manifest: machine;
  var s3: machine;
  var driver: machine;
  var snapshotFloor: int;
  var snapshotEpoch: int;
  var snapLeadingKey: ObjKey;
  var snapHasLeading: bool;
  var snapSkipGroups: bool;
  var candidates: seq[Candidate];

  start state Idle {
    entry (init: (recheckCarveOut: bool, db: machine, manifest: machine, s3: machine, driver: machine)) {
      recheckCarveOut = init.recheckCarveOut;
      db = init.db;
      manifest = init.manifest;
      s3 = init.s3;
      driver = init.driver;
    }

    on eGcSnapshot do {
      var lg: (leadingKey: ObjKey, hasLeading: bool, skipGroups: bool);
      send db, eGetConsistent, (from = this,);
      receive { case eEpochResult: (r: (ok: bool, epoch: int)) { snapshotEpoch = r.epoch; } }
      send manifest, eMRGetFloor, (from = this,);
      receive { case eMRFloorResult: (f: int) { snapshotFloor = f; } }
      /* leading_group_info/2 on the snapshot manifest: this is the carve-out
         classify will use for the whole sweep. */
      send manifest, eMRGetLeading, (from = this,);
      receive { case eMRLeadingResult: (x: (leadingKey: ObjKey, hasLeading: bool, skipGroups: bool)) { lg = x; } }
      snapLeadingKey = lg.leadingKey;
      snapHasLeading = lg.hasLeading;
      snapSkipGroups = lg.skipGroups;
      send driver, eGcSnapshotDone;
    }

    on eGcListClassify do {
      var objs: seq[Obj];
      var i: int;
      var o: Obj;
      var key: ObjKey;
      send s3, eS3List, (from = this,);
      receive { case eS3ListResult: (lst: seq[Obj]) { objs = lst; } }
      candidates = default(seq[Candidate]);
      i = 0;
      while (i < sizeof(objs)) {
        o = objs[i];
        if (o.offset < snapshotFloor) {
          key = (offset = o.offset, uid = o.uid);
          if (o.kind == FRAGMENT) {
            /* Data axis: any fragment below the floor is a candidate. */
            candidates += (sizeof(candidates), (offset = o.offset, uid = o.uid, kind = FRAGMENT, reason = BELOW_FLOOR));
          } else {
            /* classify_group/3, against the SNAPSHOT carve-out: skip the snapshot
               leading group, and skip every group in conservative skip_groups
               mode; otherwise the group below the floor is a candidate. */
            if (!snapSkipGroups && !(snapHasLeading && key == snapLeadingKey)) {
              candidates += (sizeof(candidates), (offset = o.offset, uid = o.uid, kind = GROUP, reason = BELOW_FLOOR));
            }
          }
        }
        i = i + 1;
      }
      send driver, eGcClassifyDone;
    }

    on eGcExecute do {
      var i: int;
      var c: Candidate;
      var del: bool;
      var liveFloor: int;
      var lg: (leadingKey: ObjKey, hasLeading: bool, skipGroups: bool);
      var key: ObjKey;
      i = 0;
      while (i < sizeof(candidates)) {
        c = candidates[i];
        key = (offset = c.offset, uid = c.uid);
        del = true;
        /* still_dangling/1: re-read the live floor and skip anything now at or
           above it. This is the shipped offset-only re-check, applied to both
           fragments and groups. */
        send manifest, eMRGetFloor, (from = this,);
        receive { case eMRFloorResult: (f: int) { liveFloor = f; } }
        if (!(c.offset < liveFloor)) {
          del = false;
        }
        /* The FIX: for a group finding, also re-validate the carve-out against
           the LIVE manifest. A group that is now the live referenced leading
           group (or any group when the live manifest is in skip_groups mode) must
           not be deleted, even though it is below the live floor. */
        if (del && c.kind == GROUP && recheckCarveOut) {
          send manifest, eMRGetLeading, (from = this,);
          receive { case eMRLeadingResult: (x: (leadingKey: ObjKey, hasLeading: bool, skipGroups: bool)) { lg = x; } }
          if (lg.skipGroups || (lg.hasLeading && key == lg.leadingKey)) {
            del = false;
          }
        }
        if (del) {
          send s3, eS3Delete, (from = this, key = key);
          receive { case eS3Ack: { } }
          announce eGcDelete, key;
        }
        i = i + 1;
      }
      send driver, eGcExecuteDone;
    }
  }
}
