/* The orphan GC sweep (rabbitmq_stream_s3_gc), split into the three steps that
   matter for the decide-then-act window:

     1. eGcSnapshot     - build_lookup: capture the committed epoch and the
                          snapshot first_offset (once).
     2. eGcListClassify - LIST S3 and classify each object against the SNAPSHOT
                          floor/epoch. The LIST runs after the snapshot, so a
                          post-snapshot re-tier can appear here.
     3. eGcExecute      - for each candidate, apply still_dangling/1 and delete.

   guardEnabled toggles still_dangling/1: with it on, a below_first_offset
   finding is re-validated against the LIVE floor immediately before deletion;
   with it off, the sweep deletes against the stale snapshot (the bug). */
machine GC {
  var guardEnabled: bool;
  var db: machine;
  var manifest: machine;
  var s3: machine;
  var driver: machine;
  var snapshotFloor: int;
  var snapshotEpoch: int;
  var candidates: seq[Candidate];

  start state Idle {
    entry (init: (guardEnabled: bool, db: machine, manifest: machine, s3: machine, driver: machine)) {
      guardEnabled = init.guardEnabled;
      db = init.db;
      manifest = init.manifest;
      s3 = init.s3;
      driver = init.driver;
    }

    on eGcSnapshot do {
      send db, eGetConsistent, (from = this,);
      receive { case eEpochResult: (r: (ok: bool, epoch: int)) { snapshotEpoch = r.epoch; } }
      send manifest, eMRGetFloor, (from = this,);
      receive { case eMRFloorResult: (f: int) { snapshotFloor = f; } }
      send driver, eGcSnapshotDone;
    }

    on eGcListClassify do {
      var objs: seq[Obj];
      var i: int;
      var o: Obj;
      send s3, eS3List, (from = this,);
      receive { case eS3ListResult: (lst: seq[Obj]) { objs = lst; } }
      candidates = default(seq[Candidate]);
      i = 0;
      while (i < sizeof(objs)) {
        o = objs[i];
        /* below_first_offset takes priority, matching classify/2: an object
           below the snapshot floor is a candidate regardless of epoch. */
        if (o.offset < snapshotFloor) {
          candidates += (sizeof(candidates),
            (offset = o.offset, uid = o.uid, epoch = o.epoch, reason = BELOW_FLOOR));
        } else if (o.epoch < snapshotEpoch) {
          candidates += (sizeof(candidates),
            (offset = o.offset, uid = o.uid, epoch = o.epoch, reason = STALE_EPOCH));
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
      i = 0;
      while (i < sizeof(candidates)) {
        c = candidates[i];
        del = true;
        /* still_dangling/1: re-validate below_first_offset findings against the
           LIVE floor; epoch-based findings need no re-check (epoch is
           monotonic). When the guard is disabled, the stale snapshot decision
           stands. */
        if (c.reason == BELOW_FLOOR && guardEnabled) {
          send manifest, eMRGetFloor, (from = this,);
          receive { case eMRFloorResult: (f: int) { liveFloor = f; } }
          if (!(c.offset < liveFloor)) {
            del = false;
          }
        }
        if (del) {
          send s3, eS3Delete, (from = this, key = (offset = c.offset, uid = c.uid));
          receive { case eS3Ack: { } }
          announce eGcDelete, (offset = c.offset, uid = c.uid);
        }
        i = i + 1;
      }
      send driver, eGcExecuteDone;
    }
  }
}
