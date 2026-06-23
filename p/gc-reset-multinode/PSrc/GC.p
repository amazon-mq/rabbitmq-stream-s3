/* The orphan GC sweep (rabbitmq_stream_s3_gc) on the operator CLI path
   (stream_s3_gc --mode delete via run/1 -> build_lookup), targeting ONE node's
   replica cache. Split into the three steps that matter:

     1. eGcSnapshot     - build_lookup: read the committed epoch with a quorum
                          read AND the floor from the local replica cache. The
                          epoch guard (the fix) compares the two and fails closed
                          when the cache is behind the committed epoch.
     2. eGcListClassify - LIST S3 and classify each object against the SNAPSHOT
                          floor (offset axis only; data objects have no epoch
                          axis - see classify/2).
     3. eGcExecute      - for each candidate, apply still_dangling/1 (re-read the
                          LIVE cache floor) and delete.

   Two independent guards:
     - stillDanglingEnabled: the SHIPPED offset guard (still_dangling/1). Always
       on in these tests, because the point is that it is INSUFFICIENT here: it
       re-reads the SAME lagging cache, so a stale-high floor defeats it.
     - epochGuardEnabled: the FIX. Skip the stream unless the cache's epoch
       equals the committed epoch, so a node that has not applied the reset's
       sync never sweeps against its stale floor. */
machine GC {
  var stillDanglingEnabled: bool;
  var epochGuardEnabled: bool;
  var db: machine;
  var target: machine;
  var s3: machine;
  var driver: machine;
  var snapshotFloor: int;
  var committedEpoch: int;
  var cacheEpoch: int;
  var skipped: bool;
  var candidates: seq[Candidate];

  start state Idle {
    entry (init: (stillDanglingEnabled: bool, epochGuardEnabled: bool,
                  db: machine, target: machine, s3: machine, driver: machine)) {
      stillDanglingEnabled = init.stillDanglingEnabled;
      epochGuardEnabled = init.epochGuardEnabled;
      db = init.db;
      target = init.target;
      s3 = init.s3;
      driver = init.driver;
      skipped = false;
    }

    on eGcSnapshot do {
      var fe: (floor: int, epoch: int);
      /* Quorum read of the committed epoch (get_consistent). */
      send db, eGetConsistent, (from = this,);
      receive { case eEpochResult: (r: (ok: bool, epoch: int)) { committedEpoch = r.epoch; } }
      /* Floor (and its epoch) from the LOCAL replica cache (get_manifest). */
      send target, eMRGetFloorEpoch, (from = this,);
      receive { case eMRFloorEpochResult: (x: (floor: int, epoch: int)) { fe = x; } }
      snapshotFloor = fe.floor;
      cacheEpoch = fe.epoch;
      /* The fix: a cache behind the committed epoch has a floor that predates the
         committed reset, so its floor may be stale-high. Fail closed and skip. */
      if (epochGuardEnabled && cacheEpoch != committedEpoch) {
        skipped = true;
      }
      send driver, eGcSnapshotDone;
    }

    on eGcListClassify do {
      var objs: set[ObjKey];
      var o: ObjKey;
      candidates = default(seq[Candidate]);
      if (!skipped) {
        send s3, eS3List, (from = this,);
        receive { case eS3ListResult: (lst: set[ObjKey]) { objs = lst; } }
        foreach (o in objs) {
          /* classify/2 data axis: below the snapshot floor is a candidate. */
          if (o.offset < snapshotFloor) {
            candidates += (sizeof(candidates), (offset = o.offset, uid = o.uid, reason = BELOW_FLOOR));
          }
        }
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
        /* still_dangling/1: re-read the live floor from the SAME replica cache.
           On a lagging node this returns the stale-high floor, so the re-read
           confirms the (wrong) delete - the offset guard defends nothing. */
        if (c.reason == BELOW_FLOOR && stillDanglingEnabled) {
          send target, eMRGetFloor, (from = this,);
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
