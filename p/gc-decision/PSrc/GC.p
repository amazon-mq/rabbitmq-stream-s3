/* The orphan GC sweep (rabbitmq_stream_s3_gc), the whole decision in one machine.
   Split into the three steps the code performs, driven explicitly so a driver can
   interleave a reset between them:

     1. eGcSnapshot  - build_lookup/build_stream_lookup: quorum-read the committed
                       epoch, read the cached manifest view, apply the epoch gate,
                       and form the snapshot (committed epoch + cached floor and
                       carve-out).
     2. eGcClassify  - LIST S3 and classify/2 each object against the snapshot.
     3. eGcExecute   - for each candidate apply still_dangling/1 (re-read the live
                       cache) and delete.

   Three independent guards, each the fix one single-axis model proved load-bearing:

     - epochGate    (GUARD A, build_lookup): skip the stream unless the cache
                    epoch equals the committed epoch. Without it a node that has
                    not applied a reset sweeps against a stale-high floor.
     - reread       (GUARD B, still_dangling): compare to the LIVE cache floor, not
                    the snapshot floor. Without it a reset that lowers the floor
                    between snapshot and execute lets a stale-high snapshot reap a
                    re-tiered live object.
     - leadingReread(GUARD C, still_dangling): re-derive the leading-group carve-out
                    from the LIVE manifest, not the snapshot. Without it a reset
                    plus forward retention that installs a new referenced leading
                    group below the live floor is deleted by the stale snapshot
                    carve-out.

     - epochRecheck (GUARD D, still_dangling): PROPOSED, not shipped. Re-read the
                    committed epoch and the cache epoch at execute time and fail
                    closed when they differ. GUARD A samples the committed epoch
                    once, in build_lookup; still_dangling re-reads only the floor
                    (get_manifest drops the epoch). A reset that commits AFTER the
                    snapshot, on a node whose cache has not applied the sync,
                    therefore passes the (already-sampled) epoch gate and the live
                    re-read sees the same stale-high floor. GUARD D closes that by
                    re-validating freshness at the point of deletion; it requires
                    still_dangling to use the epoch-aware read (get_manifest_and_epoch)
                    plus a fresh get_consistent. */
machine GC {
  var epochGate: bool;
  var reread: bool;
  var leadingReread: bool;
  var epochRecheck: bool;
  var db: machine;
  var cache: machine;
  var s3: machine;
  var driver: machine;

  /* Snapshot taken at build_lookup time. */
  var snapEpoch: int;
  var snap: ManifestView;
  var skipped: bool;
  var candidates: seq[Candidate];

  start state Idle {
    entry (init: (epochGate: bool, reread: bool, leadingReread: bool, epochRecheck: bool,
                  db: machine, cache: machine, s3: machine, driver: machine)) {
      epochGate = init.epochGate;
      reread = init.reread;
      leadingReread = init.leadingReread;
      epochRecheck = init.epochRecheck;
      db = init.db;
      cache = init.cache;
      s3 = init.s3;
      driver = init.driver;
      skipped = false;
    }

    on eGcSnapshot do {
      var committedEpoch: int;
      var v: ManifestView;
      /* Quorum read of the committed epoch (get_consistent). */
      send db, eGetConsistent, (from = this,);
      receive { case eEpochResult: (r: (ok: bool, epoch: int)) { committedEpoch = r.epoch; } }
      /* Cached manifest view (get_manifest_and_epoch): floor and carve-out from
         the local replica, plus the epoch it was synced at. */
      send cache, eMRGetFull, (from = this,);
      receive { case eMRFullResult: (x: ManifestView) { v = x; } }
      snapEpoch = committedEpoch;
      snap = v;
      /* No usable cached manifest (undefined or empty entries): skip the stream. */
      if (!v.present) {
        skipped = true;
      }
      /* GUARD A: a cache behind the committed epoch has a floor that predates the
         committed reset, so its floor may be stale-high. Fail closed and skip. */
      if (epochGate && v.epoch != committedEpoch) {
        skipped = true;
      }
      send driver, eGcSnapshotDone;
    }

    on eGcClassify do {
      var objs: set[Obj];
      var o: Obj;
      candidates = default(seq[Candidate]);
      if (!skipped) {
        send s3, eS3List, (from = this,);
        receive { case eS3ListResult: (lst: set[Obj]) { objs = lst; } }
        foreach (o in objs) {
          if (o.kind == DATA) {
            /* A fragment below the snapshot floor is an orphan. */
            if (o.offset < snap.floor) {
              candidates += (sizeof(candidates), (obj = o, reason = BELOW_FLOOR));
            }
          } else if (o.kind == GROUP) {
            /* A group below the floor is an orphan unless protected by the
               snapshot carve-out (referenced leading group / skip-groups). */
            if (o.offset < snap.floor &&
                !groupProtected(o, snap.leadOff, snap.leadUid, snap.leadPresent, snap.skipGroups)) {
              candidates += (sizeof(candidates), (obj = o, reason = BELOW_FLOOR));
            }
          } else {
            /* A manifest below the committed epoch is stale. */
            if (o.epoch < snapEpoch) {
              candidates += (sizeof(candidates), (obj = o, reason = STALE_EPOCH));
            }
          }
        }
      }
      send driver, eGcClassifyDone;
    }

    on eGcExecute do {
      var i: int;
      var c: Candidate;
      i = 0;
      while (i < sizeof(candidates)) {
        c = candidates[i];
        if (stillDangling(c)) {
          send s3, eS3Delete, (from = this, obj = c.obj);
          receive { case eS3Ack: { } }
          announce eGcDelete, c.obj;
        }
        i = i + 1;
      }
      send driver, eGcExecuteDone;
    }
  }

  /* still_dangling/1. A stale_epoch candidate is deleted with no re-check (epoch
     is monotonic; safety rests on ../writer-fencing). A below_first_offset
     candidate re-reads the live cache: GUARD B picks the live floor over the
     snapshot floor, GUARD C re-derives the carve-out from the live view. A live
     read with no usable manifest keeps the object (a later sweep reclaims it). */
  fun stillDangling(c: Candidate): bool {
    var live: ManifestView;
    var committedNow: int;
    var effFloor: int;
    var leadOff: int;
    var leadUid: int;
    var leadPresent: bool;
    var skipGroups: bool;

    if (c.reason == STALE_EPOCH) {
      return true;
    }

    send cache, eMRGetLive, (from = this,);
    receive { case eMRLiveResult: (x: ManifestView) { live = x; } }
    if (!live.present) {
      return false;
    }

    /* GUARD D (proposed): re-validate the committed epoch against the cache epoch
       at execute time. A reset committed after the snapshot leaves the cache
       behind the committed epoch; fail closed rather than delete against a floor
       that predates it. */
    if (epochRecheck) {
      send db, eGetConsistent, (from = this,);
      receive { case eEpochResult: (r: (ok: bool, epoch: int)) { committedNow = r.epoch; } }
      if (live.epoch != committedNow) {
        return false;
      }
    }

    if (reread) {
      effFloor = live.floor;
    } else {
      effFloor = snap.floor;
    }

    if (c.obj.kind == DATA) {
      return c.obj.offset < effFloor;
    }

    /* GROUP: re-validate the offset and the carve-out. */
    if (leadingReread) {
      leadOff = live.leadOff; leadUid = live.leadUid;
      leadPresent = live.leadPresent; skipGroups = live.skipGroups;
    } else {
      leadOff = snap.leadOff; leadUid = snap.leadUid;
      leadPresent = snap.leadPresent; skipGroups = snap.skipGroups;
    }
    return c.obj.offset < effFloor &&
           !groupProtected(c.obj, leadOff, leadUid, leadPresent, skipGroups);
  }
}
