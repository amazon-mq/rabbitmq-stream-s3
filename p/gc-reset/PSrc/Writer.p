/* The writer / reset orchestrator (rabbitmq_stream_s3_replica_reader). The only
   modeled behaviour is the remote-tier-ahead reset (restart_at_local_floor):
   lower the durable first_offset, then re-tier the live fragment at that offset
   with a fresh UID.

   Ordering is load-bearing: the floor is lowered (floorFirst) BEFORE the new
   entry is referenced and the re-tiered object is put into S3. This is the
   atomic-prefix that prevents the illegal "object live/observable while the
   floor is still high" state. The guard relies on the live floor being lowered
   before the object it protects can be observed by a sweep; floorFirst = false
   reverses the order to show the guard alone is not sufficient. */
machine Writer {
  var epoch: int;
  var floorFirst: bool;
  var manifest: machine;
  var s3: machine;
  var driver: machine;

  start state Idle {
    entry (init: (epoch: int, floorFirst: bool, manifest: machine, s3: machine, driver: machine)) {
      epoch = init.epoch;
      floorFirst = init.floorFirst;
      manifest = init.manifest;
      s3 = init.s3;
      driver = init.driver;
    }
    on eDoReset do (p: (localFloor: int, newUid: int)) {
      if (floorFirst) {
        send manifest, eMRSetFloor, (from = this, floor = p.localFloor, isReset = true);
        receive { case eMRAck: { } }
        send manifest, eMRAddEntry, (from = this, key = (offset = p.localFloor, uid = p.newUid));
        receive { case eMRAck: { } }
        send s3, eS3Put, (from = this, obj = (offset = p.localFloor, uid = p.newUid, epoch = epoch));
        receive { case eS3Ack: { } }
      } else {
        /* Atomic-prefix violation: the re-tiered object becomes referenced and
           observable while the floor is still high, so a sweep's guard re-read
           still sees the stale-high floor and deletes a live object. */
        send manifest, eMRAddEntry, (from = this, key = (offset = p.localFloor, uid = p.newUid));
        receive { case eMRAck: { } }
        send s3, eS3Put, (from = this, obj = (offset = p.localFloor, uid = p.newUid, epoch = epoch));
        receive { case eS3Ack: { } }
        send manifest, eMRSetFloor, (from = this, floor = p.localFloor, isReset = true);
        receive { case eMRAck: { } }
      }
      send driver, eResetDone;
    }
  }
}
