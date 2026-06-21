/* The writer / reset orchestrator (rabbitmq_stream_s3_replica_reader). The only
   modeled behaviour is the remote-tier-ahead reset (restart_at_local_floor):
   lower the durable first_offset, then re-tier the live fragment at that offset
   with a fresh UID.

   Ordering is load-bearing: the floor is lowered and the new entry committed
   (each acknowledged) BEFORE the re-tiered object is put into S3. This is the
   atomic-prefix that prevents the illegal "object present in S3 while the floor
   is still high" state. The guard relies on the live floor being lowered before
   the object it protects can be observed by a sweep. */
machine Writer {
  var epoch: int;
  var manifest: machine;
  var s3: machine;
  var driver: machine;

  start state Idle {
    entry (init: (epoch: int, manifest: machine, s3: machine, driver: machine)) {
      epoch = init.epoch;
      manifest = init.manifest;
      s3 = init.s3;
      driver = init.driver;
    }
    on eDoReset do (p: (localFloor: int, newUid: int)) {
      send manifest, eMRSetFloor, (from = this, floor = p.localFloor, isReset = true);
      receive { case eMRAck: { } }
      send manifest, eMRAddEntry, (from = this, key = (offset = p.localFloor, uid = p.newUid));
      receive { case eMRAck: { } }
      send s3, eS3Put, (from = this, obj = (offset = p.localFloor, uid = p.newUid, epoch = epoch));
      receive { case eS3Ack: { } }
      send driver, eResetDone;
    }
  }
}
