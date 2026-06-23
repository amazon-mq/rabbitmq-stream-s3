/* The writer / reset orchestrator (rabbitmq_stream_s3_replica_reader) plus the
   normal forward-retention floor advance.

   eDoReset models restart_at_local_floor: the remote-tier-ahead reset rebuilds
   the manifest from the local log floor, LOWERING first_offset and installing a
   fresh leading group (a GROUP object with a fresh UID - rabbitmq_stream_s3:uid/0
   is random per upload, so the new leading group never reuses the snapshot
   leading group's key). The old leading group is superseded.

   eAdvanceFloor models ordinary retention advancing first_offset FORWARD
   (isReset = false). After the reset's leading group is in place at the reset
   floor, retention advances the floor past it, so the leading group now straddles
   the floor (partial expiry) while still being the live referenced leading
   group - exactly the state the carve-out exists to protect. */
machine Writer {
  var epoch: int;
  var manifest: machine;
  var s3: machine;

  start state Idle {
    entry (init: (epoch: int, manifest: machine, s3: machine)) {
      epoch = init.epoch;
      manifest = init.manifest;
      s3 = init.s3;
    }
    on eDoReset do (p: (from: machine, newFloor: int, newUid: int, oldLeading: ObjKey)) {
      var nlg: ObjKey;
      nlg = (offset = p.newFloor, uid = p.newUid);
      /* Lower the floor first (the reset's atomic prefix), then supersede the old
         leading group with the freshly re-tiered one. */
      send manifest, eMRSetFloor, (from = this, floor = p.newFloor, isReset = true);
      receive { case eMRAck: { } }
      send manifest, eMRRemoveEntry, (from = this, key = p.oldLeading);
      receive { case eMRAck: { } }
      send manifest, eMRAddEntry, (from = this, key = nlg);
      receive { case eMRAck: { } }
      send manifest, eMRSetLeading, (from = this, leadingKey = nlg, hasLeading = true, skipGroups = false);
      receive { case eMRAck: { } }
      send s3, eS3Put, (from = this, obj = (offset = p.newFloor, uid = p.newUid, kind = GROUP, epoch = epoch));
      receive { case eS3Ack: { } }
      send p.from, eResetDone;
    }
    on eAdvanceFloor do (p: (from: machine, newFloor: int)) {
      /* Forward retention: the leading group is unchanged, but now sits below the
         floor (partial expiry). */
      send manifest, eMRSetFloor, (from = this, floor = p.newFloor, isReset = false);
      receive { case eMRAck: { } }
      send p.from, eAdvanceDone;
    }
  }
}
