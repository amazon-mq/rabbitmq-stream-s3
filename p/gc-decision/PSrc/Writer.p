/* The writer / reset orchestrator (rabbitmq_stream_s3_replica_reader). By the
   time the dangerous CLI sweep runs, the committed remote-tier-ahead reset is
   durable: the new (higher) epoch is committed in Khepri, the floor is lowered in
   the authoritative manifest, and the live fragment is re-tiered to S3 with a
   fresh UID below the old floor.

   The writer announces the AUTHORITATIVE manifest state to the monitors: the
   re-tiered live (offset, uid) becomes referenced. The only modeled uncertainty
   is whether the lagging cache the sweep reads has learned of the reset; that
   propagation (eMRSync) is driven by the test driver, never by the writer. */
machine Writer {
  var db: machine;
  var s3: machine;
  var driver: machine;

  start state Idle {
    entry (init: (db: machine, s3: machine, driver: machine)) {
      db = init.db;
      s3 = init.s3;
      driver = init.driver;
    }
    on eDoReset do (p: (from: machine, newFloor: int, newEpoch: int, retierOffset: int, retierUid: int)) {
      /* Commit the new epoch (the quorum truth a sweep's get_consistent sees). */
      send db, eSetCommittedEpoch, (from = this, epoch = p.newEpoch);
      receive { case eDbAck: { } }
      /* Re-tier the live fragment below the lowered floor with a fresh UID, and
         announce it as referenced by the committed manifest. */
      announce eObjectReferenced, (kind = DATA, offset = p.retierOffset, uid = p.retierUid, epoch = 0);
      send s3, eS3Put, (from = this, obj = (kind = DATA, offset = p.retierOffset, uid = p.retierUid, epoch = 0));
      receive { case eS3Ack: { } }
      send driver, eResetDone;
    }
  }
}
