/* The writer / reset orchestrator (rabbitmq_stream_s3_replica_reader). By the
   time the dangerous CLI sweep runs, the reset is already COMMITTED: the new
   (higher) epoch is in Khepri, the floor is lowered in the authoritative
   manifest, and the live fragment has been re-tiered to S3 with a fresh UID.

   The only modeled uncertainty is whether the lagging replica cache the sweep
   reads has learned of it - that propagation (eMRSync) is driven by the test
   driver so a scenario can drop it (the bug) or deliver it (anti-vacuity), and
   the explore driver can race it against the sweep.

   The writer announces the AUTHORITATIVE manifest state to the monitors: the
   lowered (reset) floor and the re-tiered live (offset, uid). The cache is never
   the source of truth for the spec. */
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
    on eDoReset do (p: (from: machine, newFloor: int, newUid: int, newEpoch: int)) {
      /* Commit the new epoch (the quorum truth a sweep's get_consistent sees). */
      send db, eSetCommittedEpoch, (from = this, epoch = p.newEpoch);
      receive { case eDbAck: { } }
      /* Lower the authoritative floor (labeled reset) before referencing the
         re-tiered object, then put it into S3. */
      announce eFloorChanged, (newFloor = p.newFloor, isReset = true);
      announce eObjectReferenced, (offset = p.newFloor, uid = p.newUid);
      send s3, eS3Put, (from = this, key = (offset = p.newFloor, uid = p.newUid));
      receive { case eS3Ack: { } }
      send driver, eResetDone;
    }
  }
}
