/* Durable consensus layer (rabbitmq_stream_s3_db). Holds the committed epoch and
   answers strongly-consistent reads. The committed epoch is monotonic and is the
   authoritative truth a quorum read returns; the per-node manifest cache may lag
   it. The reset commits the new (higher) epoch here BEFORE the sync that updates
   the lagging cache, which is exactly why the cache can be behind. */
machine KhepriDB {
  var committedEpoch: int;

  start state Serving {
    entry (init: (epoch: int)) {
      committedEpoch = init.epoch;
    }
    on eGetConsistent do (p: (from: machine)) {
      send p.from, eEpochResult, (ok = true, epoch = committedEpoch);
    }
    on eSetCommittedEpoch do (p: (from: machine, epoch: int)) {
      committedEpoch = p.epoch;
      send p.from, eDbAck;
    }
  }
}
