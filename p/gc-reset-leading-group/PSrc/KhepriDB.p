/* Durable consensus layer (rabbitmq_stream_s3_db). Holds the committed epoch and
   answers strongly-consistent reads. The committed epoch is monotonic. Not the
   focus here (the seam is on the offset/group axis), but the sweep still takes a
   consistent read at snapshot, exactly as build_lookup does. */
machine KhepriDB {
  var committedEpoch: int;

  start state Serving {
    entry (init: (epoch: int)) {
      committedEpoch = init.epoch;
    }
    on eGetConsistent do (p: (from: machine)) {
      send p.from, eEpochResult, (ok = true, epoch = committedEpoch);
    }
  }
}
