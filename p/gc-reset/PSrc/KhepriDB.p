/* Durable consensus layer (rabbitmq_stream_s3_db). Holds the committed epoch and
   answers strongly-consistent reads. Modeled as a single serialized actor; a
   real partition would surface as ok = false (no quorum), which a sweep treats
   as fail-closed. The committed epoch is monotonic. */
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
