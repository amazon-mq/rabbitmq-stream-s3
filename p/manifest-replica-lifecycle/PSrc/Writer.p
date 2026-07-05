/* The writer-side broadcaster (rabbitmq_stream_s3_replica_reader). It is the
   authoritative source of committed floors: every floor a replica may legitimately
   serve was committed here first. Floors advance under a monotonic (epoch, sn)
   stamp - the stream's Khepri payload_version and writer epoch - exactly as the
   shipped sequence numbering works.

   Syncs to a replica are fire-and-forget casts. Routing the emission through the
   writer (rather than the driver) models the broadcaster and lets a test replay
   an OLD committed (floor, epoch, sn) to reorder a sync behind a newer one. The
   writer NEVER updates a replica's cache directly; that propagation is the
   modeled uncertainty (dropped, delayed, or raced against a member exit). */
machine Writer {
  /* The writer's persisted floor per stream (persisted_manifest(Core)): the tuple
     a reconcile or resync re-sends. Recorded on every commit. */
  var committed: map[StreamId, (floor: int, epoch: int, sn: int)];

  start state Serving {
    on eDoCommit do (p: (from: machine, stream: StreamId, floor: int, epoch: int, sn: int)) {
      committed[p.stream] = (floor = p.floor, epoch = p.epoch, sn = p.sn);
      announce eFloorCommitted, (floor = p.floor, epoch = p.epoch, sn = p.sn);
      send p.from, eCommitAck;
    }

    /* reconcile_replicas: the writer discovers a visible member and syncs the
       node with its persisted floor, INDEPENDENT of register_acceptor. This
       cast can be delivered to the replica before that node's context exists. */
    on eReconcile do (p: (target: machine, stream: StreamId)) {
      var c: (floor: int, epoch: int, sn: int);
      if (p.stream in committed) {
        c = committed[p.stream];
        send p.target, eMRSync,
          (stream = p.stream, floor = c.floor, epoch = c.epoch, sn = c.sn, writerNode = 0);
      }
    }

    /* {resync, Node}: a replica requested a fresh sync; re-send the persisted
       floor. This is how A1' recovers a dropped premature sync. */
    on eResync do (p: (from: machine, stream: StreamId)) {
      var c: (floor: int, epoch: int, sn: int);
      if (p.stream in committed) {
        c = committed[p.stream];
        send p.from, eMRSync,
          (stream = p.stream, floor = c.floor, epoch = c.epoch, sn = c.sn, writerNode = 0);
      }
    }

    on eEmitSync do (p: (from: machine, target: machine, stream: StreamId,
                         floor: int, epoch: int, sn: int)) {
      send p.target, eMRSync,
        (stream = p.stream, floor = p.floor, epoch = p.epoch, sn = p.sn, writerNode = 0);
      send p.from, eEmitAck;
    }

    /* The eMREdit counterpart to eEmitSync: emit a sequenced edit instead of a
       full-manifest reset. */
    on eEmitEdit do (p: (from: machine, target: machine, stream: StreamId,
                         floor: int, epoch: int, sn: int)) {
      send p.target, eMREdit,
        (stream = p.stream, floor = p.floor, epoch = p.epoch, sn = p.sn, writerNode = 0);
      send p.from, eEmitAck;
    }

    /* Synchronous barrier: flush prior writer->target sync casts by riding the
       FIFO behind them, then confirm to the caller. */
    on eBarrier do (p: (from: machine, target: machine)) {
      send p.target, eBarrierPing, (from = this,);
      receive { case eBarrierPong: { } }
      send p.from, eBarrierAck;
    }

    /* register_acceptor cast: the replica reader registers the node and,
       in the same handler, broadcasts the persisted manifest back as a startup
       sync (sync_manifest on {register_acceptor, Node}). The cast carries the
       committed (floor, epoch, sn) the driver wrote, modeling persisted_manifest
       at register time. No ack: this is the writer-originated sync that races the
       node's local context registration. */
    on eRegisterAcceptor do (p: (replica: machine, stream: StreamId,
                                 floor: int, epoch: int, sn: int)) {
      send p.replica, eMRSync,
        (stream = p.stream, floor = p.floor, epoch = p.epoch, sn = p.sn, writerNode = 0);
    }
  }
}
