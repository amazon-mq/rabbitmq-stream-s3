/* Shared types and events for the manifest-replica lifecycle seam.

   This model verifies the per-node manifest replica process
   (rabbitmq_stream_s3_manifest_replica) and, specifically, the cleanup-on-
   reader-exit logic landed in commit cc50092 ("manifest replica: Clean up when
   a replica (reader) exits").

   The replica process holds three kinds of per-stream state in lockstep:

     - contexts : the osiris log context, registered by the acceptor hook when a
                  reader (osiris member) starts. The member pid is monitored.
     - seqs     : the last-applied {sn, epoch, writer_node} used for gap
                  detection on the broadcast sync stream.
     - cache    : the ETS row {manifest/floor, epoch} that GC and consumers read.

   osiris has NO terminate/delete hook, so the replica MONITORS the member that
   registered a context and, on its DOWN, releases all three (release_stream/2).
   This is the only thing that reclaims per-node state when a replica moves off
   the node or the stream is deleted; without it the metadata accretes forever
   (the bug cc50092 set out to fix).

   The model abstracts the osiris member to its MONITOR REFERENCE (an int mref):
   the replica's only handle on a member IS the monitor it holds, so this is
   faithful. The driver plays "the world that starts and kills members": a
   register returns the mref the replica assigned, and the driver later signals a
   member exit by delivering eMemberDown with that mref (the modeled DOWN).

   Syncs from the writer are fire-and-forget casts (eMRSync) that the scheduler
   may drop, delay, or reorder relative to a member exit - the same multi-node
   stale-cache substrate as ../gc-reset-multinode. is_stale_sync gates them:
   epoch dominates sequence, so a delayed lower-epoch sync is rejected. */

/* A stream id (small ints). */
type StreamId = int;

/* ---- Manifest replica (the system under test) ---- */

/* register_replica_context/5: the acceptor hook registers an osiris member's
   context. Synchronous call; the reply carries the assigned monitor ref. */
event eRegister: (from: machine, stream: StreamId);
event eRegisterAck: (mref: int);

/* The modeled member DOWN: the osiris member that owned a context exited. The
   replica releases the stream iff the mref is still live in its reverse index
   (a superseded mref - from a member that re-registered - is ignored, modeling
   demonitor(OldRef, [flush])). Acked so a driver can sequence a post-exit sync. */
event eMemberDown: (from: machine, mref: int);
event eDownAck;

/* sync/5: a full-manifest sync from the writer, fire-and-forget (cast). Carries
   the writer's epoch and sequence; applied only if not is_stale_sync. */
event eMRSync: (stream: StreamId, floor: int, epoch: int, sn: int, writerNode: int);

/* put_manifest/3: the WRITER node writes its own cache row directly (no osiris
   member to monitor); released explicitly by forget/1. Synchronous. */
event ePutManifest: (from: machine, stream: StreamId, floor: int, epoch: int, sn: int);
event ePutAck;

/* forget/1: explicit release of all per-stream state (writer-node teardown). */
event eForget: (from: machine, stream: StreamId);
event eForgetAck;

/* Quiesce barrier and held-state query. eBarrierPing flushes the writer->replica
   FIFO so all in-flight syncs are applied before the checkpoint reads state. */
event eBarrierPing: (from: machine);
event eBarrierPong;
event eQueryHeld: (from: machine);
event eHeldResult: set[StreamId];

/* ---- Writer / replica_reader (authoritative source) ---- */

/* Commit a new authoritative floor at (epoch, sn) for a stream; announces it to
   the monitors as committed truth AND records it as the writer's persisted floor
   for that stream, so a later reconcile/resync can re-send it (persisted_manifest).
   The writer is the only source of committed floors. */
event eDoCommit: (from: machine, stream: StreamId, floor: int, epoch: int, sn: int);
event eCommitAck;

/* Emit a sync (cast) to a replica for a chosen committed (floor, epoch, sn).
   Routing it through the writer models rabbitmq_stream_s3_replica_reader as the
   broadcaster; the driver can replay an OLD committed tuple to reorder. */
event eEmitSync: (from: machine, target: machine, stream: StreamId,
                  floor: int, epoch: int, sn: int);
event eEmitAck;

/* Synchronous barrier through the writer: flushes prior writer->replica casts. */
event eBarrier: (from: machine, target: machine);
event eBarrierAck;

/* register_acceptor: the acceptor hook tells the writer's replica reader to
   register this node (rabbitmq_stream_s3_hooks: gen_server:cast({register_acceptor,
   node()})). The writer responds by IMMEDIATELY emitting a startup sync of the
   persisted manifest (rabbitmq_stream_s3_replica_reader handle_cast
   {register_acceptor, Node} -> sync_manifest, :435,:771). A fire-and-forget cast:
   no ack, modeling attach decoupling #1 - a sync can be in flight to a node
   BEFORE that node's local manifest_replica context exists. */
event eRegisterAcceptor: (replica: machine, stream: StreamId, floor: int, epoch: int, sn: int);

/* reconcile_replicas: the SECOND, writer-driven startup sync trigger
   (rabbitmq_stream_s3_replica_reader.erl handle_cast(reconcile_replicas,...),
   ~:467-487). The periodic reconciler discovers replica NODES from
   osiris_writer:query_replication_state and syncs each via register_replica/2
   (~:719-737, which calls sync_manifest with persisted_manifest). This is driven
   by the osiris MEMBER becoming visible to the writer, NOT by the node's
   register_acceptor and NOT by whether the node's local manifest-replica CONTEXT
   has registered yet. So a reconcile sync can arrive at a node BEFORE its local
   context exists, and unlike the acceptor-reply sync this trigger is on the WRITER
   side, so the node's own attach ordering (A1) cannot prevent it. A fire-and-forget
   cast to a target replica; the writer re-sends its persisted committed floor. */
event eReconcile: (target: machine, stream: StreamId);

/* request_resync/2 (rabbitmq_stream_s3_manifest_replica.erl:508): the replica
   casts {resync, node()} to the writer's replica reader (via the registry),
   which answers with a fresh sync of persisted_manifest (the {resync, Node}
   handler, rabbitmq_stream_s3_replica_reader.erl:455-466 -> sync_manifest). A1'
   (resyncOnRegister) wires this into context registration: when the manifest
   replica registers a context, it requests a resync, so the writer re-sends the
   manifest even if an earlier premature (reconcile or acceptor-reply) sync was
   dropped. from is the requesting replica (the sync target). */
event eResync: (from: machine, stream: StreamId);

/* The attach node signals the driver it finished registering, carrying the
   context's monitor ref so the driver can later deliver the member DOWN. */
event eAttachDone: (mref: int);

/* Internal yield token. After registering with the writer (WriterFirst order),
   the attach node relinquishes control via a self-send + receive, so the
   scheduler MAY run the writer and deliver its startup sync to the replica
   before the local context registration - the modeled startup race. */
event eProceed;

/* ---- Spec events (announced to monitors) ---- */

/* A reader (osiris member) the driver considers live came up / exited. */
event eReaderUp: StreamId;
event eReaderDown: StreamId;

/* The replica wrote a cache row at (floor, epoch, sn). */
event eCacheUpdated: (stream: StreamId, floor: int, epoch: int, sn: int);

/* The writer committed an authoritative floor at (floor, epoch, sn). */
event eFloorCommitted: (floor: int, epoch: int, sn: int);

/* Quiesce checkpoint: the held-state snapshot is bracketed by begin/end and one
   eHeld per stream the replica still holds any state for. */
event eQuiesceBegin;
event eHeld: StreamId;
event eQuiesceEnd;

/* Lexicographic staleness: (epoch, sn) strictly older than (epoch0, seq0).
   Mirrors is_stale_sync/3: epoch dominates so a higher epoch is never stale. */
fun StaleLex(epoch: int, sn: int, epoch0: int, seq0: int): bool {
  if (epoch < epoch0) { return true; }
  if (epoch > epoch0) { return false; }
  return sn < seq0;
}
