/* Test drivers for the manifest-replica lifecycle seam.

   Each load-bearing guard ships two test cases: a guarded one that MUST HOLD and
   an unguarded one that MUST FAIL with a named counterexample. The failing runs
   are the proof the monitors have teeth.

     G1  member-DOWN cleanup     tcCleanupGuarded   / tcCleanupUnguarded   (NOLEAK)
     G2  is_stale_sync           tcStaleSyncGuarded / tcStaleSyncUnguarded (STALEFLOOR)
     G3  re-register repoint      tcReregisterGuarded/ tcReregisterUnguarded(RETAIN)
     liveness  convergence       tcConvergenceGuarded/tcConvergenceStuck   (ReplicaConverges)

   tcForgetReleasesWriterRow exercises the writer-node forget/1 path. tcExplore
   races register/sync/restart/exit across two streams with every guard on.

   GAP (headline finding): with every SHIPPED guard on, a sync that arrives after
   a member DOWN re-creates a cache row for a stream the cleanup already released,
   stranding it forever (no monitor will ever fire again). tcSyncAfterExitStrands
   demonstrates it; tcSyncAfterExitFixed shows a proposed guard closing it. */

fun Register(self: machine, replica: machine, stream: StreamId): int {
  var mref: int;
  send replica, eRegister, (from = self, stream = stream);
  receive { case eRegisterAck: (r: (mref: int)) { mref = r.mref; } }
  announce eReaderUp, stream;
  return mref;
}

fun MemberDown(self: machine, replica: machine, stream: StreamId, mref: int) {
  announce eReaderDown, stream;
  send replica, eMemberDown, (from = self, mref = mref);
  receive { case eDownAck: { } }
}

fun Commit(self: machine, writer: machine, stream: StreamId, floor: int, epoch: int, sn: int) {
  send writer, eDoCommit, (from = self, stream = stream, floor = floor, epoch = epoch, sn = sn);
  receive { case eCommitAck: { } }
}

/* Emit a sync cast (not yet applied); call Barrier to flush it. */
fun EmitSync(self: machine, writer: machine, replica: machine,
             stream: StreamId, floor: int, epoch: int, sn: int) {
  send writer, eEmitSync,
    (from = self, target = replica, stream = stream, floor = floor, epoch = epoch, sn = sn);
  receive { case eEmitAck: { } }
}

fun Barrier(self: machine, writer: machine, replica: machine) {
  send writer, eBarrier, (from = self, target = replica);
  receive { case eBarrierAck: { } }
}

fun PutManifest(self: machine, replica: machine, stream: StreamId, floor: int, epoch: int, sn: int) {
  send replica, ePutManifest, (from = self, stream = stream, floor = floor, epoch = epoch, sn = sn);
  receive { case ePutAck: { } }
}

fun Forget(self: machine, replica: machine, stream: StreamId) {
  send replica, eForget, (from = self, stream = stream);
  receive { case eForgetAck: { } }
}

/* Quiesce checkpoint: flush is the caller's responsibility (Barrier first). Snap
   the replica's held set and bracket it with begin/end for the monitor. */
fun Quiesce(self: machine, replica: machine) {
  var held: set[StreamId];
  var s: StreamId;
  send replica, eQueryHeld, (from = self,);
  receive { case eHeldResult: (h: set[StreamId]) { held = h; } }
  announce eQuiesceBegin;
  foreach (s in held) { announce eHeld, s; }
  announce eQuiesceEnd;
}

fun NewReplica(cleanup: bool, stale: bool, repoint: bool, gapFix: bool,
               resync: bool, writer: machine): machine {
  return new ManifestReplica((cleanupEnabled = cleanup, staleSyncGuardEnabled = stale,
                              repointEnabled = repoint, syncRequiresContextEnabled = gapFix,
                              resyncOnRegisterEnabled = resync, writer = writer));
}

/* ---- G1: member-DOWN cleanup ---- */

/* A reader registers, syncs, then exits. With cleanup on the replica holds
   nothing afterwards; with it off the entry is stranded. */
fun RunCleanup(self: machine, cleanup: bool) {
  var replica: machine;
  var writer: machine;
  var mref: int;
  writer = new Writer();
  replica = NewReplica(cleanup, true, true, false, false, writer);
  Commit(self, writer, 0, 1000, 1, 1);
  mref = Register(self, replica, 0);
  EmitSync(self, writer, replica, 0, 1000, 1, 1);
  Barrier(self, writer, replica);
  MemberDown(self, replica, 0, mref);
  Barrier(self, writer, replica);
  Quiesce(self, replica);
}

machine DriverCleanupGuarded { start state Init { entry { RunCleanup(this, true); } } }
machine DriverCleanupUnguarded { start state Init { entry { RunCleanup(this, false); } } }

/* ---- G2: is_stale_sync ---- */

/* Apply the reset (epoch 2), then a delayed earlier-epoch sync arrives. With the
   guard on it is dropped; with it off it rolls the cache backward. */
fun RunStaleSync(self: machine, stale: bool) {
  var replica: machine;
  var writer: machine;
  var mref: int;
  writer = new Writer();
  replica = NewReplica(true, stale, true, false, false, writer);
  Commit(self, writer, 0, 1000, 1, 1);
  Commit(self, writer, 0, 850, 2, 2);
  mref = Register(self, replica, 0);
  EmitSync(self, writer, replica, 0, 1000, 1, 1);
  Barrier(self, writer, replica);
  EmitSync(self, writer, replica, 0, 850, 2, 2);
  Barrier(self, writer, replica);
  /* The delayed, reordered sync from the deposed lower epoch. */
  EmitSync(self, writer, replica, 0, 1000, 1, 1);
  Barrier(self, writer, replica);
  MemberDown(self, replica, 0, mref);
  Barrier(self, writer, replica);
  Quiesce(self, replica);
}

machine DriverStaleGuarded { start state Init { entry { RunStaleSync(this, true); } } }
machine DriverStaleUnguarded { start state Init { entry { RunStaleSync(this, false); } } }

/* ---- G3: re-register repoint ---- */

/* A member restart re-registers the stream, then the OLD member exits. With the
   repoint on its DOWN is ignored; with it off the old DOWN evicts the live
   context registered by the new member. */
fun RunReregister(self: machine, repoint: bool) {
  var replica: machine;
  var writer: machine;
  var mrefOld: int;
  var mrefNew: int;
  writer = new Writer();
  replica = NewReplica(true, true, repoint, false, false, writer);
  Commit(self, writer, 0, 1000, 1, 1);
  mrefOld = Register(self, replica, 0);
  EmitSync(self, writer, replica, 0, 1000, 1, 1);
  Barrier(self, writer, replica);
  /* Member restart: re-register the same stream with a new incarnation. */
  mrefNew = Register(self, replica, 0);
  /* The old incarnation's DOWN must not evict the live new context. */
  MemberDown(self, replica, 0, mrefOld);
  Barrier(self, writer, replica);
  Quiesce(self, replica);
}

machine DriverReregisterGuarded { start state Init { entry { RunReregister(this, true); } } }
machine DriverReregisterUnguarded { start state Init { entry { RunReregister(this, false); } } }

/* ---- liveness: convergence ---- */

/* A live replica receives every sync and catches up to the latest committed
   floor. The hot Lagging state is entered on each commit and left on each sync. */
machine DriverConvergenceGuarded {
  start state Init {
    entry {
      var replica: machine;
      var writer: machine;
      var mref: int;
      writer = new Writer();
      replica = NewReplica(true, true, true, false, false, writer);
      mref = Register(this, replica, 0);
      Commit(this, writer, 0, 1000, 1, 1);
      EmitSync(this, writer, replica, 0, 1000, 1, 1);
      Barrier(this, writer, replica);
      Commit(this, writer, 0, 850, 2, 2);
      EmitSync(this, writer, replica, 0, 850, 2, 2);
      Barrier(this, writer, replica);
    }
  }
}

/* The sync is never delivered, so the replica stays behind the committed floor:
   the hot Lagging state is never left - a liveness violation. */
machine DriverConvergenceStuck {
  start state Init {
    entry {
      var replica: machine;
      var writer: machine;
      var mref: int;
      writer = new Writer();
      replica = NewReplica(true, true, true, false, false, writer);
      mref = Register(this, replica, 0);
      Commit(this, writer, 0, 1000, 1, 1);
    }
  }
}

/* ---- writer-node forget/1 path ---- */

/* The writer node writes its own cache row with put_manifest (no member to
   monitor) and releases it explicitly via forget/1 on teardown. */
machine DriverForget {
  start state Init {
    entry {
      var replica: machine;
      var writer: machine;
      writer = new Writer();
      replica = NewReplica(true, true, true, false, false, writer);
      Commit(this, writer, 0, 1000, 1, 1);
      PutManifest(this, replica, 0, 1000, 1, 1);
      Forget(this, replica, 0);
      Quiesce(this, replica);
    }
  }
}

/* ---- GAP: sync after exit ---- */

/* Every shipped guard on. A sync delivered AFTER the member DOWN (causally after
   the cleanup released the stream) re-creates the cache row and gap sequence -
   with no context, so no monitor will ever clean it up. gapFix selects the
   proposed guard: ignore a sync for a stream with no live context. */
fun RunSyncAfterExit(self: machine, gapFix: bool) {
  var replica: machine;
  var writer: machine;
  var mref: int;
  writer = new Writer();
  replica = NewReplica(true, true, true, gapFix, false, writer);
  Commit(self, writer, 0, 1000, 1, 1);
  mref = Register(self, replica, 0);
  EmitSync(self, writer, replica, 0, 1000, 1, 1);
  Barrier(self, writer, replica);
  /* Synchronous: the cleanup has fully released the stream before we proceed. */
  MemberDown(self, replica, 0, mref);
  /* The straggler sync, now strictly after the release. */
  EmitSync(self, writer, replica, 0, 1000, 1, 1);
  Barrier(self, writer, replica);
  Quiesce(self, replica);
}

machine DriverSyncAfterExitStrands { start state Init { entry { RunSyncAfterExit(this, false); } } }
machine DriverSyncAfterExitFixed { start state Init { entry { RunSyncAfterExit(this, true); } } }

/* ---- STARTUP RACE: attach ordering vs the syncRequiresContext guard ---- */

/* The acceptor attach node (rabbitmq_stream_s3_hooks on_init(acceptor, ...)).
   It performs two registrations whose ORDER is the new axis:

     - register the LOCAL manifest-replica context (eRegister), and
     - register the node with the WRITER (eRegisterAcceptor), which makes the
       writer immediately broadcast a startup sync back to the replica.

   contextFirst selects the order:

     ContextFirst (A1 fix, the discovery path attach_replica/1 order, hooks.erl
       :350-358): register the context BEFORE the writer, so the context exists
       before any sync can be emitted - the startup sync always lands on a live
       context.
     WriterFirst (the SHIPPED acceptor order, hooks.erl:83-95): register with the
       writer FIRST, then yield, then register the local context. The writer's
       startup sync can be delivered to the replica BEFORE the local context
       exists - the modeled race. Paired with the syncRequiresContext guard, that
       sync is DROPPED and (the writer's register is idempotent, so it never
       re-syncs) the cache may stay empty forever.

   A barrier from THIS machine (the same sender as the eRegisterAcceptor cast, so
   FIFO orders the ping behind the startup sync) flushes the sync before the node
   reports done, so the driver observes a settled cache. */
machine AttachNode {
  start state Init {
    entry (p: (parent: machine, writer: machine, replica: machine, stream: StreamId,
               floor: int, epoch: int, sn: int, contextFirst: bool)) {
      var mref: int;
      if (p.contextFirst) {
        send p.replica, eRegister, (from = this, stream = p.stream);
        receive { case eRegisterAck: (r: (mref: int)) { mref = r.mref; } }
        announce eReaderUp, p.stream;
        send p.writer, eRegisterAcceptor,
          (replica = p.replica, stream = p.stream, floor = p.floor, epoch = p.epoch, sn = p.sn);
      } else {
        send p.writer, eRegisterAcceptor,
          (replica = p.replica, stream = p.stream, floor = p.floor, epoch = p.epoch, sn = p.sn);
        /* Yield so the writer may emit (and the replica may apply/drop) the
           startup sync before the local context registration below. */
        send this, eProceed;
        receive { case eProceed: { } }
        send p.replica, eRegister, (from = this, stream = p.stream);
        receive { case eRegisterAck: (r: (mref: int)) { mref = r.mref; } }
        announce eReaderUp, p.stream;
      }
      send p.writer, eBarrier, (from = this, target = p.replica);
      receive { case eBarrierAck: { } }
      send p.parent, eAttachDone, (mref = mref,);
    }
  }
}

/* Commit a floor, attach a node with the chosen ordering (every shipped guard
   AND the syncRequiresContext guard on), then move the member off the node and
   let a straggler sync arrive after the DOWN (decoupling #2: the writer keeps
   broadcasting). With ContextFirst the startup sync always lands so the cache
   converges and nothing strands; with WriterFirst the guard can drop the startup
   sync, leaving the cache permanently empty - a convergence violation. */
fun RunStartupRace(self: machine, contextFirst: bool) {
  var replica: machine;
  var writer: machine;
  var attach: machine;
  var mref: int;
  writer = new Writer();
  replica = NewReplica(true, true, true, true, false, writer);
  Commit(self, writer, 0, 1000, 1, 1);
  attach = new AttachNode((parent = self, writer = writer, replica = replica,
                           stream = 0, floor = 1000, epoch = 1, sn = 1,
                           contextFirst = contextFirst));
  receive { case eAttachDone: (r: (mref: int)) { mref = r.mref; } }
  /* The member moves off this node; the writer keeps targeting it (singleton
     still live), so a straggler sync arrives after the DOWN. */
  MemberDown(self, replica, 0, mref);
  EmitSync(self, writer, replica, 0, 1000, 1, 1);
  Barrier(self, writer, replica);
  Quiesce(self, replica);
}

machine DriverStartupRaceWriterFirst { start state Init { entry { RunStartupRace(this, false); } } }
machine DriverStartupRaceContextFirst { start state Init { entry { RunStartupRace(this, true); } } }

/* ---- RECONCILE PATH: the writer-driven startup sync trigger ---- */

/* The node's local context registration in isolation - a clean, context-first
   (A1-consistent) event with NO register_acceptor. It races the writer's
   reconcile-path sync, which is delivered to the replica from a different sender
   (the writer), so the scheduler may process either first. */
machine ContextRegistrar {
  start state Init {
    entry (p: (parent: machine, replica: machine, stream: StreamId)) {
      var mref: int;
      send p.replica, eRegister, (from = this, stream = p.stream);
      receive { case eRegisterAck: (r: (mref: int)) { mref = r.mref; } }
      announce eReaderUp, p.stream;
      send p.parent, eAttachDone, (mref = mref,);
    }
  }
}

/* Every shipped guard AND A2 (syncRequiresContext) on. The reconcile-path sync
   (writer-driven, triggered by the member becoming visible) races the node's
   local context registration: the eReconcile cast makes the writer send an eMRSync
   to the replica, WHILE a ContextRegistrar concurrently sends eRegister to the same
   replica. The two arrive from different senders, so the scheduler interleaves them
   BOTH ways - a genuine race the reconcile sync can win (sync before register).
   This is the trigger the attach-ordering fix (A1) CANNOT reach: it is on the
   writer side, unordered w.r.t. context registration.

   resync selects A1' (resyncOnRegister). OFF: when the reconcile sync wins the
   race it is dropped by A2 (no context yet) and never re-sent, so the cache stays
   empty forever - a ReplicaConverges violation, proving A1 is insufficient for the
   writer-driven trigger. ON: registering the context requests a resync, so the
   writer re-sends the manifest and the cache converges. */
fun RunReconcileRace(self: machine, resync: bool) {
  var replica: machine;
  var writer: machine;
  var reg: machine;
  var mref: int;
  writer = new Writer();
  Commit(self, writer, 0, 1000, 1, 1);
  replica = NewReplica(true, true, true, true, resync, writer);
  /* Context registration and the reconcile-path sync are launched unordered. */
  reg = new ContextRegistrar((parent = self, replica = replica, stream = 0));
  send writer, eReconcile, (target = replica, stream = 0);
  receive { case eAttachDone: (r: (mref: int)) { mref = r.mref; } }
  Barrier(self, writer, replica);
  Quiesce(self, replica);
}

machine DriverReconcileRaceNoResync { start state Init { entry { RunReconcileRace(this, false); } } }
machine DriverReconcileRaceResync   { start state Init { entry { RunReconcileRace(this, true); } } }

/* ---- exploration ---- */

/* Race register / sync / restart / exit across two streams with every guard and
   the gap fix on. A sync may be in flight when a member exits; no interleaving
   may leave the replica's held set out of step with the live readers. */
machine DriverExplore {
  var replica: machine;
  var writer: machine;
  start state Init {
    entry {
      writer = new Writer();
      replica = NewReplica(true, true, true, true, false, writer);
      /* Commit each stream's floors up front so syncs replay committed tuples. */
      Commit(this, writer, 0, 1010, 1, 1);
      Commit(this, writer, 0, 1005, 2, 2);
      Commit(this, writer, 1, 2010, 1, 1);
      Commit(this, writer, 1, 2005, 2, 2);
      ChurnStream(0, 1010, 1005);
      ChurnStream(1, 2010, 2005);
      Barrier(this, writer, replica);
      Quiesce(this, replica);
    }
  }

  fun ChurnStream(stream: StreamId, floorA: int, floorB: int) {
    var mref: int;
    var mref2: int;
    mref = Register(this, replica, stream);
    if ($) {
      /* A sync may be in flight when the member exits (no Barrier here). */
      EmitSync(this, writer, replica, stream, floorA, 1, 1);
    }
    if ($) {
      /* Member restart: re-register, retiring the old incarnation. */
      mref2 = Register(this, replica, stream);
      MemberDown(this, replica, stream, mref);
      if ($) {
        EmitSync(this, writer, replica, stream, floorB, 2, 2);
      }
      if ($) {
        MemberDown(this, replica, stream, mref2);
      }
    } else {
      if ($) {
        /* Possibly-reordered second sync; the stale guard must drop a stale one. */
        EmitSync(this, writer, replica, stream, floorB, 2, 2);
      }
      if ($) {
        MemberDown(this, replica, stream, mref);
      }
    }
  }
}

/* ---- test declarations ---- */

test tcCleanupGuarded [main = DriverCleanupGuarded]:
  assert ReplicaStateMatchesReaders, NoStaleFloorServed in
  { DriverCleanupGuarded, ManifestReplica, Writer };

test tcCleanupUnguarded [main = DriverCleanupUnguarded]:
  assert ReplicaStateMatchesReaders in
  { DriverCleanupUnguarded, ManifestReplica, Writer };

test tcStaleSyncGuarded [main = DriverStaleGuarded]:
  assert NoStaleFloorServed, ReplicaStateMatchesReaders in
  { DriverStaleGuarded, ManifestReplica, Writer };

test tcStaleSyncUnguarded [main = DriverStaleUnguarded]:
  assert NoStaleFloorServed in
  { DriverStaleUnguarded, ManifestReplica, Writer };

test tcReregisterGuarded [main = DriverReregisterGuarded]:
  assert ReplicaStateMatchesReaders in
  { DriverReregisterGuarded, ManifestReplica, Writer };

test tcReregisterUnguarded [main = DriverReregisterUnguarded]:
  assert ReplicaStateMatchesReaders in
  { DriverReregisterUnguarded, ManifestReplica, Writer };

test tcConvergenceGuarded [main = DriverConvergenceGuarded]:
  assert ReplicaConverges in
  { DriverConvergenceGuarded, ManifestReplica, Writer };

test tcConvergenceStuck [main = DriverConvergenceStuck]:
  assert ReplicaConverges in
  { DriverConvergenceStuck, ManifestReplica, Writer };

test tcForgetReleasesWriterRow [main = DriverForget]:
  assert ReplicaStateMatchesReaders, NoStaleFloorServed in
  { DriverForget, ManifestReplica, Writer };

test tcSyncAfterExitStrands [main = DriverSyncAfterExitStrands]:
  assert ReplicaStateMatchesReaders in
  { DriverSyncAfterExitStrands, ManifestReplica, Writer };

test tcSyncAfterExitFixed [main = DriverSyncAfterExitFixed]:
  assert ReplicaStateMatchesReaders in
  { DriverSyncAfterExitFixed, ManifestReplica, Writer };

test tcExplore [main = DriverExplore]:
  assert ReplicaStateMatchesReaders, NoStaleFloorServed in
  { DriverExplore, ManifestReplica, Writer };

/* Headline new gate: the syncRequiresContext guard ALONE (with the shipped
   writer-first attach order) is UNSAFE. A startup sync that beats the local
   context registration is dropped and never re-sent, so the cache stays empty -
   a ReplicaConverges violation - even though no state leaks (ReplicaStateMatchesReaders
   stays green). MUST FAIL on convergence. */
test tcStartupRaceWriterFirst [main = DriverStartupRaceWriterFirst]:
  assert ReplicaConverges, ReplicaStateMatchesReaders in
  { DriverStartupRaceWriterFirst, AttachNode, ManifestReplica, Writer };

/* The coordinated fix: A1 attach-ordering (context before writer) + A2
   syncRequiresContext guard. The startup sync always lands on a live context so
   the cache converges, and the post-DOWN straggler is dropped so nothing strands.
   All monitors HOLD. */
test tcStartupRaceContextFirst [main = DriverStartupRaceContextFirst]:
  assert ReplicaConverges, ReplicaStateMatchesReaders, NoStaleFloorServed in
  { DriverStartupRaceContextFirst, AttachNode, ManifestReplica, Writer };

/* Headline second-trigger gate: A1 does NOT cover the writer-driven reconcile
   path. Context registration is clean (A1-consistent) and A2 is on, but the
   reconcile-path sync races ahead of the context registration, is dropped, and
   (resyncOnRegister OFF) is never recovered - the cache stays empty. MUST FAIL on
   convergence, proving the attach-ordering fix is insufficient for this trigger. */
test tcReconcileRaceNoResync [main = DriverReconcileRaceNoResync]:
  assert ReplicaConverges, ReplicaStateMatchesReaders in
  { DriverReconcileRaceNoResync, ContextRegistrar, ManifestReplica, Writer };

/* A2 + A1' covers the reconcile trigger: registering the context requests a
   resync, so the writer re-sends the manifest even when the reconcile sync won
   the race and was dropped. All monitors HOLD. */
test tcReconcileRaceResync [main = DriverReconcileRaceResync]:
  assert ReplicaConverges, ReplicaStateMatchesReaders, NoStaleFloorServed in
  { DriverReconcileRaceResync, ContextRegistrar, ManifestReplica, Writer };
