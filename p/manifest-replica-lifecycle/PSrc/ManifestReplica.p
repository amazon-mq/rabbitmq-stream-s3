/* The per-node manifest replica process (rabbitmq_stream_s3_manifest_replica),
   the system under test. Holds three per-stream maps in lockstep:

     contexts : stream -> mref      (the registered osiris member's monitor ref)
     seqs     : stream -> {epoch, sn, writerNode}   (gap detection)
     cache    : stream -> {floor, epoch, sn}        (the ETS row GC/consumers read)

   plus a reverse index monitors : mref -> stream so a DOWN can find the stream
   to release, exactly as the shipped #state{} carries.

   Five toggles let the validation-gate tests remove one guard at a time:

     cleanupEnabled            : G1. On a member DOWN, release_stream/2 drops all
                                 per-stream state. OFF models the pre-cc50092 code
                                 that never monitored or released, so an exited
                                 reader's metadata accretes forever.
     staleSyncGuardEnabled     : G2. is_stale_sync/3 - drop a sync whose (epoch,
                                 sn) is older than what is recorded. OFF lets a
                                 reordered older sync roll the cache backward.
     repointEnabled            : G3. On re-registration (member restart) drop the
                                 previous monitor first, so the old incarnation's
                                 DOWN cannot evict the new context. OFF leaves the
                                 stale mref in the reverse index, so the old DOWN
                                 releases the LIVE new context.
     syncRequiresContextEnabled: A2. Ignore a sync OR an edit for a stream with no
                                 registered context, so either write path arriving
                                 after the member DOWN cannot re-strand a row.
                                 OFF (the pre-fix behaviour) re-creates the row
                                 from either path; this now-shipped guard covers
                                 eMRSync and eMREdit identically (one check, one
                                 toggle, mirroring the real code's shared
                                 drop_no_context/6 helper).
     resyncOnRegisterEnabled   : A1'. The writer-independent recovery for A2: on
                                 registering a context, request a resync from the
                                 writer (request_resync/2), so the writer re-sends
                                 the manifest even if an earlier premature sync or
                                 edit - from the acceptor reply OR the writer-driven
                                 reconcile path - was dropped by A2. Unlike the A1
                                 attach-ordering fix, this covers the reconcile
                                 trigger, which A1 cannot reach. */
machine ManifestReplica {
  var contexts: map[StreamId, int];
  var seqs: map[StreamId, (epoch: int, sn: int, writerNode: int)];
  var cache: map[StreamId, (floor: int, epoch: int, sn: int)];
  var monitors: map[int, StreamId];
  var mrefCounter: int;

  var cleanupEnabled: bool;
  var staleSyncGuardEnabled: bool;
  var repointEnabled: bool;
  var syncRequiresContextEnabled: bool;
  var resyncOnRegisterEnabled: bool;

  /* The writer's replica reader for this replica's streams. In the shipped code
     the replica resolves it per (stream, writer_node) via the registry when it
     casts {resync, node()}; every driver here has a single writer, so we carry
     one handle. Only used when resyncOnRegisterEnabled (A1'). */
  var writer: machine;

  /* release_stream/2: drop the context (and its monitor), the gap-detection
     sequence, and the cached row for a stream. Used by both the member DOWN
     handler and the explicit forget/1. */
  fun ReleaseStream(stream: StreamId) {
    if (stream in contexts) {
      monitors -= (contexts[stream]);
      contexts -= (stream);
    }
    if (stream in seqs) { seqs -= (stream); }
    if (stream in cache) { cache -= (stream); }
  }

  /* The set of streams the replica still holds ANY state for. Cleanup must drive
     this to exclude every stream whose last reader has exited. */
  fun HeldStreams(): set[StreamId] {
    var held: set[StreamId];
    var s: StreamId;
    held = default(set[StreamId]);
    foreach (s in keys(contexts)) { held += (s); }
    foreach (s in keys(seqs)) { held += (s); }
    foreach (s in keys(cache)) { held += (s); }
    return held;
  }

  start state Serving {
    entry (init: (cleanupEnabled: bool, staleSyncGuardEnabled: bool,
                  repointEnabled: bool, syncRequiresContextEnabled: bool,
                  resyncOnRegisterEnabled: bool, writer: machine)) {
      cleanupEnabled = init.cleanupEnabled;
      staleSyncGuardEnabled = init.staleSyncGuardEnabled;
      repointEnabled = init.repointEnabled;
      syncRequiresContextEnabled = init.syncRequiresContextEnabled;
      resyncOnRegisterEnabled = init.resyncOnRegisterEnabled;
      writer = init.writer;
      mrefCounter = 0;
    }

    /* register_replica_context/5: monitor the member and record the context.
       A re-registration for a stream already registered repoints the monitor:
       drop the previous mref from the reverse index first (G3). */
    on eRegister do (p: (from: machine, stream: StreamId)) {
      var newMref: int;
      if (repointEnabled && p.stream in contexts) {
        monitors -= (contexts[p.stream]);
      }
      mrefCounter = mrefCounter + 1;
      newMref = mrefCounter;
      contexts[p.stream] = newMref;
      monitors[newMref] = p.stream;
      /* A1' (resyncOnRegister): now that a context exists, ask the writer to
         re-send the manifest. This recovers a premature sync (acceptor-reply or
         reconcile-path) that A2 dropped before this context existed. The resync
         reply lands on the now-live context. */
      if (resyncOnRegisterEnabled) {
        send writer, eResync, (from = this, stream = p.stream);
      }
      send p.from, eRegisterAck, (mref = newMref,);
    }

    /* handle_info({'DOWN', MRef, ...}): a monitored member exited. Release its
       stream iff the mref is still live in the reverse index. An mref absent
       from the map belongs to a superseded registration and is ignored. With
       cleanup disabled the replica never monitored, so a DOWN does nothing. */
    on eMemberDown do (p: (from: machine, mref: int)) {
      if (cleanupEnabled && p.mref in monitors) {
        ReleaseStream(monitors[p.mref]);
      }
      send p.from, eDownAck;
    }

    /* maybe_apply_sync/8: a fire-and-forget full-manifest sync from the writer.
       Drop it when is_stale_sync (the recorded (epoch, sn) is at least as new).
       Otherwise write the cache row, update the gap sequence, and announce the
       new cached floor to the monitors. */
    on eMRSync do (p: (stream: StreamId, floor: int, epoch: int, sn: int, writerNode: int)) {
      var stale: bool;
      var rec: (epoch: int, sn: int, writerNode: int);
      stale = false;
      if (staleSyncGuardEnabled && p.stream in seqs) {
        rec = seqs[p.stream];
        stale = StaleLex(p.epoch, p.sn, rec.epoch, rec.sn);
      }
      if (!stale) {
        /* A2 (syncRequiresContextEnabled): ignore a sync for a stream with no
           live context, so a sync racing/after a member DOWN cannot re-strand a
           cache row. Models the shipped fix; OFF reproduces the pre-fix code
           that re-creates the row (tcSyncAfterExitStrands). */
        if (syncRequiresContextEnabled && !(p.stream in contexts)) {
          return;
        }
        cache[p.stream] = (floor = p.floor, epoch = p.epoch, sn = p.sn);
        seqs[p.stream] = (epoch = p.epoch, sn = p.sn, writerNode = p.writerNode);
        announce eCacheUpdated, (stream = p.stream, floor = p.floor, epoch = p.epoch, sn = p.sn);
      }
    }

    /* apply_edits/5: a fire-and-forget batch of sequenced edits from the writer.
       Requires sn == last_seq + 1 against seqs (a gap or epoch mismatch is
       dropped without touching cache/seqs; the real code requests a resync
       there, which this model already covers for free via the unconditional
       resync-on-register in eRegister above). A stream with no recorded seq yet
       accepts the edit as a fresh baseline - the same "unrecorded is never
       rejected" convention eMRSync's StaleLex already uses, since gap-recovery
       of a genuinely out-of-order first arrival is a data-prefix concern left
       to the TLA+ companion model (see README), not this model's lifecycle
       scope.

       Same gate as eMRSync (syncRequiresContextEnabled, A2): drop an edit for a
       stream with no live context, so an edit racing/after a member DOWN
       cannot re-strand a cache row either. */
    on eMREdit do (p: (stream: StreamId, floor: int, epoch: int, sn: int, writerNode: int)) {
      var gap: bool;
      var rec: (epoch: int, sn: int, writerNode: int);
      if (syncRequiresContextEnabled && !(p.stream in contexts)) {
        return;
      }
      gap = false;
      if (p.stream in seqs) {
        rec = seqs[p.stream];
        if (!(rec.epoch == p.epoch && rec.sn + 1 == p.sn)) {
          gap = true;
        }
      }
      if (gap) {
        return;
      }
      cache[p.stream] = (floor = p.floor, epoch = p.epoch, sn = p.sn);
      seqs[p.stream] = (epoch = p.epoch, sn = p.sn, writerNode = p.writerNode);
      announce eCacheUpdated, (stream = p.stream, floor = p.floor, epoch = p.epoch, sn = p.sn);
    }

    /* put_manifest/3: the writer node writes its own cache row (no member). */
    on ePutManifest do (p: (from: machine, stream: StreamId, floor: int, epoch: int, sn: int)) {
      cache[p.stream] = (floor = p.floor, epoch = p.epoch, sn = p.sn);
      announce eCacheUpdated, (stream = p.stream, floor = p.floor, epoch = p.epoch, sn = p.sn);
      send p.from, ePutAck;
    }

    /* forget/1: explicit release (writer-node reader teardown). */
    on eForget do (p: (from: machine, stream: StreamId)) {
      ReleaseStream(p.stream);
      send p.from, eForgetAck;
    }

    /* Quiesce barrier: a no-op call that, by FIFO from the writer, is processed
       only after every prior writer->replica sync cast. */
    on eBarrierPing do (p: (from: machine)) {
      send p.from, eBarrierPong;
    }

    on eQueryHeld do (p: (from: machine)) {
      send p.from, eHeldResult, HeldStreams();
    }
  }
}
