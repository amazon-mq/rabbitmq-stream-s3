/* Global invariant monitors for the manifest-replica lifecycle seam.

   ReplicaStateMatchesReaders (safety, INV: no leak / no premature loss). At a
   quiesce checkpoint, the set of streams the replica still holds state for must
   equal the set of streams with a live reader. Two directions, each tied to a
   guard:
     - held => live  (no leak): a stranded entry for an exited reader trips this.
       Proves the member-DOWN cleanup (G1) load-bearing, and surfaces the
       sync-after-exit gap (a sync re-creates a row after cleanup released it).
     - live => held  (no loss): evicting a live reader's state trips this. Proves
       the re-registration monitor repoint (G3) load-bearing.

   NoStaleFloorServed (safety, INV: no stale floor). Read-side analogue of the
   ../gc-reset-multinode finding. The cache's applied (epoch, sn) must be
   monotonically non-decreasing and must only ever hold a committed floor, so a
   replica never serves a floor that a newer write already superseded. Proves
   is_stale_sync (G2) load-bearing.

   ReplicaConverges (liveness, hot state). Once the writer has committed a floor,
   a replica that keeps receiving syncs must eventually catch up to the latest
   committed (epoch, sn). A run that ends with the cache permanently behind is a
   liveness violation. */

/* held(stream) == (live readers for stream > 0), checked at quiesce. */
spec ReplicaStateMatchesReaders
  observes eReaderUp, eReaderDown, eQuiesceBegin, eHeld, eQuiesceEnd {
  var liveReaders: map[StreamId, int];
  var held: set[StreamId];

  start state Watching {
    on eReaderUp do (s: StreamId) {
      if (s in liveReaders) {
        liveReaders[s] = liveReaders[s] + 1;
      } else {
        liveReaders[s] = 1;
      }
    }
    on eReaderDown do (s: StreamId) {
      if (s in liveReaders) {
        liveReaders[s] = liveReaders[s] - 1;
        if (liveReaders[s] == 0) {
          liveReaders -= (s);
        }
      }
    }
    on eQuiesceBegin do {
      held = default(set[StreamId]);
    }
    on eHeld do (s: StreamId) {
      held += (s);
    }
    on eQuiesceEnd do {
      var s: StreamId;
      /* No leak: every held stream must still have a live reader. */
      foreach (s in held) {
        assert s in liveReaders,
          format("NOLEAK violated: replica holds per-node state for stream {0} with no live reader (an exited reader was not cleaned up, or a sync re-stranded the row after cleanup)",
            s);
      }
      /* No loss: every live reader's stream must still be held. */
      foreach (s in keys(liveReaders)) {
        assert s in held,
          format("RETAIN violated: replica dropped per-node state for stream {0} that still has a live reader (a superseded member DOWN evicted the live context)",
            s);
      }
    }
  }
}

/* The cache may only ever hold a committed floor, and its applied (epoch, sn)
   must never regress - the read-side stale-floor guard. */
spec NoStaleFloorServed observes eFloorCommitted, eCacheUpdated {
  var committed: set[(floor: int, epoch: int, sn: int)];
  var maxApplied: map[StreamId, (epoch: int, sn: int)];

  start state Watching {
    on eFloorCommitted do (p: (floor: int, epoch: int, sn: int)) {
      committed += ((floor = p.floor, epoch = p.epoch, sn = p.sn));
    }
    on eCacheUpdated do (p: (stream: StreamId, floor: int, epoch: int, sn: int)) {
      var prev: (epoch: int, sn: int);
      assert (floor = p.floor, epoch = p.epoch, sn = p.sn) in committed,
        format("STALEFLOOR violated: replica cached an uncommitted floor for stream {0} (floor={1}, epoch={2}, sn={3})",
          p.stream, p.floor, p.epoch, p.sn);
      if (p.stream in maxApplied) {
        prev = maxApplied[p.stream];
        assert !StaleLex(p.epoch, p.sn, prev.epoch, prev.sn),
          format("STALEFLOOR violated: cache for stream {0} regressed to (epoch={1}, sn={2}) below already-applied (epoch={3}, sn={4}) - a stale sync rolled the floor backward",
            p.stream, p.epoch, p.sn, prev.epoch, prev.sn);
      }
      maxApplied[p.stream] = (epoch = p.epoch, sn = p.sn);
    }
  }
}

/* Liveness: a single tracked stream's cache must eventually reach the latest
   committed (epoch, sn). Lagging is hot: a terminal state with the cache behind
   the committed floor is a violation. */
spec ReplicaConverges observes eFloorCommitted, eCacheUpdated {
  var committedEpoch: int;
  var committedSeq: int;
  var cacheEpoch: int;
  var cacheSeq: int;

  start state Caught {
    on eFloorCommitted do (p: (floor: int, epoch: int, sn: int)) {
      committedEpoch = p.epoch;
      committedSeq = p.sn;
      if (StaleLex(cacheEpoch, cacheSeq, committedEpoch, committedSeq)) {
        goto Lagging;
      }
    }
    on eCacheUpdated do (p: (stream: StreamId, floor: int, epoch: int, sn: int)) {
      cacheEpoch = p.epoch;
      cacheSeq = p.sn;
    }
  }

  hot state Lagging {
    on eFloorCommitted do (p: (floor: int, epoch: int, sn: int)) {
      committedEpoch = p.epoch;
      committedSeq = p.sn;
    }
    on eCacheUpdated do (p: (stream: StreamId, floor: int, epoch: int, sn: int)) {
      cacheEpoch = p.epoch;
      cacheSeq = p.sn;
      if (!StaleLex(cacheEpoch, cacheSeq, committedEpoch, committedSeq)) {
        goto Caught;
      }
    }
  }
}
