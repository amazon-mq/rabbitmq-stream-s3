/* Global safety monitors for the anchor-before-first-fragment design.

   NoReapLive is the headline: a reap may happen only after the queue is deleted.
   It catches both load-bearing failures - a stale anchor read and an
   anchor-written-after-the-fragment ordering violation each cause the sweep to
   reap a live stream, which trips here. EventuallyEmpty is the by-construction
   reclaim: once the stream is deleted and sweeps quiesce, no object remains, even
   across a crash mid-sweep. */

/* A reap is legitimate only for a deleted stream. */
spec NoReapLive observes eQueueDeleted, eObjReaped {
  var queueDeleted: bool;

  start state Watching {
    on eQueueDeleted do {
      queueDeleted = true;
    }
    on eObjReaped do (k: ObjKey) {
      assert queueDeleted,
        format("INV NOREAPLIVE violated: GC reaped object (offset={0}, uid={1}) while the stream's queue is still live (the anchor read was stale, or the anchor was written after the fragment)",
          k.offset, k.uid);
    }
  }
}

/* Once the stream is deleted and sweeps quiesce, nothing is left under the prefix. */
spec EventuallyEmpty observes eObjStored, eObjReaped, eSweepQuiesced {
  var stored: map[ObjKey, bool];

  start state Watching {
    on eObjStored do (k: ObjKey) {
      if (!(k in stored)) {
        stored += (k, true);
      }
    }
    on eObjReaped do (k: ObjKey) {
      if (k in stored) {
        stored -= (k);
      }
    }
    on eSweepQuiesced do {
      assert sizeof(stored) == 0,
        format("INV RECLAIM violated: {0} object(s) left under the prefix after the stream was deleted and sweeps quiesced",
          sizeof(stored));
    }
  }
}
