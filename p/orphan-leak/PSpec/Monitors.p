/* INV#3 (no unbounded orphan leak): every orphaned object must eventually be
   reclaimed. Dirty is a hot state: an execution that ends (or loops) with an
   orphan still outstanding is a liveness violation. */
spec OrphanEventuallyReclaimed observes eOrphanCreated, eOrphanDeleted {
  var outstanding: set[int];

  start state Clean {
    on eOrphanCreated do (id: int) {
      outstanding += (id);
      goto Dirty;
    }
    on eOrphanDeleted do (id: int) {
      if (id in outstanding) {
        outstanding -= (id);
      }
    }
  }
  hot state Dirty {
    on eOrphanCreated do (id: int) {
      outstanding += (id);
    }
    on eOrphanDeleted do (id: int) {
      if (id in outstanding) {
        outstanding -= (id);
      }
      if (sizeof(outstanding) == 0) {
        goto Clean;
      }
    }
  }
}
