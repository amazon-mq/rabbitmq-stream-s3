/* The S3 object store, holding orphaned objects (failed/unreferenced uploads).
   A delete fails transiently while faultsRemaining > 0, modeling the reaper's
   routine partial-batch DeleteObjects failures; once the budget is exhausted the
   delete succeeds (the transient clears). Objects are a map id -> present so
   keys/0 yields the listing. */
machine S3Store {
  var objects: map[int, bool];
  var faultsRemaining: int;

  start state Serving {
    entry (init: (faultsRemaining: int)) {
      faultsRemaining = init.faultsRemaining;
    }
    on eAddOrphan do (p: (from: machine, id: int)) {
      if (p.id in objects) {
        objects[p.id] = true;
      } else {
        objects += (p.id, true);
      }
      announce eOrphanCreated, p.id;
      send p.from, eAddAck;
    }
    on eList do (p: (from: machine)) {
      send p.from, eListResult, keys(objects);
    }
    on eDelete do (p: (from: machine, id: int)) {
      if (faultsRemaining > 0) {
        faultsRemaining = faultsRemaining - 1;
        send p.from, eDeleteFailed, p.id;
      } else {
        if (p.id in objects) {
          objects -= (p.id);
          announce eOrphanDeleted, p.id;
        }
        send p.from, eDeleteOk, p.id;
      }
    }
  }
}
