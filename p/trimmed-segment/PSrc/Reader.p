/* The replica reader driving a fragment transfer. On a transfer failure it
   mirrors handle_transfer_failure: when checkLocalLogAhead is set it consults
   local_log_ahead and, if the local floor has advanced past next_offset (the
   segment is permanently trimmed), recovers via restart_at_local_floor rather
   than resubmitting. With the check disabled (the #225 bug) every failure is
   treated as retriable and resubmitted, so a trimmed segment loops forever. */
machine Reader {
  var checkLocalLogAhead: bool;
  var log: machine;
  var worker: machine;
  var driver: machine;

  start state Idle {
    entry (init: (checkLLA: bool, log: machine, worker: machine, driver: machine)) {
      checkLocalLogAhead = init.checkLLA;
      log = init.log;
      worker = init.worker;
      driver = init.driver;
    }
    on eStartTransfer do {
      announce eTransferSubmitted;
      send worker, eDoUpload, (reader = this, log = log);
    }
    on eTransferResult do (p: (ok: bool)) {
      var f: (localFirst: int, nextOffset: int);
      if (p.ok) {
        announce eTransferResolved;
      } else {
        if (checkLocalLogAhead) {
          send log, eQueryFloor, (from = this,);
          receive { case eFloorResult: (r: (localFirst: int, nextOffset: int)) { f = r; } }
          if (f.localFirst > f.nextOffset) {
            /* restart_at_local_floor: discard the unreachable range and reset
               the frontier to the live local floor. This is forward progress. */
            announce eFrontierReset, (target = f.localFirst,);
            announce eTransferResolved;
          } else {
            send worker, eDoUpload, (reader = this, log = log);
          }
        } else {
          send worker, eDoUpload, (reader = this, log = log);
        }
      }
    }
  }
}
