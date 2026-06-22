/* Test drivers and declarations for the #225 trimmed-segment seam.

   The gate contrasts tcTrimGuarded (the local_log_ahead recovery: a trimmed
   transfer resolves via reset, liveness holds) with tcTrimBuggy (the check
   removed: the transfer resubmits forever, the liveness obligation is never
   discharged). */

fun BuildAndTrim(self: machine, checkLLA: bool): machine {
  var log: machine;
  var worker: machine;
  var reader: machine;
  log = new LocalLog((localFirst = 0, nextOffset = 5));
  worker = new Worker();
  reader = new Reader((checkLLA = checkLLA, log = log, worker = worker, driver = self));
  /* Retention trims the local log past next_offset before the upload reads it. */
  send log, eTrim, (from = self,);
  receive { case eTrimAck: { } }
  return reader;
}

machine DriverGuarded {
  start state Init {
    entry {
      var reader: machine;
      reader = BuildAndTrim(this, true);
      send reader, eStartTransfer;
    }
  }
}

machine DriverBuggy {
  start state Init {
    entry {
      var reader: machine;
      reader = BuildAndTrim(this, false);
      send reader, eStartTransfer;
    }
  }
}

/* Exploration: race the trim against the transfer (guard on). The transfer must
   resolve whether the trim lands before, during, or after the upload. */
machine DriverExplore {
  start state Init {
    entry {
      var log: machine;
      var worker: machine;
      var reader: machine;
      log = new LocalLog((localFirst = 0, nextOffset = 5));
      worker = new Worker();
      reader = new Reader((checkLLA = true, log = log, worker = worker, driver = this));
      /* Race the trim against the transfer: both are dispatched before either is
         awaited, so the upload's eReadSegment and the eTrim interleave at the
         log's queue. The eTrimAck is consumed so it is not an unhandled event. */
      send log, eTrim, (from = this,);
      send reader, eStartTransfer;
      receive { case eTrimAck: { } }
    }
  }
}

/* Current code: a trimmed transfer recovers. Liveness holds. */
test tcTrimGuarded [main = DriverGuarded]:
  assert TransferEventuallyResolves, ResetTargetsLocalFloor in
  { DriverGuarded, LocalLog, Worker, Reader };

/* #225 bug (local_log_ahead check removed): the transfer resubmits forever.
   MUST fail the liveness obligation. This failing run is the gate. */
test tcTrimBuggy [main = DriverBuggy]:
  assert TransferEventuallyResolves, ResetTargetsLocalFloor in
  { DriverBuggy, LocalLog, Worker, Reader };

/* Guard on, trim raced against the transfer. Liveness holds. */
test tcTrimExplore [main = DriverExplore]:
  assert TransferEventuallyResolves, ResetTargetsLocalFloor in
  { DriverExplore, LocalLog, Worker, Reader };
