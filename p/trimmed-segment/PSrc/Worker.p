/* The upload worker (a governor transfer task): preads the local segment and
   reports success, or failure when the segment has been trimmed away. */
machine Worker {
  start state Ready {
    on eDoUpload do (p: (reader: machine, log: machine)) {
      var ok: bool;
      send p.log, eReadSegment, (from = this,);
      receive {
        case eReadOk: { ok = true; }
        case eReadTrimmed: { ok = false; }
      }
      send p.reader, eTransferResult, (ok = ok,);
    }
  }
}
