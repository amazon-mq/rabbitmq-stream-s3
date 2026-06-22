/* The S3-backed group object fetched while descending to the first fragment
   (manifest:get_group_fun -> GetGroup). A fetch fails (group_fetch_failed) when
   the group is absent (deleted by retention) or transiently unfetchable (an S3
   error surviving retries). alwaysFail forces the deterministic transient case
   for the validation gate; otherwise a present group fails nondeterministically. */
machine GroupStore {
  var present: bool;
  var alwaysFail: bool;

  start state Serving {
    entry (init: (present: bool, alwaysFail: bool)) {
      present = init.present;
      alwaysFail = init.alwaysFail;
    }
    on eSetPresent do (p: (from: machine, present: bool)) {
      present = p.present;
      send p.from, eGSAck;
    }
    on eFetchGroup do (p: (from: machine)) {
      if (!present) {
        send p.from, eFetchFailed;
      } else if (alwaysFail) {
        send p.from, eFetchFailed;
      } else if ($) {
        /* Transient S3 error: the group is present but momentarily unfetchable. */
        send p.from, eFetchFailed;
      } else {
        send p.from, eFetchOk;
      }
    }
  }
}
