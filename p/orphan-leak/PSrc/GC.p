/* The orphan GC sweep. It lists orphans and issues deletes (the reaper batch);
   failed deletes are simply left behind, as the real reaper does. reSweep is the
   liveness mechanism: when set, the sweep re-lists and re-deletes until the store
   is clean, so a transiently failed delete is reclaimed on a later pass. With it
   off (the bug) a single pass leaves a failed delete leaked forever. */
machine GC {
  var reSweep: bool;
  var s3: machine;
  var driver: machine;

  start state Idle {
    entry (init: (reSweep: bool, s3: machine, driver: machine)) {
      reSweep = init.reSweep;
      s3 = init.s3;
      driver = init.driver;
    }
    on eSweep do {
      var orphans: seq[int];
      var i: int;
      var clean: bool;
      clean = false;
      while (!clean) {
        send s3, eList, (from = this,);
        receive { case eListResult: (l: seq[int]) { orphans = l; } }
        if (sizeof(orphans) == 0) {
          clean = true;
        } else {
          i = 0;
          while (i < sizeof(orphans)) {
            send s3, eDelete, (from = this, id = orphans[i]);
            receive {
              case eDeleteOk: (x: int) { }
              case eDeleteFailed: (x: int) { }
            }
            i = i + 1;
          }
          if (!reSweep) {
            clean = true;
          }
        }
      }
      send driver, eSweepFinished;
    }
  }
}
