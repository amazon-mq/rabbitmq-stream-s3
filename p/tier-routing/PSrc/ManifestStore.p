/* The remote-tier extent as seen by a reader: whether the remote tier has any
   entries, and the offset range [remoteFirst, remoteNext) it covers. */
machine ManifestStore {
  var nonEmpty: bool;
  var remoteFirst: int;
  var remoteNext: int;

  start state Serving {
    entry (init: (nonEmpty: bool, remoteFirst: int, remoteNext: int)) {
      nonEmpty = init.nonEmpty;
      remoteFirst = init.remoteFirst;
      remoteNext = init.remoteNext;
    }
    on eGetManifest do (p: (from: machine)) {
      send p.from, eManifestResult,
        (nonEmpty = nonEmpty, remoteFirst = remoteFirst, remoteNext = remoteNext);
    }
  }
}
