/* The manifest replica's remote-tier metadata as seen by a reader resolving an
   offset spec. Retention can empty the remote tier (advance first_offset to the
   local floor and delete the leading group); after that, resolving 'first' to
   the local tier is correct. */
machine ManifestStore {
  var nonEmpty: bool;
  var remoteFirst: int;
  var localFloor: int;
  var groups: machine;

  start state Serving {
    entry (init: (nonEmpty: bool, remoteFirst: int, localFloor: int, groups: machine)) {
      nonEmpty = init.nonEmpty;
      remoteFirst = init.remoteFirst;
      localFloor = init.localFloor;
      groups = init.groups;
    }
    on eGetRemoteState do (p: (from: machine)) {
      send p.from, eRemoteStateResult,
        (nonEmpty = nonEmpty, remoteFirst = remoteFirst, localFloor = localFloor);
    }
    on eAdvanceRetention do (p: (from: machine)) {
      nonEmpty = false;
      remoteFirst = localFloor;
      send groups, eSetPresent, (from = this, present = false);
      receive { case eGSAck: { } }
      announce eRemoteEmptied;
      send p.from, eMSAck;
    }
  }
}
