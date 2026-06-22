/* The local osiris log and its retention. A trim past next_offset removes the
   segment backing the head fragment permanently; subsequent preads fail
   (enoent), and the local floor (localFirst) advances past next_offset. */
machine LocalLog {
  var trimmed: bool;
  var localFirst: int;
  var nextOffset: int;

  start state Serving {
    entry (init: (localFirst: int, nextOffset: int)) {
      localFirst = init.localFirst;
      nextOffset = init.nextOffset;
    }
    on eTrim do (p: (from: machine)) {
      trimmed = true;
      localFirst = nextOffset + 1;
      send p.from, eTrimAck;
    }
    on eQueryFloor do (p: (from: machine)) {
      send p.from, eFloorResult, (localFirst = localFirst, nextOffset = nextOffset);
    }
    on eReadSegment do (p: (from: machine)) {
      if (trimmed) {
        send p.from, eReadTrimmed;
      } else {
        send p.from, eReadOk;
      }
    }
  }
}
