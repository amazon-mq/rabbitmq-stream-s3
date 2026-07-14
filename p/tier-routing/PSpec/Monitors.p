/* INV#4 (tier resolution total / correct): an offset held only by the remote
   tier (covered by the remote extent, not by the local log) must not route to
   the local tier. Routing it locally silently skips the remote data. RETRY is
   acceptable there (fail closed, the consumer retries); LOCAL is not. An
   offset the local tier also holds may route locally (osiris prefers the local
   reader), so the obligation is only on offsets the local tier does NOT cover.

   Independent oracle: coverage is re-derived here from the ground-truth extent
   the DRIVER announced, never from anything the Reader or the cache reported.
   The previous version of this spec computed remoteCovers from the
   ManifestStore's reply, which made the cache simultaneously the component's
   input and the monitor's oracle: a reader that believed an empty or missing
   cache could not be flagged, and the boot-window bug was unrepresentable. */
spec TierRoutingCorrect observes eGroundTruth, eResolution {
  var truthKnown: bool;
  var nonEmpty: bool;
  var remoteFirst: int;
  var remoteNext: int;

  start state Watching {
    on eGroundTruth do (g: (nonEmpty: bool, remoteFirst: int, remoteNext: int)) {
      truthKnown = true;
      nonEmpty = g.nonEmpty;
      remoteFirst = g.remoteFirst;
      remoteNext = g.remoteNext;
    }
    on eResolution do (r: (outcome: Outcome, offset: int, firstChunkId: int)) {
      var remoteCovers: bool;
      var localCovers: bool;
      assert truthKnown, "eResolution observed before eGroundTruth: driver must announce the true extent first";
      remoteCovers = nonEmpty && r.offset >= remoteFirst && r.offset < remoteNext;
      localCovers = (r.firstChunkId != -1) && (r.offset >= r.firstChunkId);
      if (remoteCovers && !localCovers) {
        assert r.outcome != LOCAL,
          "INV#4 violated: an offset held only by the remote tier routed to the local tier (silent remote skip)";
      }
    }
  }
}
