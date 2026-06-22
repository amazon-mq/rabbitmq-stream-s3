/* INV#4 (tier resolution total / correct): an offset held only by the remote
   tier (covered by the remote extent, not by the local log) must route to the
   remote tier. Routing it locally silently skips the remote data. An offset the
   local tier also holds may route locally (osiris prefers the local reader), so
   the obligation is only on offsets the local tier does NOT cover. */
spec TierRoutingCorrect observes eResolution {
  start state Watching {
    on eResolution do (r: (tier: Tier, remoteCovers: bool, localCovers: bool)) {
      if (r.remoteCovers && !r.localCovers) {
        assert r.tier == REMOTE,
          "INV#4 violated: an offset held only by the remote tier routed to the local tier (silent remote skip)";
      }
    }
  }
}
