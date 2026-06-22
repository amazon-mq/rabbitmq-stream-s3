/* Split-brain safety: the committed epoch never regresses. A commit that lowers
   the epoch means a deposed (lower-epoch) writer overwrote a newer writer's
   manifest - exactly what the epoch fence prevents. This is the monotonicity the
   gc-reset model's epoch axis assumes. */
spec NoEpochRegression observes eCommitted {
  var maxEpoch: int;
  var started: bool;

  start state Watching {
    on eCommitted do (e: int) {
      if (started) {
        assert e >= maxEpoch,
          format("split-brain: committed epoch regressed from {0} to {1} (a deposed writer overwrote a newer writer)",
            maxEpoch, e);
      }
      maxEpoch = e;
      started = true;
    }
  }
}
