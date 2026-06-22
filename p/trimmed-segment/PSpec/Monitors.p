/* INV#3 (progress / no unbounded wedge): every submitted transfer must
   eventually resolve - either by completing or by recovering via
   restart_at_local_floor. A trimmed-segment failure that is resubmitted forever
   leaves this obligation undischarged. AwaitingResolution is a hot state: an
   execution that stays in it (the #225 resubmit loop) is a liveness violation. */
spec TransferEventuallyResolves observes eTransferSubmitted, eTransferResolved {
  start state Idle {
    on eTransferSubmitted goto AwaitingResolution;
  }
  hot state AwaitingResolution {
    on eTransferResolved goto Idle;
  }
}

/* Safety: a recovery reset targets the live local floor (the only correct
   restart point once the original range is permanently trimmed). */
spec ResetTargetsLocalFloor observes eFrontierReset {
  start state Watching {
    on eFrontierReset do (p: (target: int)) {
      assert p.target > 0,
        "recovery reset must target a positive local floor (restart_at_local_floor)";
    }
  }
}
