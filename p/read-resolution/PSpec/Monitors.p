/* INV#4 (tier resolution totality / no silent remote skip): resolving 'first'
   may return the local tier only when the remote tier is genuinely empty. A
   transient group fetch failure must surface as a retry; collapsing it to a
   local-tier resolution silently skips the remote range below the local floor.

   remoteEmpty tracks whether retention has emptied the remote tier. Because
   eRemoteEmptied is announced inside the (serialized) retention handler, it
   strictly precedes any resolution that observes the emptied state, so a
   legitimate post-retention local resolution does not trip this monitor. */
spec NoSilentRemoteSkip observes eRemoteEmptied, eResolution {
  var remoteEmpty: bool;

  start state Watching {
    on eRemoteEmptied do {
      remoteEmpty = true;
    }
    on eResolution do (r: Resolution) {
      if (r == LOCAL_FIRST) {
        assert remoteEmpty,
          "INV#4 violated: resolved 'first' to the local tier while the remote tier is non-empty (a transient group fetch must surface as retry, not a silent skip of remote data)";
      }
    }
  }
}
