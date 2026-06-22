/* The reader routing an integer offset to a tier (resolve_remote_location/2).
   bugNoMinusOneGuard drops the `first_chunk_id =/= -1` guard from the local-tier
   check, reproducing the empty-local-log silent remote skip. */
machine Reader {
  var bugNoMinusOneGuard: bool;
  var manifest: machine;
  var driver: machine;

  start state Idle {
    entry (init: (bug: bool, manifest: machine, driver: machine)) {
      bugNoMinusOneGuard = init.bug;
      manifest = init.manifest;
      driver = init.driver;
    }
    on eResolve do (p: (from: machine, firstChunkId: int, offset: int)) {
      var m: (nonEmpty: bool, remoteFirst: int, remoteNext: int);
      var tier: Tier;
      var localCheck: bool;
      var localCovers: bool;
      var remoteCovers: bool;
      send manifest, eGetManifest, (from = this,);
      receive {
        case eManifestResult: (r: (nonEmpty: bool, remoteFirst: int, remoteNext: int)) { m = r; }
      }
      if (bugNoMinusOneGuard) {
        localCheck = p.offset >= p.firstChunkId;
      } else {
        localCheck = (p.firstChunkId != -1) && (p.offset >= p.firstChunkId);
      }
      if (localCheck) {
        tier = LOCAL;
      } else if (!m.nonEmpty) {
        /* No remote tier: emulate osiris_log and attach locally. */
        tier = LOCAL;
      } else if (p.firstChunkId == -1 && p.offset >= m.remoteNext) {
        /* Local empty and the offset is beyond the remote tail: local tail wait. */
        tier = LOCAL;
      } else {
        tier = REMOTE;
      }
      localCovers = (p.firstChunkId != -1) && (p.offset >= p.firstChunkId);
      remoteCovers = m.nonEmpty && (p.offset >= m.remoteFirst) && (p.offset < m.remoteNext);
      announce eResolution, (tier = tier, remoteCovers = remoteCovers, localCovers = localCovers);
      send p.from, eResolveDone, tier;
    }
  }
}
