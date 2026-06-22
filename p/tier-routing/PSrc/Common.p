/* Shared types and events for the offset -> tier routing seam.

   log_reader:resolve_remote_location/2 routes an integer offset to the local or
   remote tier. The local-tier check is

     first_chunk_id =/= -1 andalso Offset >= first_chunk_id

   first_chunk_id = -1 means the local log is empty (fully trimmed or not yet
   populated). Without the =/= -1 guard, Offset >= -1 is always true, so an empty
   local log routes every offset to the local tail and silently skips the remote
   tier. The =/= -1 guard makes an empty local log fall through to remote
   resolution. */

enum Tier { LOCAL, REMOTE }

/* Manifest store (remote tier extent). */
event eGetManifest: (from: machine);
event eManifestResult: (nonEmpty: bool, remoteFirst: int, remoteNext: int);

/* Reader. */
event eResolve: (from: machine, firstChunkId: int, offset: int);
event eResolveDone: Tier;

/* Spec event (announce): the routing decision plus the ground-truth coverage. */
event eResolution: (tier: Tier, remoteCovers: bool, localCovers: bool);
