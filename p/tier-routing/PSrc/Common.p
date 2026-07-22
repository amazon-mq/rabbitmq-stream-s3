/* Shared types and events for the offset -> tier routing seam.

   log_reader:resolve_remote_location/2 routes an integer offset to the local or
   remote tier by consulting the node's manifest CACHE, which is not the same
   thing as the remote tier itself. The cache row for a stream is in one of
   three states, and the reader's obligation differs per state:

   - RESOLVED: the manifest is known; route by extent comparison.
   - PENDING: the plugin is attached but the manifest has not resolved or
     synced yet. The remote extent is unknown, so an offset below the local
     floor must FAIL CLOSED (retry), never fall back to the local tier.
   - ABSENT: no plugin state for the stream on this node (un-tiered stream).
     The local log is the whole stream, so local fallback is correct.

   The historical bug modeled here (alongside the older =/= -1 floor-guard bug)
   is collapsing PENDING/ABSENT into "no remote tier -> local", which silently
   skips the remote range below the local floor for up to one reconciliation
   period after a node restart.

   The local-tier check itself is

     first_chunk_id =/= -1 andalso Offset >= first_chunk_id

   first_chunk_id = -1 means the local log is empty (fully trimmed or not yet
   populated). Without the =/= -1 guard, Offset >= -1 is always true, so an
   empty local log routes every offset to the local tail and silently skips the
   remote tier. */

enum Outcome { LOCAL, REMOTE, RETRY }
enum CacheReply { RESOLVED, PENDING, ABSENT }

/* Manifest cache. eGetManifest reports the CACHE's view; the extent fields are
   meaningful only when reply == RESOLVED. */
event eGetManifest: (from: machine);
event eManifestResult: (reply: CacheReply, nonEmpty: bool, remoteFirst: int, remoteNext: int);

/* Lifecycle: the member-init hook marks the row pending (insert-if-absent);
   manifest resolution installs the extent. eMSAck acknowledges either, so
   drivers can sequence deterministically when they need to. */
event eMarkPending: (from: machine);
event eResolveManifest: (from: machine);
event eMSAck;

/* Reader. */
event eResolve: (from: machine, firstChunkId: int, offset: int);
event eResolveDone: Outcome;

/* Spec events (announce). eGroundTruth carries the ACTUAL remote extent,
   announced by the driver, never read by the Reader: the monitor's oracle is
   independent of the component under test. eResolution carries the decision
   plus its inputs, and the monitor re-derives coverage itself. */
event eGroundTruth: (nonEmpty: bool, remoteFirst: int, remoteNext: int);
event eResolution: (outcome: Outcome, offset: int, firstChunkId: int);
