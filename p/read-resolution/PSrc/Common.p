/* Shared types and events for the read / tier-resolution seam.

   Models log_reader:resolve_first/2 and resolve_first_lookup/1: resolving the
   'first' offset spec descends into the leading manifest group (a fragment_iterator
   step that fetches the group from S3). A transient group fetch failure must
   surface as a retry, NOT a silent fallback to the local tier. The pre-e3f931b
   catch-all collapsed group_fetch_failed into {local, first}, silently skipping
   the remote range below the local floor. */

/* The three resolution outcomes of resolve_first_lookup/1:
   {remote, _} | {local, first} | {retry, _} (surfaced as {error, _}). */
enum Resolution { REMOTE, LOCAL_FIRST, RETRY }

/* Manifest store (remote tier metadata + retention). */
event eGetRemoteState: (from: machine);
event eRemoteStateResult: (nonEmpty: bool, remoteFirst: int, localFloor: int);
event eAdvanceRetention: (from: machine);
event eMSAck;

/* Group store (the leading group fetched while descending to the first fragment).
   A fetch returns ok, or group_fetch_failed (a transient S3 error or a group a
   concurrent retention has deleted). */
event eFetchGroup: (from: machine);
event eFetchOk;
event eFetchFailed;
event eSetPresent: (from: machine, present: bool);
event eGSAck;

/* Reader. */
event eResolveFirst: (from: machine);
event eResolveDone: Resolution;

/* Spec events (announce). */
event eRemoteEmptied;
event eResolution: Resolution;
