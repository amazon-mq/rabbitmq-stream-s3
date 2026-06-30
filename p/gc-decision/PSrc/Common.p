/* Shared types and events for the integrated GC reap-decision model.

   The single-axis GC models each prove one guard in isolation: ../gc-reset
   (still_dangling re-reads the live floor), ../gc-reset-multinode (build_lookup
   gates on cache epoch == committed epoch), ../gc-leading-group and
   ../gc-reset-leading-group (the leading-group carve-out, snapshot and live).
   Each collapses the other axes into a constant. This model composes the WHOLE
   rabbitmq_stream_s3_gc decision into one state space so the guards can interact:
   all three classify reasons and all three guards, two stores with independent
   freshness, and a sweep a reset can interleave into.

   The pipeline mirrors the code exactly:

     1. build_lookup / build_stream_lookup - read the committed epoch with a
        QUORUM read (rabbitmq_stream_s3_db:get_consistent) and read the floor plus
        the leading-group carve-out from the LOCAL manifest_replica cache
        (get_manifest_and_epoch). GUARD A (epoch gate): proceed only when the
        cache epoch equals the committed epoch, else fail closed and skip. The
        snapshot uses the committed epoch and the cached floor/carve-out.

     2. classify - against the SNAPSHOT: a data object is an orphan iff its offset
        is below the snapshot floor (reason below_first_offset); a group object
        iff below the floor AND not the snapshot referenced leading group AND not
        in conservative skip-groups mode; a manifest object iff its epoch is below
        the committed epoch (reason stale_epoch).

     3. still_dangling - re-validate each candidate immediately before deleting.
        A stale_epoch candidate is deleted with no re-check (epoch is monotonic;
        its safety rests on the ../writer-fencing guarantee). A below_first_offset
        candidate re-reads the LIVE cache: GUARD B (re-read) compares the offset
        to the live floor rather than the snapshot floor; GUARD C (live carve-out)
        re-derives the leading group from the live manifest rather than the
        snapshot. Either re-read returning undefined keeps the object.

   The committed manifest truth is carried only by the authoritative
   eObjectReferenced / eObjectUnreferenced announcements (as in ../gc-reset-multinode):
   the cache is the single materialized copy GC reads, and it may lag. A reap is
   a bug iff it deletes a currently-referenced object. */

/* An S3 object: a data fragment, a metadata group, or a root manifest. For data
   and group objects epoch is unused (0); for manifest objects offset is unused
   (0). Identified by all four fields, so a stale UID and a freshly re-tiered one
   at the same offset are distinct objects. */
enum ObjKind { DATA, GROUP, MANIFEST }
type Obj = (kind: ObjKind, offset: int, uid: int, epoch: int);

/* Why classify/2 flagged an object. */
enum Reason { BELOW_FLOOR, STALE_EPOCH }
type Candidate = (obj: Obj, reason: Reason);

/* Khepri (rabbitmq_stream_s3_db): the committed epoch, answered by a
   strongly-consistent quorum read. Monotonic; the reset bumps it. */
event eGetConsistent: (from: machine);
event eEpochResult: (ok: bool, epoch: int);
event eSetCommittedEpoch: (from: machine, epoch: int);
event eDbAck;

/* The local manifest_replica cache. A ManifestView is the cached manifest the
   reads return: present is false when the cache has no usable manifest (the
   get_manifest undefined case and the build_lookup empty-entries skip); floor is
   first_offset; epoch is the epoch the cache was synced at; (leadOff, leadUid,
   leadPresent) is the referenced leading group below the floor; skipGroups is the
   conservative leading kilo-/mega-group mode. eMRSync is the fire-and-forget
   propagation from the writer, applied only when the incoming epoch is not behind
   the cached one (is_stale_sync). */
type ManifestView = (present: bool, floor: int, epoch: int,
                     leadOff: int, leadUid: int, leadPresent: bool, skipGroups: bool);
event eMRSync: (from: machine, view: ManifestView);
/* get_manifest_and_epoch: the epoch-aware read build_lookup uses (GUARD A). */
event eMRGetFull: (from: machine);
event eMRFullResult: ManifestView;
/* get_manifest: the live read still_dangling re-uses. The real read drops the
   epoch; this model ignores the epoch field of the result there. */
event eMRGetLive: (from: machine);
event eMRLiveResult: ManifestView;

/* S3 object store. */
event eS3Put: (from: machine, obj: Obj);
event eS3Delete: (from: machine, obj: Obj);
event eS3List: (from: machine);
event eS3ListResult: set[Obj];
event eS3Has: (from: machine, obj: Obj);
event eS3HasResult: bool;
event eS3Ack;

/* Writer / reset orchestrator (rabbitmq_stream_s3_replica_reader). The reset is a
   committed remote-tier-ahead reset: it bumps the committed epoch, lowers the
   floor, and re-tiers a live fragment with a fresh UID below the old floor. It
   announces the authoritative new floor and the re-tiered live (offset, uid). It
   does NOT touch the cache - the cache learns of it only via eMRSync, which the
   driver controls. */
event eDoReset: (from: machine, newFloor: int, newEpoch: int, retierOffset: int, retierUid: int);
event eResetDone;

/* GC sweep steps, driven explicitly so a driver can force an exact interleaving
   (e.g. snapshot, then a reset, then classify, then execute). */
event eGcSnapshot;
event eGcSnapshotDone;
event eGcClassify;
event eGcClassifyDone;
event eGcExecute;
event eGcExecuteDone;

/* Spec events, delivered to monitors via announce. The AUTHORITATIVE committed
   manifest drives them; the lagging cache is never their source. */
event eObjectReferenced: Obj;
event eObjectUnreferenced: Obj;
event eGcDelete: Obj;

/* Whether a group object is protected by a (snapshot or live) leading-group
   carve-out: the stream is in conservative skip-groups mode, or the object is the
   referenced leading group. Mirrors classify_group/3 and live_leading_group/3. */
fun groupProtected(o: Obj, leadOff: int, leadUid: int, leadPresent: bool, skipGroups: bool): bool {
  if (skipGroups) {
    return true;
  }
  if (leadPresent && o.offset == leadOff && o.uid == leadUid) {
    return true;
  }
  return false;
}
