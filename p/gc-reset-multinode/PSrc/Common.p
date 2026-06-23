/* Shared types and events for the multi-node GC x reset durability seam.

   This model is the cross-node companion to ../gc-reset. That model collapses
   every node into one synchronous manifest_replica, so GC and the writer always
   read the same live floor. Here the floor GC reads comes from a SEPARATE,
   lagging per-node replica cache (rabbitmq_stream_s3_manifest_replica), updated
   from the writer by a fire-and-forget sync that may be dropped or delayed.

   The seam: rabbitmq_stream_s3_gc:build_lookup reads the committed epoch with a
   quorum read (rabbitmq_stream_s3_db:get_consistent) but reads first_offset from
   the LOCAL replica ETS cache (manifest_replica:get_manifest). Nothing ties the
   cached floor's freshness to the committed epoch. An operator CLI delete sweep
   (stream_s3_gc --mode delete) against a node that has not yet applied the
   reset's sync reads a stale-HIGH floor; still_dangling/1 re-reads the same
   stale cache, so the offset guard defends nothing and a freshly re-tiered live
   object below the stale floor is deleted. */

/* An S3 object / manifest entry is identified by (offset, uid): two objects can
   share an offset (a stale UID and a freshly re-tiered one). */
type ObjKey = (offset: int, uid: int);

/* Why GC flagged a data object. Only the below_first_offset (offset) axis is
   modeled: in rabbitmq_stream_s3_gc:classify/2 the stale_epoch axis applies only
   to manifest root objects, never to fragments, so it is irrelevant here. */
enum Reason { BELOW_FLOOR }
type Candidate = (offset: int, uid: int, reason: Reason);

/* Khepri (durable consensus over the committed epoch). The reset bumps it. */
event eGetConsistent: (from: machine);
event eEpochResult: (ok: bool, epoch: int);
event eSetCommittedEpoch: (from: machine, epoch: int);
event eDbAck;

/* Per-node manifest replica cache. eMRSync is the fire-and-forget propagation
   of a reset from the writer; the replica applies it only if the incoming epoch
   is at least the cached one (is_stale_sync: epoch dominates). When the sync is
   dropped/delayed the cache keeps its stale-high floor at its old epoch. */
event eMRSync: (from: machine, floor: int, epoch: int);
event eMRGetFloor: (from: machine);
event eMRFloorResult: int;
event eMRGetFloorEpoch: (from: machine);
event eMRFloorEpochResult: (floor: int, epoch: int);

/* S3 object store. */
event eS3Put: (from: machine, key: ObjKey);
event eS3Delete: (from: machine, key: ObjKey);
event eS3List: (from: machine);
event eS3ListResult: set[ObjKey];
event eS3Has: (from: machine, key: ObjKey);
event eS3HasResult: bool;
event eS3Ack;

/* Writer: orchestrates the committed remote-tier-ahead reset (the durable side
   is already done; the only open question is whether the cache learns of it). */
event eDoReset: (from: machine, newFloor: int, newUid: int, newEpoch: int);
event eResetDone;

/* GC sweep steps, driven explicitly so a test driver can force an exact
   interleaving for the validation gate, or race the sync against them. */
event eGcSnapshot;
event eGcSnapshotDone;
event eGcListClassify;
event eGcClassifyDone;
event eGcExecute;
event eGcExecuteDone;

/* Spec events, delivered to monitors via announce. The AUTHORITATIVE manifest
   state (writer/committed) drives them; the lagging cache is a silent observer
   of the truth, never its source. */
event eObjectReferenced: ObjKey;
event eObjectUnreferenced: ObjKey;
event eFloorChanged: (newFloor: int, isReset: bool);
event eGcDelete: ObjKey;
