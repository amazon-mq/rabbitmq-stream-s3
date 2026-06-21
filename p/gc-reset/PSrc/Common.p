/* Shared types, enums, and events for the GC x reset durability-seam model.

   Models the decide-then-act window in rabbitmq_stream_s3_gc: a sweep captures
   a snapshot first_offset, a concurrent remote-tier-ahead reset lowers the live
   first_offset and re-tiers a live fragment (with a fresh UID) below the
   snapshot floor, and the sweep then deletes against the stale snapshot. The
   still_dangling/1 guard re-reads the live floor before each delete. */

/* An S3 object is identified by (offset, uid): the on-disk key is
   <offset>.<uid>.fragment, so two objects can share an offset (a stale UID and
   a freshly re-tiered one). */
type ObjKey = (offset: int, uid: int);
type Obj = (offset: int, uid: int, epoch: int);

/* Why GC flagged an object. below_first_offset is re-validated by still_dangling
   against the live floor; stale_epoch is not, because epoch is monotonic. */
enum Reason { BELOW_FLOOR, STALE_EPOCH }
type Candidate = (offset: int, uid: int, epoch: int, reason: Reason);

/* Khepri (durable consensus over the committed epoch). */
event eGetConsistent: (from: machine);
event eEpochResult: (ok: bool, epoch: int);

/* Manifest replica: the serialized owner of the live first_offset and the set
   of live (offset, uid) entries. */
event eMRSetFloor: (from: machine, floor: int, isReset: bool);
event eMRAddEntry: (from: machine, key: ObjKey);
event eMRRemoveEntry: (from: machine, key: ObjKey);
event eMRGetFloor: (from: machine);
event eMRFloorResult: int;
event eMRAck;

/* S3 object store. */
event eS3Put: (from: machine, obj: Obj);
event eS3Delete: (from: machine, key: ObjKey);
event eS3List: (from: machine);
event eS3ListResult: seq[Obj];
event eS3Has: (from: machine, key: ObjKey);
event eS3HasResult: bool;
event eS3Ack;

/* Writer: orchestrates the remote-tier-ahead reset. */
event eDoReset: (localFloor: int, newUid: int);
event eResetDone;

/* GC sweep steps, driven explicitly so a test driver can force an exact
   interleaving for the validation gate, or race them for exploration. */
event eGcSnapshot;
event eGcSnapshotDone;
event eGcListClassify;
event eGcClassifyDone;
event eGcExecute;
event eGcExecuteDone;

/* Spec events, delivered to monitors via announce. */
event eObjectReferenced: ObjKey;
event eObjectUnreferenced: ObjKey;
event eFloorChanged: (newFloor: int, isReset: bool);
event eGcDelete: ObjKey;
