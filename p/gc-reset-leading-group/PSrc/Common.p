/* Shared types and events for the GC x reset x leading-group durability seam.

   This model sits at the INTERSECTION that neither shipped sibling model
   exercises:

     - ../gc-reset proves still_dangling/1's live-floor re-check is sound for
       DATA fragments keyed by (offset, uid): a fresh-UID re-tier coexists with a
       stale UID at the same offset, so the (offset, uid)-keyed delete is legal.
       It is flat-offset: it has no groups and no leading-group carve-out.

     - ../gc-leading-group proves the classify-time carve-out
       (classify_group / referenced_group_key): the leading group, which retention
       has pushed below first_offset while it is still referenced, must not be
       classified as a deletable orphan. It is static: no reset, no concurrency,
       no still_dangling re-check.

   The seam here is the three-way interaction of BOTH guards with a reset:

     - The carve-out's referenced_group_key is computed ONCE, at the sweep's
       snapshot (build_lookup / build_stream_lookup -> lookup_entry), and passed
       by value through list_and_classify. It names the SNAPSHOT leading group.
     - still_dangling/1, for a group finding, re-reads only the live FLOOR
       (Offset < first_offset). It does NOT re-run leading_group_info/2, so it
       never re-validates the carve-out against the LIVE manifest.

   So a reset that installs a NEW leading group below the (lowered, then
   retention-re-advanced) floor produces a group that is the LIVE referenced
   leading group but is NOT the snapshot's leading group. classify flags it as a
   below_first_offset orphan; still_dangling's offset-only re-check confirms the
   delete; GC deletes a live referenced group. No UID collision is required - it
   is an ordinary GC x reset x retention race. */

/* An S3 object / manifest entry is identified by (offset, uid). */
type ObjKey = (offset: int, uid: int);

/* Objects carry a Kind: the data axis (FRAGMENT) and the group axis (GROUP)
   classify differently (classify/2 -> classify_group/3 for groups). */
enum Kind { FRAGMENT, GROUP }
type Obj = (offset: int, uid: int, kind: Kind, epoch: int);

/* Only the below_first_offset (offset) axis is modeled: the stale_epoch axis
   applies to manifest root objects only, never to fragments or groups. */
enum Reason { BELOW_FLOOR }
type Candidate = (offset: int, uid: int, kind: Kind, reason: Reason);

/* Khepri (durable consensus over the committed epoch). */
event eGetConsistent: (from: machine);
event eEpochResult: (ok: bool, epoch: int);

/* Manifest replica: the serialized owner of the live first_offset, the live
   (offset, uid) entries, and the leading-group carve-out info that
   leading_group_info/2 derives from the manifest's first entry. */
event eMRSetFloor: (from: machine, floor: int, isReset: bool);
event eMRAddEntry: (from: machine, key: ObjKey);
event eMRRemoveEntry: (from: machine, key: ObjKey);
event eMRSetLeading: (from: machine, leadingKey: ObjKey, hasLeading: bool, skipGroups: bool);
event eMRGetFloor: (from: machine);
event eMRFloorResult: int;
event eMRGetLeading: (from: machine);
event eMRLeadingResult: (leadingKey: ObjKey, hasLeading: bool, skipGroups: bool);
event eMRAck;

/* S3 object store. */
event eS3Put: (from: machine, obj: Obj);
event eS3Delete: (from: machine, key: ObjKey);
event eS3List: (from: machine);
event eS3ListResult: seq[Obj];
event eS3Has: (from: machine, key: ObjKey);
event eS3HasResult: bool;
event eS3Ack;

/* Writer: the reset orchestrator plus normal forward retention. A reset lowers
   the floor and installs a new leading group; retention then advances the floor
   forward, leaving the new leading group straddling it (partial expiry). */
event eDoReset: (from: machine, newFloor: int, newUid: int, oldLeading: ObjKey);
event eAdvanceFloor: (from: machine, newFloor: int);
event eResetDone;
event eAdvanceDone;

/* GC sweep steps, driven explicitly so a test driver can force the exact
   interleaving for the validation gate, or race them for exploration. */
event eGcSnapshot;
event eGcSnapshotDone;
event eGcListClassify;
event eGcClassifyDone;
event eGcExecute;
event eGcExecuteDone;

/* Spec events, delivered to monitors via announce. The AUTHORITATIVE manifest
   state drives them. */
event eObjectReferenced: ObjKey;
event eObjectUnreferenced: ObjKey;
event eFloorChanged: (newFloor: int, isReset: bool);
event eGcDelete: ObjKey;
