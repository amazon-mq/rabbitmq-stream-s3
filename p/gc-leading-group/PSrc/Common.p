/* Shared types and events for the GC leading-group carve-out.

   classify_group (rabbitmq_stream_s3_gc) deletes a group object below
   first_offset as an orphan, EXCEPT the one leading group that still straddles
   the floor: retention advances first_offset INTO the leading group on partial
   expiry, so that group remains referenced even though its offset is below the
   floor. referenced_group_key carves it out of offset-based deletion. Removing
   the carve-out deletes a live group (a dangling reference). */

enum Kind { FRAGMENT, GROUP }
type ObjKey = (offset: int, uid: int);
type Obj = (offset: int, uid: int, kind: Kind);

/* Manifest replica: live floor, referenced entries, and the leading group key. */
event eMRGetFloor: (from: machine);
event eMRFloorResult: int;
event eMRGetLeadingGroup: (from: machine);
event eMRLeadingGroupResult: (has: bool, key: ObjKey);
event eMRAddEntry: (from: machine, key: ObjKey);
event eMRSetLeadingGroup: (from: machine, key: ObjKey);
event eMRAck;

/* S3 object store. */
event eS3Put: (from: machine, obj: Obj);
event eS3List: (from: machine);
event eS3ListResult: seq[Obj];
event eS3Delete: (from: machine, key: ObjKey);
event eS3Has: (from: machine, key: ObjKey);
event eS3HasResult: bool;
event eS3Ack;

/* GC sweep. */
event eGcSweep;
event eGcSweepDone;

/* Spec events (announce). */
event eObjectReferenced: ObjKey;
event eGcDelete: ObjKey;
