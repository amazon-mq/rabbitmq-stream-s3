/* Shared types and events for the anchor-before-first-fragment cleanup design.

   The design:

     - Before the very first S3 fragment is written for a stream, the replica
       reader writes a dedicated ANCHOR node in Khepri, keep_while'd to the queue,
       as a BLOCKING step. The anchor's write must COMMIT strictly before the
       first S3 PUT.
     - Cleanup is driven by the anchor's ABSENCE, not by any record written at
       deletion time. The keep_while removes the anchor in the same transaction
       that deletes the queue, so "anchor absent" is produced atomically and is
       permanent. A sweep classifies the prefix as junk iff objects are present
       AND the anchor is absent.

   This is correct by construction off one ordering invariant: an object exists
   only after the anchor committed, and the anchor disappears only when the queue
   does, so there is NO live state with objects-but-no-anchor. Two properties
   fall out for free: a stream that never committed a manifest still has an
   anchor (it predates the first fragment), and a crash anywhere in the deletion
   path cannot lose the signal (the signal is the anchor's permanent absence, not
   a write that can be dropped).

   TWO things are load-bearing, and this model exists to prove both:

     1. CONSISTENT READS. The sweep must read the anchor with a strongly-consistent
        (committed) read. A stale local replica can report "anchor absent" for a
        stream whose anchor just committed while S3 already shows its first
        fragment, and a stale-read sweep then reaps LIVE data. (Same shape as the
        gc-reset-multinode stale-floor finding.)

     2. ORDERING. The anchor must commit BEFORE the first fragment PUT. If a
        fragment can exist before the anchor commits, a sweep in that window sees
        objects-but-no-anchor for a LIVE stream and reaps it, even with consistent
        reads. */

/* An S3 object under the stream's prefix, identified by (offset, uid). One
   stream is modeled; every object here belongs to that stream's prefix. */
type ObjKey = (offset: int, uid: int);

/* S3 object store (the stream's prefix). */
event eS3Put: (from: machine, key: ObjKey);
event eS3DeleteAll: (from: machine);
event eS3List: (from: machine);
event eS3ListResult: int;
event eS3Ack;

/* Khepri: the committed truth (queue + anchor) plus a lagging local replica
   cache. A consistent read returns the committed anchor; a local read returns the
   cached anchor, which may be stale. */
event eWriteAnchor: (from: machine);
event eReplicate: (from: machine);
event eDeleteQueue: (from: machine);
event eReadAnchor: (from: machine, consistent: bool);
event eAnchorResult: bool;
event eKAck;

/* Writer / replica reader: ensure the anchor, then PUT the first fragment.
   anchorBeforeFragment selects whether the load-bearing ordering is honored. */
event eEnsureAnchorThenPut: (from: machine, key: ObjKey, anchorBeforeFragment: bool);
event eWriterDone;

/* GC sweep: list the prefix, read the anchor, reap iff objects present and anchor
   absent. consistent selects the read mode; crashBeforeDelete drops the reap to
   model a crash mid-sweep (the next sweep must still be correct). */
event eSweep: (from: machine, consistent: bool, crashBeforeDelete: bool);
event eSweepDone;

/* Spec events, delivered to monitors via announce. */
event eObjStored: ObjKey;
event eObjReaped: ObjKey;
event eQueueDeleted;
event eSweepQuiesced;
