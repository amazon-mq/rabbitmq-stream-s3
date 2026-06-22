/* Shared events for the orphan-leak seam.

   The reaper issues batched DeleteObjects and tolerates partial failures: a
   per-key failure is a routine transient and the unconfirmed object is "left
   for orphan GC" (rabbitmq_stream_s3_reaper:delete_batch/1). Reclamation
   liveness therefore rests on GC RE-SWEEPING: the next sweep re-lists and
   re-deletes what a previous delete missed. Without the re-run a transiently
   failed delete leaks forever. */

event eList: (from: machine);
event eListResult: seq[int];
event eDelete: (from: machine, id: int);
event eDeleteOk: int;
event eDeleteFailed: int;
event eAddOrphan: (from: machine, id: int);
event eAddAck;

event eSweep;
event eSweepFinished;

/* Spec events (announce). */
event eOrphanCreated: int;
event eOrphanDeleted: int;
