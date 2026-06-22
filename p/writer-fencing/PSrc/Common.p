/* Shared events for the writer/epoch fencing seam.

   rabbitmq_stream_s3_db:do_put/5 commits a manifest root with an optimistic lock:
   the put succeeds only if the current revision matches the expected one AND the
   new epoch is >= the stored epoch (the #if_data_matches {'>=', Epoch, '$1'}
   fence). The fence stops a deposed (lower-epoch) writer that has read the
   current revision from overwriting a newer writer's commit. This is what makes
   the committed epoch monotonic - the assumption the gc-reset model's epoch axis
   relies on. */

/* Khepri metadata store. */
event eGet: (from: machine);
event eGetResult: (uid: int, epoch: int, revision: int);
event ePut: (from: machine, expectedRev: int, epoch: int, uid: int);
event ePutOk: int;
event ePutConflict: (uid: int, epoch: int, revision: int);

/* Writer control. */
event eAttemptCommit;
event eCommitDone: bool;

/* Spec event (announce): a committed epoch. */
event eCommitted: int;
