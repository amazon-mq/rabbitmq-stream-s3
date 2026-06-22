/* Shared events for the #225 trimmed-segment seam.

   A head fragment (first offset == manifest next_offset) is submitted for
   transfer; the upload worker preads the local segment. If local retention
   trims the log past next_offset first, the segment is permanently gone and the
   pread fails (enoent). handle_transfer_failure must then recover via
   restart_at_local_floor (the local_log_ahead branch) rather than resubmit
   forever, otherwise tiering wedges and local disk grows unbounded. */

/* Local log (osiris segments) and retention trim. */
event eQueryFloor: (from: machine);
event eFloorResult: (localFirst: int, nextOffset: int);
event eReadSegment: (from: machine);
event eReadOk;
event eReadTrimmed;
event eTrim: (from: machine);
event eTrimAck;

/* Upload worker (governor task). */
event eDoUpload: (reader: machine, log: machine);
event eTransferResult: (ok: bool);

/* Reader control. */
event eStartTransfer;

/* Spec events (announce). */
event eTransferSubmitted;
event eTransferResolved;
event eFrontierReset: (target: int);
