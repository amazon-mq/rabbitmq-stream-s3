/* A single per-node manifest replica cache (rabbitmq_stream_s3_manifest_replica).
   This is the lagging, non-writer node that an operator CLI sweep happens to
   target. It holds a cached first_offset (floor) and the epoch that floor was
   synced at - the real cache tracks {seq, epoch, writer_node} per stream for gap
   detection, but get_manifest/1 (the read GC uses) drops the epoch, which is the
   crux of the seam.

   The cache learns of a reset only via eMRSync, a fire-and-forget cast. It is
   applied only when the incoming epoch is at least the cached one (modeling
   is_stale_sync, where epoch dominates sequence), so a higher epoch always wins
   and a stale delayed sync is rejected. When the sync is never delivered the
   cache keeps its stale-high floor at its old epoch. The cache never announces
   to the monitors: it is not the source of truth, only a possibly-stale view. */
machine ManifestReplica {
  var floor: int;
  var epoch: int;

  start state Serving {
    entry (init: (floor: int, epoch: int)) {
      floor = init.floor;
      epoch = init.epoch;
    }

    /* Fire-and-forget sync from the writer. is_stale_sync: apply only if the
       incoming epoch is not behind the cached epoch. No ack (cast). */
    on eMRSync do (p: (from: machine, floor: int, epoch: int)) {
      if (p.epoch >= epoch) {
        floor = p.floor;
        epoch = p.epoch;
      }
    }

    /* get_manifest/1: returns only the cached floor (epoch dropped) - the read
       still_dangling/1 re-uses, hence its blindness to cache staleness. */
    on eMRGetFloor do (p: (from: machine)) {
      send p.from, eMRFloorResult, floor;
    }

    /* The epoch-aware read the fix needs: returns the cached floor together with
       the epoch it was synced at, so build_lookup can compare it to the
       committed epoch and fail closed when the cache is behind. */
    on eMRGetFloorEpoch do (p: (from: machine)) {
      send p.from, eMRFloorEpochResult, (floor = floor, epoch = epoch);
    }
  }
}
