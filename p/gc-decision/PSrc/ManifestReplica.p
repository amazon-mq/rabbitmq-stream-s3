/* A single per-node manifest replica cache (rabbitmq_stream_s3_manifest_replica),
   the lagging non-writer node an operator CLI sweep happens to target. It holds
   the cached manifest view: floor, the epoch that view was synced at, and the
   leading-group carve-out (referenced leading group and the conservative
   skip-groups flag) that build_lookup's lookup_entry derives from the manifest.

   The cache learns of a committed change only via eMRSync, a fire-and-forget
   cast applied only when the incoming epoch is not behind the cached epoch
   (is_stale_sync, where epoch dominates). When the sync is never delivered the
   cache keeps its stale view at its old epoch. The cache never announces to the
   monitors: it is a possibly-stale view, not the source of truth.

   get_manifest_and_epoch (eMRGetFull) returns the view WITH its epoch, the read
   build_lookup uses for the epoch gate. get_manifest (eMRGetLive) is the read
   still_dangling re-uses; the real function drops the epoch, so callers there
   must not depend on the returned epoch field. */
machine ManifestReplica {
  var view: ManifestView;

  start state Serving {
    entry (init: (view: ManifestView)) {
      view = init.view;
    }

    on eMRSync do (p: (from: machine, view: ManifestView)) {
      if (p.view.epoch >= view.epoch) {
        view = p.view;
      }
    }

    on eMRGetFull do (p: (from: machine)) {
      send p.from, eMRFullResult, view;
    }

    on eMRGetLive do (p: (from: machine)) {
      send p.from, eMRLiveResult, view;
    }
  }
}
