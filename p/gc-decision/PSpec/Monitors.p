/* Global safety monitor for the integrated GC decision. It observes the
   AUTHORITATIVE committed manifest state announced by the writer and drivers and
   asserts GC never deletes a currently-referenced object. The lagging cache is
   never a source of these events, so "referenced" always means what the committed
   manifest references, regardless of which node's cache the sweep read.

   This is INV#2 (no dangling-reference deletion) generalized over all three object
   kinds at once: a data fragment at or above the live floor, a manifest at the
   committed epoch, and the referenced leading group below the floor are all live
   and all protected by the same assertion. */
spec NoDanglingReference observes eObjectReferenced, eObjectUnreferenced, eGcDelete {
  var live: set[Obj];

  start state Watching {
    on eObjectReferenced do (o: Obj) {
      live += (o);
    }
    on eObjectUnreferenced do (o: Obj) {
      if (o in live) {
        live -= (o);
      }
    }
    on eGcDelete do (o: Obj) {
      assert !(o in live),
        format("INV#2 violated: GC deleted live object (kind={0}, offset={1}, uid={2}, epoch={3}) still referenced by the committed manifest",
          o.kind, o.offset, o.uid, o.epoch);
    }
  }
}
