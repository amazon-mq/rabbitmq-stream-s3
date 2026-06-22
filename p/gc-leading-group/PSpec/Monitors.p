/* INV#2 (no dangling reference): GC must never delete a referenced object. Here
   the protected object is the leading group, which is referenced by the manifest
   even though its offset is below the live floor. */
spec NoDanglingReference observes eObjectReferenced, eGcDelete {
  var live: set[ObjKey];

  start state Watching {
    on eObjectReferenced do (k: ObjKey) {
      live += (k);
    }
    on eGcDelete do (k: ObjKey) {
      assert !(k in live),
        format("INV#2 violated: GC deleted referenced object (offset={0}, uid={1})",
          k.offset, k.uid);
    }
  }
}
