/* The S3 object store. Objects are keyed by (offset, uid). LIST returns whatever
   is present at the moment of the call. The committed reset has already put the
   re-tiered live object here (the durable side of the reset is done); the open
   question the model explores is whether the lagging cache the sweep reads knows
   about the lowered floor. */
machine S3Store {
  var objects: set[ObjKey];

  start state Serving {
    on eS3Put do (p: (from: machine, key: ObjKey)) {
      objects += (p.key);
      send p.from, eS3Ack;
    }
    on eS3Delete do (p: (from: machine, key: ObjKey)) {
      if (p.key in objects) {
        objects -= (p.key);
      }
      send p.from, eS3Ack;
    }
    on eS3Has do (p: (from: machine, key: ObjKey)) {
      send p.from, eS3HasResult, (p.key in objects);
    }
    on eS3List do (p: (from: machine)) {
      send p.from, eS3ListResult, objects;
    }
  }
}
