/* The S3 object store. Objects are identified by all of (kind, offset, uid,
   epoch). LIST returns whatever is present at the moment of the call. The
   committed reset has already put any re-tiered live object here (the durable
   side of the reset is done); what the model explores is whether the lagging
   cache the sweep reads knows about the lowered floor. */
machine S3Store {
  var objects: set[Obj];

  start state Serving {
    on eS3Put do (p: (from: machine, obj: Obj)) {
      objects += (p.obj);
      send p.from, eS3Ack;
    }
    on eS3Delete do (p: (from: machine, obj: Obj)) {
      if (p.obj in objects) {
        objects -= (p.obj);
      }
      send p.from, eS3Ack;
    }
    on eS3Has do (p: (from: machine, obj: Obj)) {
      send p.from, eS3HasResult, (p.obj in objects);
    }
    on eS3List do (p: (from: machine)) {
      send p.from, eS3ListResult, objects;
    }
  }
}
