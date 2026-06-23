/* The S3 object store. Objects are keyed by (offset, uid); the stored value is
   the object's (kind, epoch). LIST returns whatever is present at the moment of
   the call, so a post-snapshot re-tier becomes visible to a sweep's classify. */
machine S3Store {
  var objects: map[ObjKey, (kind: Kind, epoch: int)];

  start state Serving {
    on eS3Put do (p: (from: machine, obj: Obj)) {
      var k: ObjKey;
      k = (offset = p.obj.offset, uid = p.obj.uid);
      if (k in objects) {
        objects[k] = (kind = p.obj.kind, epoch = p.obj.epoch);
      } else {
        objects += (k, (kind = p.obj.kind, epoch = p.obj.epoch));
      }
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
      var out: seq[Obj];
      var ks: seq[ObjKey];
      var i: int;
      var k: ObjKey;
      ks = keys(objects);
      i = 0;
      while (i < sizeof(ks)) {
        k = ks[i];
        out += (sizeof(out),
          (offset = k.offset, uid = k.uid, kind = objects[k].kind, epoch = objects[k].epoch));
        i = i + 1;
      }
      send p.from, eS3ListResult, out;
    }
  }
}
