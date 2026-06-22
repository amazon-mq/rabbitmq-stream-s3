/* The S3 object store, holding fragments and groups keyed by (offset, uid). */
machine S3Store {
  var objects: map[ObjKey, Obj];

  start state Serving {
    on eS3Put do (p: (from: machine, obj: Obj)) {
      var k: ObjKey;
      k = (offset = p.obj.offset, uid = p.obj.uid);
      if (k in objects) {
        objects[k] = p.obj;
      } else {
        objects += (k, p.obj);
      }
      send p.from, eS3Ack;
    }
    on eS3List do (p: (from: machine)) {
      send p.from, eS3ListResult, values(objects);
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
  }
}
