/* The S3 object store for the stream's prefix. A LIST returns the current object
   count; a reap deletes the whole prefix. Each store and each delete is announced
   so the monitors can tell a live-stream reap from a legitimate junk reap. */
machine S3Store {
  var objects: map[ObjKey, bool];

  start state Serving {
    on eS3Put do (p: (from: machine, key: ObjKey)) {
      if (!(p.key in objects)) {
        objects += (p.key, true);
      }
      announce eObjStored, p.key;
      send p.from, eS3Ack;
    }
    on eS3DeleteAll do (p: (from: machine)) {
      var ks: seq[ObjKey];
      var i: int;
      ks = keys(objects);
      i = 0;
      while (i < sizeof(ks)) {
        announce eObjReaped, ks[i];
        i = i + 1;
      }
      objects = default(map[ObjKey, bool]);
      send p.from, eS3Ack;
    }
    on eS3List do (p: (from: machine)) {
      send p.from, eS3ListResult, sizeof(objects);
    }
  }
}
