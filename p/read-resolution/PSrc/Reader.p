/* The reader resolving the 'first' offset spec (log_reader:resolve_first/2 ->
   fragment_iterator:next -> GetGroup -> resolve_first_lookup/1).

   When the remote tier is non-empty it descends into the leading group; the
   fetch outcome maps to a resolution:
     - ok                 -> {remote, _}
     - group_fetch_failed -> {retry, _}  (correct)  OR  {local, first}  (the
                             pre-e3f931b catch-all bug, toggled by bugCatchAll)
   When the remote tier is empty, 'first' is correctly served by the local tier. */
machine Reader {
  var bugCatchAll: bool;
  var manifest: machine;
  var groups: machine;
  var driver: machine;

  start state Idle {
    entry (init: (bugCatchAll: bool, manifest: machine, groups: machine, driver: machine)) {
      bugCatchAll = init.bugCatchAll;
      manifest = init.manifest;
      groups = init.groups;
      driver = init.driver;
    }
    on eResolveFirst do (p: (from: machine)) {
      var st: (nonEmpty: bool, remoteFirst: int, localFloor: int);
      var res: Resolution;
      send manifest, eGetRemoteState, (from = this,);
      receive {
        case eRemoteStateResult: (s: (nonEmpty: bool, remoteFirst: int, localFloor: int)) { st = s; }
      }
      if (!st.nonEmpty) {
        res = LOCAL_FIRST;
      } else {
        send groups, eFetchGroup, (from = this,);
        receive {
          case eFetchOk: { res = REMOTE; }
          case eFetchFailed: {
            if (bugCatchAll) {
              res = LOCAL_FIRST;
            } else {
              res = RETRY;
            }
          }
        }
      }
      announce eResolution, res;
      send p.from, eResolveDone, res;
    }
  }
}
