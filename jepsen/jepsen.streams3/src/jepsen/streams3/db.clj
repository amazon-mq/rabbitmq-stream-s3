(ns jepsen.streams3.db
  "Installs, configures, starts and stops a RabbitMQ node with the
  rabbitmq_stream_s3 plugin on each DB node.

  The broker is installed from a *generic-unix* tarball produced by
  `make package-generic-unix` at the umbrella root (it bundles every deps/
  plugin, including rabbitmq_stream_s3 — no release is required). The tarball
  is made available to every node at /shared/rabbitmq.tar.xz via a volume in
  docker-compose; db setup installs it under /opt/rabbitmq.

  The S3 tier points at the MinIO sidecar. The AWS backend
  (rabbitmq_stream_s3_api_aws) is hardwired to TLS/443 with verify_peer and
  uses virtual-hosted-style addressing (Host = <bucket>.<endpoint>), so:
    * the node trusts the test CA (resources/ca.crt -> system store),
    * region_endpoints.<region> resolves <bucket>.<endpoint> to Toxiproxy,
      which L4-passes through to MinIO:443,
    * static access_key_id/secret_key are configured so the IMDS/container
      credential paths are never used."
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.streams3.client :as sc]))

(def base-dir "/opt/rabbitmq")
(def tarball "file:///shared/rabbitmq.tar.xz")
(def ca-cert "/shared/ca.crt")
(def conf-file (str base-dir "/etc/rabbitmq/rabbitmq.conf"))
;; RABBITMQ_CONFIG_FILE is given WITHOUT the .conf extension; the broker
;; appends it. The file on disk is conf-file (with .conf).
(def conf-file-base (str base-dir "/etc/rabbitmq/rabbitmq"))
(def enabled-plugins-file (str base-dir "/etc/rabbitmq/enabled_plugins"))
(def log-dir (str base-dir "/var/log/rabbitmq"))
(def erlang-cookie "JEPSENSTREAMS3COOKIE")

;; The AWS backend builds the endpoint host the AWS way: `s3.<region>.<tld>`
;; (cf. s3.us-east-1.amazonaws.com), where <tld> is the region_endpoints value.
;; So region=jepsen + tld=local yields connection host `s3.jepsen.local` and
;; per-request virtual-host `jepsen.s3.jepsen.local`. docker-compose aliases
;; both `s3.jepsen.local` and `jepsen.s3.jepsen.local` to Toxiproxy, the cert
;; covers them, and MINIO_DOMAIN=s3.jepsen.local routes the bucket. (Setting the
;; tld to the full `s3.jepsen.local` would wrongly yield s3.jepsen.s3.jepsen.local.)
(def s3-region   "jepsen")
(def s3-region-tld "local")
(def s3-bucket   "jepsen")
(def s3-access-key "jepsenjepsen")
(def s3-secret-key "jepsenjepsenjepsen")

(defn enabled-plugins []
  ;; enabled_plugins is an Erlang term: a LIST of plugin atoms.
  "[rabbitmq_stream,rabbitmq_stream_management,rabbitmq_stream_s3].")

(defn rabbitmq-conf
  "Renders rabbitmq.conf. `nodes` is the full node list; node 0 is the seed.
  Retention is intentionally generous: we want data to *tier to S3*
  (small fragments + tiering) so reads exercise the remote tier, but we do NOT
  want the local floor to trim past the upload seam, which would lose data the
  plugin never promised to keep ([f,n) durability only — see docs/invariants.md).
  Aggressive-retention scenarios are still to come, with a bounded-durability
  checker."
  [test node]
  (str/join
    "\n"
    [;; --- listeners ---
     "listeners.tcp.default = 5672"
     "stream.listeners.tcp.default = 5552"
     "management.tcp.port = 15672"
     ;; The jepsen client connects as guest from the control node (a remote
     ;; host), so lift the default loopback-only restriction on guest.
     "loopback_users = none"
     ;; --- clustering ---
     "cluster_formation.peer_discovery_backend = classic_config"
     (str/join
       "\n"
       (map-indexed
         (fn [i n] (str "cluster_formation.classic_config.nodes." (inc i)
                        " = rabbit@" (name n)))
         (:nodes test)))
     ;; --- S3 tier: point the AWS backend at MinIO via Toxiproxy ---
     (str "stream_s3.region = " s3-region)
     (str "stream_s3.region_endpoints." s3-region " = " s3-region-tld)
     (str "stream_s3.access_key_id = " s3-access-key)
     (str "stream_s3.secret_key = " s3-secret-key)
     (str "stream_s3.bucket = " s3-bucket)
     ;; Small fragments + frequent persists so tiering and manifest replication
     ;; actually happen within a short test run. Combined with the small stream
     ;; segment size and padded messages set by the client, this makes data
     ;; really upload to S3 and local segments really trim, so the S3 nemeses
     ;; have live traffic to disrupt and reads genuinely fall back to S3.
     "stream_s3.fragment_target_size = 16KB"
     "stream_s3.persist_interval_ms = 500"
     "stream_s3.verbose_logging = true"
     ;; Keep transfers from saturating the link so S3-latency faults are visible.
     "stream_s3.max_transfer_bytes_per_sec = 50MB"
     ""]))

(defn install!
  [test node]
  (info node "installing RabbitMQ + rabbitmq_stream_s3")
  ;; Erlang 27 is provided by the node base image (erlang:27); we only need
  ;; the CA tooling here to trust the test S3 CA.
  (debian/install [:openssl :ca-certificates])
  (cu/install-archive! tarball base-dir)
  ;; Trust the test CA so verify_peer against MinIO's cert succeeds.
  (c/su
    (c/exec :cp ca-cert "/usr/local/share/ca-certificates/jepsen-s3-ca.crt")
    (c/exec :update-ca-certificates))
  (c/exec :mkdir :-p (str base-dir "/etc/rabbitmq") log-dir)
  (cu/write-file! (enabled-plugins) enabled-plugins-file)
  (cu/write-file! (rabbitmq-conf test node) conf-file)
  ;; Shared Erlang cookie so nodes can cluster.
  (cu/write-file! erlang-cookie "/root/.erlang.cookie")
  (c/exec :chmod :600 "/root/.erlang.cookie"))

(defn env-args
  "Renders the broker environment as `VAR=value` args for `env`."
  [node]
  (map (fn [[k v]] (str (name k) "=" v))
       {:RABBITMQ_BASE base-dir
        :RABBITMQ_CONFIG_FILE conf-file-base
        :RABBITMQ_ENABLED_PLUGINS_FILE enabled-plugins-file
        :RABBITMQ_LOG_BASE log-dir
        :RABBITMQ_NODENAME (str "rabbit@" (name node))
        :HOME "/root"}))

(defn rabbitmqctl [node & args]
  (apply c/exec :env (concat (env-args node)
                             [(str base-dir "/sbin/rabbitmqctl")] args)))

;; Seconds to bound an Erlang eval. rabbitmqctl eval has no timeout, so a broker
;; wedged by leader-move churn would hang the eval (and the whole test) forever.
(def ^:private eval-timeout-secs "30")

(defn rabbitmqctl-eval
  "Runs an Erlang eval on `node`, bounded by the `timeout` command so a wedged
  broker fails the eval (exit 124) instead of hanging the test; the broker-side
  eval may keep running, harmlessly. Returns stdout; throws on timeout, so
  callers wrap with util/meh or try/catch."
  [node eval-str]
  (apply c/exec :timeout eval-timeout-secs :env
         (concat (env-args node)
                 [(str base-dir "/sbin/rabbitmqctl") :eval eval-str])))

;; One rabbitmqctl eval that, on the local node, triggers local-retention
;; evaluation for every jepsen-<k> stream — looping in Erlang rather than
;; spawning a slow CLI per stream. evaluate_local_retention no-ops where the
;; node holds no running member, so running this on every node reaches each
;; stream's writer.
(def ^:private trim-eval
  (str "Names = [element(4, amqqueue:get_name(Q)) "
       "|| Q <- rabbit_amqqueue:list(<<\"/\">>)], "
       "[catch rabbitmq_stream_s3_replica_reader:evaluate_local_retention(<<\"/\">>, N) "
       "|| N <- Names, byte_size(N) >= 7, binary:part(N, 0, 7) =:= <<\"jepsen-\">>], ok."))

(defn force-local-trim!
  "Triggers local-retention evaluation for the jepsen streams on `node`, so
  segments already uploaded to S3 are trimmed locally and reads of older
  offsets fall back to the S3 tier. Runs in an SSH context (caller wraps with
  c/on-nodes)."
  [node]
  (util/meh (rabbitmqctl-eval node trim-eval)))

;; One rabbitmqctl eval that transfers ONE randomly chosen jepsen-<k> stream's
;; leader to its first replica, moving that writer (and uploader) to a new node
;; and bumping the stream's epoch. The stream coordinator is cluster-global, so
;; this runs on a single node. transfer_leadership restarts the stream with the
;; target preferred as leader; replica_nodes excludes the current leader, so the
;; leader genuinely moves and the epoch increments.
;;
;; One stream per tick, not all of them: moving every stream at once repeatedly
;; keeps the whole cluster's subscriptions in perpetual recovery and can wedge it
;; so a run hangs before the final reads. One at a time still fences a writer
;; each tick (and over a run bumps epochs past 1, which the tiering checker
;; requires), while leaving the cluster room to recover between moves. Fencing
;; correctness is verified by the durability checker, which reads each stream
;; fresh after the run regardless of how aggressive the churn was.
(def ^:private leader-move-eval
  (str "Names = [element(4, amqqueue:get_name(Q)) "
       "|| Q <- rabbit_amqqueue:list(<<\"/\">>)], "
       "Jeps = [N || N <- Names, byte_size(N) >= 7, binary:part(N, 0, 7) =:= <<\"jepsen-\">>], "
       "case Jeps of "
       "[] -> ok; "
       "_ -> "
       "N = lists:nth(rand:uniform(length(Jeps)), Jeps), "
       "Q = element(2, rabbit_amqqueue:lookup(rabbit_misc:r(<<\"/\">>, queue, N))), "
       "case maps:get(replica_nodes, amqqueue:get_type_state(Q), []) of "
       "[Target | _] -> catch rabbit_stream_queue:transfer_leadership(Q, Target); "
       "[] -> ok "
       "end "
       "end, ok."))

(defn force-leader-move!
  "Transfers one randomly chosen jepsen stream's leader to its first replica,
  moving that writer (and uploader) to a new node and bumping the stream's epoch.
  This fences the deposed writer: a straggler upload carrying the old epoch must
  be rejected (a manifest persist conflict), never overwrite the new leader's
  data. Run on a single node (the coordinator is cluster-global). Runs in an SSH
  context."
  [node]
  (util/meh (rabbitmqctl-eval node leader-move-eval)))

;; The plugin's read/write paths are instrumented, not logged, so these
;; counters are how the test proves S3 was actually exercised (see the
;; coverage checker). The management/Prometheus listener is on 15692.
(def tiering-metrics
  ["rabbitmq_stream_s3_transfers_completed"  ; fragments uploaded to S3
   "rabbitmq_stream_s3_get_range"            ; range GETs (remote reads) from S3
   "rabbitmq_stream_s3_read"                 ; remote_reader read calls
   "rabbitmq_stream_s3_resolve"              ; read-path offset resolutions
   ;; Writer-fencing evidence: a deposed leader's straggler upload is rejected
   ;; on a Khepri epoch conflict, and stale manifest syncs/resyncs are dropped
   ;; or re-requested when the epoch moves under the leader-move nemesis.
   "rabbitmq_stream_s3_persist_conflicts"
   "rabbitmq_stream_s3_syncs_rejected"
   "rabbitmq_stream_s3_resyncs_requested"
   ;; The manifest-replica sync-context fix's gated path: a sync arriving for a
   ;; stream with no registered replica context is dropped (rather than
   ;; re-creating an unmonitored cache row) and a resync is requested once a
   ;; context registers. The replica-consistency checker reports this as evidence
   ;; the A2 guard actually engaged.
   "rabbitmq_stream_s3_syncs_dropped_no_context"
   ;; S3-disruption evidence, reported for visibility (not asserted): the plugin
   ;; tolerates an s3-outage gracefully (writes stay local, uploads pause and
   ;; retry), so these often stay 0 even when the link is cut. The Toxiproxy
   ;; status check (nemesis.clj) is what proves the outage was actually injected.
   "rabbitmq_stream_s3_transfers_failed"
   "rabbitmq_stream_s3_put_errors"
   "rabbitmq_stream_s3_request_timeouts"])

(defn scrape-tiering-metrics
  "Scrapes the plugin's Prometheus counters on `node`, returning a map of
  metric name -> summed value (over all label sets). Runs in an SSH context."
  [node]
  (let [out   (try (c/exec :curl :-s "http://localhost:15692/metrics")
                   (catch Exception _ ""))
        lines (str/split-lines out)]
    (into {}
          (for [m tiering-metrics]
            [m (->> lines
                    (keep (fn [l]
                            (when-let [[_ v] (re-find
                                               (re-pattern
                                                 (str "^" m "(?:\\{[^}]*\\})?\\s+([0-9.]+)"))
                                               l)]
                              (parse-double v))))
                    (reduce + 0.0))]))))

;; The maximum stream coordinator epoch across the jepsen streams. The epoch
;; starts at 1 and increments on every leader move, so it is how the coverage
;; checker proves the writer-fencing (leader-move) path actually ran. It is a
;; cluster-global value, so querying any one node suffices.
(def ^:private max-epoch-eval
  (str "Names = [element(4, amqqueue:get_name(Q)) "
       "|| Q <- rabbit_amqqueue:list(<<\"/\">>)], "
       "Jeps = [N || N <- Names, byte_size(N) >= 7, binary:part(N, 0, 7) =:= <<\"jepsen-\">>], "
       "Epochs = [maps:get(epoch, amqqueue:get_type_state(element(2, "
       "rabbit_amqqueue:lookup(rabbit_misc:r(<<\"/\">>, queue, N)))), 1) || N <- Jeps], "
       "lists:max([1 | Epochs])."))

(defn scrape-max-epoch
  "Returns the maximum stream coordinator epoch across the jepsen streams on
  `node` (a cluster-global value). Runs in an SSH context."
  [node]
  (let [out (try (rabbitmqctl-eval node max-epoch-eval) (catch Exception _ ""))]
    (or (some-> (re-find #"\d+" (str out)) parse-long) 0)))

;; For each jepsen-<k> stream, the integer key and its committed offset (the
;; last durably-committed offset, -1 if empty). The durability checker reads
;; exactly these streams, and the authoritative read waits until each consumer
;; reaches this offset rather than guessing the tail from idle time.
(def ^:private committed-offsets-eval
  (str "Names = [element(4, amqqueue:get_name(Q)) "
       "|| Q <- rabbit_amqqueue:list(<<\"/\">>)], "
       "Jeps = [N || N <- Names, byte_size(N) >= 8, binary:part(N, 0, 7) =:= <<\"jepsen-\">>], "
       "[begin "
       "K = binary_to_integer(binary:part(N, 7, byte_size(N) - 7)), "
       "Co = case catch rabbit_stream_queue:status(<<\"/\">>, N) of "
       "L when is_list(L) -> lists:max([-1 | [proplists:get_value(committed_offset, M, -1) || M <- L]]); "
       "_ -> -1 "
       "end, "
       "{K, Co} "
       "end || N <- Jeps]."))

(defn committed-offsets
  "Returns {key -> committed offset} for the jepsen streams on `node` (-1 for an
  empty stream). Runs in an SSH context."
  [node]
  (let [out  (try (rabbitmqctl-eval node committed-offsets-eval) (catch Exception _ ""))
        ints (map parse-long (re-seq #"-?\d+" (str out)))]
    (into {} (map vec (partition 2 ints)))))

;; For each jepsen-<k> stream, this node's view of the manifest-replica cache
;; (ETS table rabbitmq_stream_s3_manifest_cache, rows {StreamId, Manifest, Epoch})
;; plus the stream's cluster-global leader. Per stream we emit six integers:
;;   {K, Cached, Floor, Epoch, Ctx, IsLeader}
;; where
;;   * Cached   1 if this node holds a cache row for the stream, else 0
;;   * Floor    the cached manifest's first_offset (the remote-tier floor), read
;;              via get_range/1 whose first element IS the manifest first_offset;
;;              0 for an empty (entries = <<>>) cached manifest, -1 if not cached
;;   * Epoch    the writer epoch the row was stored at (-1 if cached without one)
;;   * Ctx      1 if a replica context (a monitored osiris member) is registered
;;              on this node for the stream, else 0
;;   * IsLeader 1 if this node is the stream's current leader, else 0
;; The leader's own cache row is written by the writer path (put_manifest), not by
;; a monitored replica context, so it legitimately has Ctx = 0; IsLeader lets the
;; checker exclude it from the leaked-row test. All-integer output so it parses the
;; same way as committed-offsets-eval.
(def ^:private replica-cache-eval
  (str "Qs = [Q || Q <- rabbit_amqqueue:list(<<\"/\">>), "
       "begin N = element(4, amqqueue:get_name(Q)), "
       "byte_size(N) >= 8 andalso binary:part(N, 0, 7) =:= <<\"jepsen-\">> end], "
       "Mod = rabbitmq_stream_s3_manifest_replica, Self = node(), "
       "[begin "
       "Name = element(4, amqqueue:get_name(Q)), "
       "K = binary_to_integer(binary:part(Name, 7, byte_size(Name) - 7)), "
       "TS = amqqueue:get_type_state(Q), "
       ;; The stream_id is the manifest cache's ETS key, a binary; type_state
       ;; carries it as a char list, so coerce it or every lookup misses.
       "SId = iolist_to_binary(maps:get(name, TS)), "
       "IsLeader = case maps:get(leader_node, TS, undefined) of Self -> 1; _ -> 0 end, "
       "{Cached, Floor, Epoch, Ctx} = "
       "case catch Mod:get_manifest_and_epoch(SId) of "
       "{_M, E} -> "
       "Fl = case catch Mod:get_range(SId) of {Fst, _} -> Fst; _ -> 0 end, "
       "Ep = case E of Ei when is_integer(Ei) -> Ei; _ -> -1 end, "
       "Cx = case catch Mod:is_context_registered(SId) of true -> 1; _ -> 0 end, "
       "{1, Fl, Ep, Cx}; "
       "_ -> {0, -1, -1, 0} "
       "end, "
       "{K, Cached, Floor, Epoch, Ctx, IsLeader} "
       "end || Q <- Qs]."))

(defn replica-cache
  "Returns {key -> {:cached? :floor :epoch :ctx? :leader?}} describing the
  manifest-replica cache state on `node` for each jepsen stream, where :leader?
  is whether `node` is the stream's current leader (a cluster-global fact). Runs
  in an SSH context."
  [node]
  (let [out  (try (rabbitmqctl-eval node replica-cache-eval) (catch Exception _ ""))
        ints (map parse-long (re-seq #"-?\d+" (str out)))]
    (into {}
          (for [[k cached floor epoch ctx leader] (partition 6 ints)]
            [k {:cached? (= 1 cached)
                :floor   floor
                :epoch   epoch
                :ctx?    (= 1 ctx)
                :leader? (= 1 leader)}]))))

;; Cluster-wide S3 counter sums for the run, accumulated across nodes in
;; log-files (which runs after the workload but before the checker, while the
;; brokers are still up). The coverage checker reads this. Collecting it here
;; rather than as a history op keeps it out of the kafka checker's analysis,
;; which chokes on a trailing non-client op.
(defonce tiering-stats (atom {}))

(defn record-tiering-stats!
  "Folds `node`'s S3 state into the shared per-run totals: counters are summed
  across nodes, the epoch is maxed (it is cluster-global, so the max is exact)."
  [node]
  (swap! tiering-stats
         (fn [acc]
           (-> (merge-with + acc (scrape-tiering-metrics node))
               (update "max_epoch" (fnil max 0) (scrape-max-epoch node))))))

;; Per-node manifest-replica cache snapshots for the run, captured in log-files
;; (brokers still up) alongside record-tiering-stats!. node -> {key -> {:cached?
;; :floor :epoch :ctx? :leader?}}. The replica-consistency checker reads this to
;; assert the caches converged and served no stale floor or leaked row.
(defonce replica-floors (atom {}))

;; The committed offset of each jepsen stream at the end of the run (the same
;; scrape capture-authoritative-reads! uses), captured once on the primary in
;; log-files. The replica-consistency checker reads this as the oracle for the
;; stale-floor test. key -> committed offset (-1 for an empty stream).
(defonce committed-offsets-snapshot (atom {}))

(defn record-replica-cache!
  "Stores `node`'s manifest-replica cache snapshot into the shared per-run map."
  [node]
  (swap! replica-floors assoc node (replica-cache node)))

(defn start! [test node]
  (info node "starting broker")
  (apply c/exec :env (concat (env-args node)
                             [(str base-dir "/sbin/rabbitmq-server") :-detached]))
  ;; rabbitmq-server -detached returns before the node is reachable (the beam
  ;; needs a moment to register with epmd), and await_startup errors rather
  ;; than waiting when it cannot connect. Poll it until the node is both
  ;; reachable and fully started — which, with classic_config, also covers the
  ;; window where this node is still joining the cluster.
  (util/await-fn (fn [] (rabbitmqctl node :await_startup))
                 {:retry-interval 1000
                  :timeout 120000
                  :log-message (str "Waiting for broker on " (name node))}))

(defn stop! [test node]
  (info node "stopping broker")
  (util/meh (rabbitmqctl node :shutdown)))

(def probe-stream-url
  "http://localhost:15672/api/queues/%2F/jepsen-probe-stream")

(defn await-stream-ready
  "Waits until a stream can actually be created. Right after the cluster forms
  the stream coordinator may still be electing a leader, so the workload's
  first creates/producers/consumers would crash; this gates the workload on the
  subsystem being ready by creating (and deleting) a probe stream via the
  management API. Run once on the primary via setup-primary!."
  [node]
  (util/await-fn
    (fn []
      (let [code (c/exec :curl :-s :-o "/dev/null" :-w "%{http_code}"
                         :-u "guest:guest" :-H "content-type: application/json"
                         :-X "PUT" probe-stream-url
                         :-d "{\"durable\":true,\"arguments\":{\"x-queue-type\":\"stream\"}}")]
        (when-not (#{"201" "204"} code)
          (throw (ex-info "stream subsystem not ready" {:http-code code})))))
    {:retry-interval 1000
     :timeout 120000
     :log-message (str "Waiting for stream subsystem on " (name node))})
  (util/meh
    (c/exec :curl :-s :-o "/dev/null" :-u "guest:guest" :-X "DELETE" probe-stream-url)))

(defn db
  "RabbitMQ stream_s3 DB. One generic-unix tarball install per node."
  []
  (reify db/DB
    (setup! [_ test node]
      ;; Clean slate for this run: coverage counters, authoritative reads, and
      ;; the shared client connection state (so a REPL or test-all multi-run in
      ;; one JVM does not carry stale stream declarations or publishingId
      ;; counters against a freshly-wiped cluster). Idempotent across nodes.
      (reset! tiering-stats {})
      (reset! replica-floors {})
      (reset! committed-offsets-snapshot {})
      (reset! sc/authoritative-reads {})
      (sc/reset-run-state!)
      (install! test node)
      (start! test node))

    (teardown! [_ test node]
      (stop! test node)
      (c/su (c/exec :rm :-rf base-dir)))

    db/Primary
    (primaries [_ test] [(first (:nodes test))])
    (setup-primary! [_ test node]
      (info node "waiting for the stream subsystem to be ready")
      (await-stream-ready node))

    db/LogFiles
    (log-files [_ test node]
      ;; Scrape this node's S3 counters into the run totals (brokers are still
      ;; up here, before teardown) so the coverage checker can read them.
      (util/meh (record-tiering-stats! node))
      ;; Snapshot this node's manifest-replica cache (floor, epoch, context and
      ;; leadership per jepsen stream) for the replica-consistency checker, while
      ;; the brokers are still up.
      (util/meh (record-replica-cache! node))
      ;; Authoritatively read every jepsen stream end-to-end for the durability
      ;; checker, once (on the primary), while the brokers are still up. The read
      ;; waits for each consumer to reach the stream's committed offset, so query
      ;; those (over SSH) and hand them to the read (which uses the Stream client
      ;; over a fresh Environment, no SSH context). The same committed offsets are
      ;; the replica-consistency checker's stale-floor oracle, so snapshot them.
      (when (= node (first (:nodes test)))
        (util/meh
          (let [co (committed-offsets node)]
            (reset! committed-offsets-snapshot co)
            (sc/capture-authoritative-reads! test co))))
      ;; Broker logs + the S3 plugin's status output for post-mortem.
      (util/meh
        (rabbitmqctl node :stream_s3_status
                     :> (str log-dir "/stream_s3_status.txt")))
      ;; NB: cu/ls-full is broken in jepsen 0.3.11 (it calls assoc with two
      ;; args), so use cu/ls with :full-path? directly.
      (try (cu/ls log-dir {:full-path? true})
           (catch Throwable _ [])))))
