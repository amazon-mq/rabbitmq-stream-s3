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
            [jepsen.os.debian :as debian]))

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

;; The S3 endpoint the broker talks to. region_endpoints maps the region name
;; to this "TLD"; the AWS backend then forms <bucket>.<endpoint> and connects
;; over TLS/443. docker-compose must give the Toxiproxy container the network
;; aliases `s3.jepsen.local` and `jepsen.s3.jepsen.local`.
(def s3-region   "jepsen")
(def s3-endpoint "s3.jepsen.local")
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
     (str "stream_s3.region_endpoints." s3-region " = " s3-endpoint)
     (str "stream_s3.access_key_id = " s3-access-key)
     (str "stream_s3.secret_key = " s3-secret-key)
     (str "stream_s3.bucket = " s3-bucket)
     ;; Small fragments + frequent persists so tiering and manifest replication
     ;; actually happen within a short test run.
     "stream_s3.fragment_target_size = 1MB"
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
      ;; Broker logs + the S3 plugin's status output for post-mortem.
      (util/meh
        (rabbitmqctl node :stream_s3_status
                     :> (str log-dir "/stream_s3_status.txt")))
      ;; NB: cu/ls-full is broken in jepsen 0.3.11 (it calls assoc with two
      ;; args), so use cu/ls with :full-path? directly.
      (try (cu/ls log-dir {:full-path? true})
           (catch Throwable _ [])))))
