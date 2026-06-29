(ns jepsen.streams3.nemesis
  "Fault injection: network partitions plus the storage-tier and writer-fencing
  faults that are the whole point of testing this plugin. Currently:

    partition   - network partition (random halves)
    s3-outage   - cut the broker->MinIO link (disable the Toxiproxy proxy):
                  exercises 'write availability never blocks on S3', uploads
                  falling behind, replica manifest staleness, and reads of
                  already-tiered data failing then recovering
    s3-latency  - a Toxiproxy latency toxic on the S3 link: uploads fall behind
                  and manifests age while writes continue
    leader-move - relocate every stream's leader to a replica mid-upload: the
                  writer (and uploader) moves and the stream epoch bumps, fencing
                  the deposed writer (its straggler uploads must be rejected on an
                  epoch conflict, never overwrite the new leader's data)

  Still stubbed (see bottom): force-trim (needs the bounded-durability
  checker)."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [nemesis :as nemesis]
                    [generator :as gen]]
            [jepsen.streams3.db :as db])
  (:import (java.net URI)
           (java.net.http HttpClient
                          HttpRequest
                          HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)))

(defn faults
  "The set of enabled faults, parsed from the --faults option."
  [opts]
  (set (remove str/blank? (str/split (or (:faults opts) "") #","))))

;; ---------------------------------------------------------------------------
;; Toxiproxy admin API (reached from the control node at toxiproxy:8474)
;; ---------------------------------------------------------------------------

(def ^:private toxiproxy-base "http://toxiproxy:8474")

;; The S3 proxy as declared in docker/toxiproxy.json; re-sent in full when
;; toggling `enabled`.
(def ^:private s3-proxy
  {:name "s3" :listen "0.0.0.0:443" :upstream "minio:443"})

(defn- json [m]
  (str "{" (str/join "," (map (fn [[k v]]
                                (str \" (name k) "\":"
                                     (cond (string? v) (str \" v \")
                                           (map? v) (json v)
                                           :else v)))
                              m)) "}"))

(defn- toxiproxy! [method path body]
  (let [b (-> (HttpRequest/newBuilder (URI/create (str toxiproxy-base path)))
              (.header "Content-Type" "application/json"))
        req (-> (case method
                  :post   (.POST b (HttpRequest$BodyPublishers/ofString (or body "")))
                  :delete (.DELETE b)
                  :get    (.GET b))
                (.build))
        resp (.send (HttpClient/newHttpClient) req (HttpResponse$BodyHandlers/ofString))]
    {:status (.statusCode resp) :body (.body resp)}))

(defn- set-s3-enabled! [enabled?]
  (toxiproxy! :post "/proxies/s3" (json (assoc s3-proxy :enabled enabled?))))

(defn- add-s3-latency! [ms jitter]
  (toxiproxy! :post "/proxies/s3/toxics"
              (json {:name "s3-latency" :type "latency" :stream "downstream"
                     :attributes {:latency ms :jitter jitter}})))

(defn- remove-s3-latency! []
  (toxiproxy! :delete "/proxies/s3/toxics/s3-latency" nil))

;; ---------------------------------------------------------------------------
;; Nemeses
;; ---------------------------------------------------------------------------

(defn s3-nemesis
  "Drives S3 faults through Toxiproxy. teardown! restores the link."
  []
  (reify nemesis/Nemesis
    (setup! [this _test] this)
    (invoke! [_ _test op]
      (assoc op :value
             (case (:f op)
               :start-s3-outage  (do (info "S3 outage: disabling proxy") (set-s3-enabled! false))
               :stop-s3-outage   (do (info "S3 outage: re-enabling proxy") (set-s3-enabled! true))
               :start-s3-latency (do (info "S3 latency: adding toxic") (add-s3-latency! 2000 1000))
               :stop-s3-latency  (do (info "S3 latency: removing toxic") (remove-s3-latency!)))))
    (teardown! [this _test]
      (try (set-s3-enabled! true) (catch Exception _))
      (try (remove-s3-latency!) (catch Exception _))
      this)))

(defn trim-nemesis
  "Not a fault: forces local-retention evaluation on every node so segments
  already uploaded to S3 are trimmed locally and reads of older offsets fall
  back to the S3 tier. Without this the workload's data stays local and the
  read-from-S3 path is never exercised."
  []
  (reify nemesis/Nemesis
    (setup! [this _test] this)
    (invoke! [_ test op]
      (c/on-nodes test (fn [_test node] (db/force-local-trim! node)))
      (assoc op :value :trimmed))
    (teardown! [this _test] this)))

(defn leader-move-nemesis
  "Not a network fault: gracefully relocates every jepsen stream's leader to a
  replica, moving the writer/uploader and bumping the stream epoch, which fences
  the deposed writer. Run on one node only (the stream coordinator is cluster-
  global), so each leadership transfer issues once rather than once per node."
  []
  (reify nemesis/Nemesis
    (setup! [this _test] this)
    (invoke! [_ test op]
      (c/on-nodes test [(first (:nodes test))]
                  (fn [_test node] (db/force-leader-move! node)))
      (assoc op :value :leaders-moved))
    (teardown! [this _test] this)))

(defn full-nemesis
  "Composes all nemeses; the generator decides which faults actually fire."
  []
  (nemesis/compose
    {{:start-partition :start
      :stop-partition  :stop}        (nemesis/partition-random-halves)
     #{:start-s3-outage :stop-s3-outage
       :start-s3-latency :stop-s3-latency} (s3-nemesis)
     #{:trim-local}                  (trim-nemesis)
     #{:move-leaders}                (leader-move-nemesis)}))

;; ---------------------------------------------------------------------------
;; Generator
;; ---------------------------------------------------------------------------

(def ^:private fault->events
  {"partition"  [:start-partition :stop-partition]
   "s3-outage"  [:start-s3-outage :stop-s3-outage]
   "s3-latency" [:start-s3-latency :stop-s3-latency]})

;; Background single-shot ops (not start/stop faults): a retention trim and a
;; leader move. Each, when selected, runs steadily throughout the test — spread
;; across the inter-fault gaps and, on its own, when no start/stop fault is
;; enabled.
(def ^:private fault->bg-op
  {"trim"        {:type :info :f :trim-local}
   "leader-move" {:type :info :f :move-leaders}})

(defn nemesis-generator
  "Rotates through the enabled start/stop faults one at a time, each led by a
  quiet baseline period (so the workload establishes itself before the first
  fault, and recovers between faults). Background single-shot ops selected via
  --faults (`trim`, `leader-move`) run steadily throughout — between fault
  transitions and, on their own, when no start/stop fault is enabled — so the
  S3 read path and the writer-fencing path stay exercised. With nothing
  selected, an empty generator — never a long sleep, which would deadlock a
  phase barrier on the nemesis."
  [opts]
  (let [fs           (faults opts)
        interval     (or (:nemesis-interval opts) 15)
        bg-ops       (keep fault->bg-op (sort fs))
        fault-events (keep fault->events (sort fs))
        ;; One inter-fault gap: fire each background op twice across the
        ;; interval, or, with no background op, just a quiet sleep.
        spacer       (if (seq bg-ops)
                       (let [gap (gen/sleep (/ interval (count bg-ops) 2))]
                         (vec (mapcat (fn [op] [gap op gap op]) bg-ops)))
                       [(gen/sleep interval)])]
    (cond
      (seq fault-events)
      (mapcat (fn [[start stop]]
                (concat spacer [{:type :info :f start}]
                        spacer [{:type :info :f stop}]))
              (cycle fault-events))

      (seq bg-ops)
      (gen/stagger (/ interval 2) (cycle bg-ops))

      :else [])))

;; ---------------------------------------------------------------------------
;; Still stubbed
;; ---------------------------------------------------------------------------
;;
;; force-trim   — drop a stream's local retention so f_local crosses the seam
;;                n: drives reads to S3 (Tier overlap / Exactly-once) and, when
;;                pushed past un-uploaded data, Reset safety + bounded loss.
;;                Needs the bounded-durability checker first.
