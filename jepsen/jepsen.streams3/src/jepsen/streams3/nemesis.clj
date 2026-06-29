(ns jepsen.streams3.nemesis
  "Fault injection: network partitions plus the storage-tier
  faults that are the whole point of testing this plugin. Currently:

    partition   - network partition (random halves)
    s3-outage   - cut the broker->MinIO link (disable the Toxiproxy proxy):
                  exercises 'write availability never blocks on S3', uploads
                  falling behind, replica manifest staleness, and reads of
                  already-tiered data failing then recovering
    s3-latency  - a Toxiproxy latency toxic on the S3 link: uploads fall behind
                  and manifests age while writes continue

  Still stubbed (see bottom): force-trim (needs the bounded-durability
  checker) and leader-move (writer fencing)."
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

(defn full-nemesis
  "Composes all nemeses; the generator decides which faults actually fire."
  []
  (nemesis/compose
    {{:start-partition :start
      :stop-partition  :stop}        (nemesis/partition-random-halves)
     #{:start-s3-outage :stop-s3-outage
       :start-s3-latency :stop-s3-latency} (s3-nemesis)
     #{:trim-local}                  (trim-nemesis)}))

;; ---------------------------------------------------------------------------
;; Generator
;; ---------------------------------------------------------------------------

(def ^:private fault->events
  {"partition"  [:start-partition :stop-partition]
   "s3-outage"  [:start-s3-outage :stop-s3-outage]
   "s3-latency" [:start-s3-latency :stop-s3-latency]})

(defn nemesis-generator
  "Rotates through the enabled faults one at a time, each led by a quiet
  baseline period (so the workload establishes itself before the first fault,
  and recovers between faults). When `trim` is selected (via --faults), a
  steady local-retention trim runs throughout — between fault transitions and,
  on its own, when no faults are enabled — so uploaded data is trimmed locally
  and reads fall to the S3 tier. With nothing selected, an empty generator —
  never a long sleep, which would deadlock a phase barrier on the nemesis."
  [opts]
  (let [fs           (faults opts)
        interval     (or (:nemesis-interval opts) 15)
        trim?        (contains? fs "trim")
        fault-events (keep fault->events (sort (disj fs "trim")))
        ;; One inter-fault gap: either a couple of trims spread across the
        ;; interval, or just a quiet sleep.
        spacer       (if trim?
                       [(gen/sleep (/ interval 2)) {:type :info :f :trim-local}
                        (gen/sleep (/ interval 2)) {:type :info :f :trim-local}]
                       [(gen/sleep interval)])]
    (cond
      (seq fault-events)
      (mapcat (fn [[start stop]]
                (concat spacer [{:type :info :f start}]
                        spacer [{:type :info :f stop}]))
              (cycle fault-events))

      trim?
      (gen/stagger (/ interval 2) (repeat {:type :info :f :trim-local}))

      :else [])))

;; ---------------------------------------------------------------------------
;; Still stubbed
;; ---------------------------------------------------------------------------
;;
;; force-trim   — drop a stream's local retention so f_local crosses the seam
;;                n: drives reads to S3 (Tier overlap / Exactly-once) and, when
;;                pushed past un-uploaded data, Reset safety + bounded loss.
;;                Needs the bounded-durability checker first.
;; leader-move  — relocate a stream leader mid-upload: deposed-writer uploads
;;                must become GC orphans, never overwrite (key disjointness +
;;                epoch monotonicity / writer fencing).
