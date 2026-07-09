(ns jepsen.streams3.core
  "Entry point and test assembly for the RabbitMQ stream_s3 Jepsen test.

  Usage:
    lein run test --nodes-file nodes --time-limit 300 --rate 200 --concurrency 20

  See jepsen/README.md for the full docker-based run procedure."
  (:require [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [jepsen.streams3.checker :as s3-checker]
            [jepsen.streams3.db :as db]
            [jepsen.streams3.nemesis :as nem]
            [jepsen.streams3.workload :as workload]))

(def cli-opts
  "Extra CLI options for this test."
  [[nil "--rate HZ" "Approximate request rate, per second"
    :default 200 :parse-fn read-string]
   [nil "--key-count N" "Number of streams (topic-partitions)"
    :default 8 :parse-fn parse-long]
   [nil "--nemesis-interval SEC" "Seconds between fault transitions"
    :default 15 :parse-fn parse-long]
   [nil "--final-time-limit SEC"
    "Cap on the final-reads phase (the kafka final-polls loop is unbounded)"
    :default 180 :parse-fn parse-long]
   [nil "--final-settle-sec SEC"
    "Quiet settle period after healing faults, before the final reads"
    :default 15 :parse-fn parse-long]
   [nil "--auth-read-timeout-sec SEC"
    "Cap on the durability checker's end-to-end read (per committed-offset target)"
    :default 180 :parse-fn parse-long]
   [nil "--faults FAULTS"
    "Comma-separated: partition, s3-outage, s3-latency, leader-move, member-churn"
    :default "partition"]
   [nil "--final-read-tier TIER"
    (str "Which tier the final verification reads exercise: 'local' (leave data "
         "where it lies) or 's3' (force one local-retention trim after healing so "
         "the reads drain from the remote tier). A coverage knob, not a fault.")
    :default "local"
    :validate [#{"local" "s3"} "Must be one of: local, s3"]]])

(defn streams3-test
  [opts]
  (nem/validate-faults! opts)
  (let [wl (workload/workload opts)]
    (merge
      tests/noop-test
      opts
      ;; The kafka workload's generator/checker/client read these options from
      ;; the *test map*. Merge only the scalar options — merging the whole
      ;; workload map would drag in un-serializable generator Delays that break
      ;; jepsen's fressian store.
      (select-keys wl [:sub-via :txn? :crash-clients? :crash-client-interval])
      {:name      "rabbitmq-stream-s3"
       :os        debian/os
       :db        (db/db)
       :client    (:client wl)
       :nemesis   (nem/full-nemesis)
       :checker   (checker/compose
                    {:perf     (checker/perf)
                     :timeline (timeline/html)
                     ;; The kafka workload checker is authoritative on its own,
                     ;; except when a fault restarts the stream's writer: producer
                     ;; recovery then drifts our send offsets (see client.clj), so
                     ;; its offset-consistency analyzers go red on artifacts. Both
                     ;; leader-move and member-churn do this — the latter because
                     ;; the coordinator's delete_replica stops and restarts every
                     ;; member, not just the removed one. We downgrade the checker
                     ;; to advisory in those cases and let the durability checker
                     ;; carry the safety verdict.
                     :workload (s3-checker/downgrade-when
                                 (fn [t] (let [fs (nem/faults t)]
                                           (or (contains? fs "leader-move")
                                               (contains? fs "member-churn"))))
                                 (:checker wl))
                     ;; Authoritative no-loss / no-duplicate via an end-to-end
                     ;; read, sound even when send offsets are unreliable.
                     :durability (s3-checker/durability-checker)
                     ;; Fails the run unless S3 was actually exercised, so a
                     ;; green result can't silently stop using the remote tier.
                     :tiering  (s3-checker/tiering-checker)
                     ;; Asserts the per-node manifest-replica caches converge and
                     ;; serve no stale floor or leaked (contextless) row.
                     :replica  (s3-checker/replica-consistency-checker)})
       :generator (gen/phases
                    ;; Main phase: workload ops + faults for the time limit.
                    (->> (:generator wl)
                         (gen/stagger (/ 1 (:rate opts)))
                         (gen/nemesis (nem/nemesis-generator opts))
                         (gen/time-limit (:time-limit opts)))
                    ;; Heal every fault (the main phase may end mid-fault, and
                    ;; the nemesis teardown runs only after analysis), then let
                    ;; things settle before the final reads. The stop ops are
                    ;; idempotent, so healing an inactive fault is harmless.
                    (gen/nemesis [{:type :info :f :stop-partition}
                                  {:type :info :f :stop-s3-outage}
                                  {:type :info :f :stop-s3-latency}])
                    (gen/sleep (:final-settle-sec opts))
                    ;; With --final-read-tier s3, force one trim after healing so
                    ;; the final reads drain from the now-trimmed (S3-only) tier
                    ;; and actually exercise the read-from-S3 path. The plugin
                    ;; trims eagerly on its own; this only makes the tier the
                    ;; final reads hit deterministic rather than timing-dependent.
                    (gen/nemesis (when (= "s3" (:final-read-tier opts))
                                   {:type :info :f :trim-local}))
                    ;; Final reads: the kafka workload drains everything written.
                    ;; The kafka final-polls generator is unbounded by design —
                    ;; it loops until every acknowledged offset is read back — so
                    ;; bound it. If the data is readable this drains in seconds;
                    ;; if a regression makes acknowledged data permanently
                    ;; unreadable, this fails fast (the checker reports the
                    ;; unseen writes) instead of hanging indefinitely.
                    (gen/time-limit
                      (:final-time-limit opts)
                      (gen/clients (:final-generator wl))))})))

(defn -main [& args]
  (cli/run!
    (merge (cli/single-test-cmd {:test-fn streams3-test
                                 :opt-spec cli-opts})
           (cli/serve-cmd))
    args))
