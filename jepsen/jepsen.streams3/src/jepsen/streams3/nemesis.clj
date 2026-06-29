(ns jepsen.streams3.nemesis
  "Fault injection. Phase 1 ships the standard consensus-level faults
  (partitions, broker kills). The storage-tier faults that are the whole point
  of testing this plugin — S3 outage/latency via Toxiproxy, forced retention
  trimming so reads must hit S3 / cross the seam, and forced leader moves to
  exercise writer fencing — are stubbed here and land in phase 2."
  (:require [clojure.string :as str]
            [jepsen [nemesis :as nemesis]
                    [generator :as gen]]))

(defn faults
  "The set of enabled faults, parsed from the --faults option."
  [opts]
  (set (remove str/blank? (str/split (or (:faults opts) "") #","))))

;; ---------------------------------------------------------------------------
;; Phase 1: partitions + kills
;; ---------------------------------------------------------------------------

(defn full-nemesis
  "Composes the phase-1 nemeses. Each is addressed by its own :f so the
  generator can target them independently. Phase 1 is partitions only; broker
  kills and the storage-tier faults are added in phase 2 (see below)."
  []
  (nemesis/compose
    {{:start-partition :start
      :stop-partition  :stop} (nemesis/partition-random-halves)}))

(defn nemesis-generator
  "Alternates partitioning and healing on a fixed interval. Leads with a quiet
  period (a sleep before the first fault) so the workload can create streams
  and establish a baseline before anything is partitioned — otherwise the run
  starts under a partition and never gets a clean footing. Phase 1 is
  partitions only."
  [opts]
  (let [interval (or (:nemesis-interval opts) 15)]
    (if (contains? (faults opts) "partition")
      (cycle [(gen/sleep interval)
              {:type :info :f :start-partition}
              (gen/sleep interval)
              {:type :info :f :stop-partition}])
      ;; No faults selected: an empty generator that exhausts immediately, so
      ;; the nemesis never holds up a phase barrier (a long sleep here would).
      [])))

;; ---------------------------------------------------------------------------
;; Phase 2 (stubs): storage-tier faults
;; ---------------------------------------------------------------------------
;;
;; s3-outage     — Toxiproxy: cut/timeout the broker->MinIO link mid-upload.
;;                 Exercises "write availability never blocks on S3" + replica
;;                 manifest staleness/resync.
;; s3-latency    — Toxiproxy latency toxic; uploads fall behind, manifests age.
;; force-trim    — drop a stream's local retention so f_local crosses the seam
;;                 n: drives reads to S3 (Tier overlap / Exactly-once) and, when
;;                 pushed past un-uploaded data, Reset safety + bounded loss.
;; leader-move   — relocate a stream leader mid-upload: deposed-writer uploads
;;                 must become GC orphans, never overwrite (key disjointness +
;;                 epoch monotonicity / writer fencing).
;;
;; These compose with the phase-1 faults; the bounded-durability checker
;; (phase 3) is required before running force-trim aggressively.
