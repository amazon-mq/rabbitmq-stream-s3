(ns jepsen.streams3.checker
  "Checks beyond correctness — that the S3 code paths were actually exercised.

  The kafka workload checker validates correctness only; it cannot tell whether
  a single byte reached S3. So a regression that silently no-ops tiering (a
  rejected upload, a misbuilt endpoint, broken trimming, a soft-dependency
  timeout swallowing everything) would leave all data local, keep correctness
  intact, and still pass green. This checker closes that gap by asserting the
  plugin's S3 counters (scraped into db/tiering-stats before teardown) cleared
  the floor."
  (:require [jepsen.checker :as checker]
            [jepsen.streams3.db :as db]
            [jepsen.streams3.nemesis :as nem]))

(defn tiering-checker
  "Fails the test unless S3 was genuinely used: fragments must have uploaded,
  and when `trim` is enabled (which forces reads off the local tier) reads must
  have been served from S3."
  []
  (reify checker/Checker
    (check [_ test _history _opts]
      (let [stats    @db/tiering-stats
            uploads  (get stats "rabbitmq_stream_s3_transfers_completed" 0)
            reads    (get stats "rabbitmq_stream_s3_get_range" 0)
            trim?    (contains? (nem/faults test) "trim")
            problems (cond-> []
                       (empty? stats)
                       (conj :no-tiering-stats-collected)

                       (not (pos? uploads))
                       (conj :no-fragments-uploaded-to-s3)

                       (and trim? (not (pos? reads)))
                       (conj :no-reads-served-from-s3))]
        {:valid?   (empty? problems)
         :stats    stats
         :problems problems}))))
