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
            [jepsen.streams3.client :as sc]
            [jepsen.streams3.db :as db]
            [jepsen.streams3.nemesis :as nem]))

(defn tiering-checker
  "Fails the test unless S3 was genuinely used: fragments must have uploaded,
  when `trim` is enabled (which forces reads off the local tier) reads must have
  been served from S3, and when `leader-move` is enabled the stream epoch must
  have advanced past its initial 1 (proving a writer was actually fenced)."
  []
  (reify checker/Checker
    (check [_ test _history _opts]
      (let [stats        @db/tiering-stats
            uploads      (get stats "rabbitmq_stream_s3_transfers_completed" 0)
            reads        (get stats "rabbitmq_stream_s3_get_range" 0)
            max-epoch    (get stats "max_epoch" 0)
            fs           (nem/faults test)
            trim?        (contains? fs "trim")
            leader-move? (contains? fs "leader-move")
            problems (cond-> []
                       (empty? stats)
                       (conj :no-tiering-stats-collected)

                       (not (pos? uploads))
                       (conj :no-fragments-uploaded-to-s3)

                       (and trim? (not (pos? reads)))
                       (conj :no-reads-served-from-s3)

                       ;; The epoch starts at 1 and bumps on every leader move;
                       ;; if it never rose, no leader moved and no writer was
                       ;; ever fenced, so the path went unexercised.
                       (and leader-move? (not (> max-epoch 1)))
                       (conj :no-leader-moves))]
        {:valid?   (empty? problems)
         :stats    stats
         :problems problems}))))

(defn- acked-sends-by-key
  "Map of key -> set of acknowledged sent values, from the history."
  [history]
  (->> history
       (filter #(and (= :ok (:type %)) (= :send (:f %))))
       (reduce (fn [m op]
                 (reduce (fn [m [_ k [_ v]]] (update m k (fnil conj #{}) v))
                         m (:value op)))
               {})))

(defn durability-checker
  "Authoritative no-loss / no-duplicate check, independent of send offsets.

  The kafka workload derives loss from the offsets our client reports, but under
  the leader-move nemesis those send offsets drift from the true offset (see
  client.clj), which is unsound. This checker sidesteps that entirely: it
  compares every acknowledged send in the history against an end-to-end read of
  each stream captured after the run (db/log-files -> client/authoritative-reads),
  which uses only true consumer offsets. It fails if any acknowledged value is
  missing from its stream (lost) or appears more than once (duplicated) — the two
  safety properties writer fencing must preserve."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [reads @sc/authoritative-reads
            sent  (acked-sends-by-key history)
            errs  (for [[k vs] sent
                        :let [pairs   (get reads k)
                              vals    (map second pairs)
                              present (set vals)
                              lost    (sort (remove present vs))
                              dups    (->> vals frequencies
                                           (keep (fn [[v c]] (when (> c 1) v)))
                                           sort)
                              ;; Offsets each duplicated value sits at, so we can
                              ;; tell a real stream duplicate (distinct offsets)
                              ;; from a read redelivery (the same offset twice).
                              dup-offsets (into {}
                                                (for [v dups]
                                                  [v (mapv first (filter #(= (second %) v) pairs))]))]
                        :when (or (nil? pairs) (seq lost) (seq dups))]
                    (cond-> {:key k}
                      (nil? pairs) (assoc :not-read true)
                      (seq lost)   (assoc :lost lost)
                      (seq dups)   (assoc :duplicated dups :duplicate-offsets dup-offsets)))
            problems (cond-> []
                       (empty? reads) (conj :no-authoritative-reads)
                       (seq errs)     (conj :acked-writes-lost-or-duplicated))]
        {:valid?      (empty? problems)
         :problems    problems
         :keys-sent   (count sent)
         :keys-read   (count reads)
         :violations  (vec errs)}))))

(defn replica-consistency-checker
  "Asserts the per-node manifest-replica caches stay consistent under churn,
  reading the snapshots db captured after the run quiesced (db/replica-floors,
  the per-node cache; db/committed-offsets-snapshot, the committed-offset oracle).
  Black-box analogue of the manifest-replica model's convergence, stale-floor and
  NOLEAK properties. Three problems:

    :replicas-diverged   - some stream's cached floor is not identical across the
                           nodes that cache it. After the final settle and reads
                           the caches must have converged on the floor; a lingering
                           disagreement means a sync or edit was lost or applied
                           out of order. The cached epoch is reported alongside for
                           diagnosis but is NOT part of the equality: a deposed
                           leader's cache legitimately keeps the older epoch on an
                           idle stream until the next edit, and the plugin's GC
                           explicitly tolerates a cache that lags the committed
                           epoch (get_manifest_and_epoch/1), so an epoch lag is not
                           a convergence violation.
    :stale-floor-served  - the cross-node-agreed floor sits beyond the stream's
                           committed offset (floor > committed + 1). The remote
                           tier cannot start past committed data and retention is
                           wide, so a floor ahead of the committed tail is a stale
                           or corrupt cached floor. An empty stream reports a
                           committed offset of -1, so its bound is 0.
    :leaked-replica-row  - a cache row on a node that is NOT the stream's leader
                           has no replica context registered. A replica row is
                           created alongside a monitored osiris member, so a
                           contextless one is a row a sync re-created after the
                           member's DOWN released it, with no monitor to reclaim
                           it. The leader's own row is written by the writer path
                           (no replica context) and is legitimate, so the leader
                           node is excluded to avoid a false positive.

  Honesty note: convergence and the stale-floor bound are genuinely exercised by
  the s3-outage / leader-move / partition faults. The leaked-row guard only fires
  when an osiris member departs a node permanently mid-sync, which graceful
  leader-move does not cause, so it ships as a cheap always-on guard, not
  something the current faults reproduce. Reliably exercising it needs a hard-kill
  / stream-churn nemesis (see jepsen/BACKLOG.md)."
  []
  (reify checker/Checker
    (check [_ _test _history _opts]
      (let [per-node  @db/replica-floors
            committed @db/committed-offsets-snapshot
            ;; stream key -> {node -> rec} over only the nodes that cache it.
            by-stream (reduce-kv
                        (fn [acc node kmap]
                          (reduce-kv (fn [a k rec]
                                       (if (:cached? rec)
                                         (assoc-in a [k node] rec)
                                         a))
                                     acc kmap))
                        {} per-node)
            diverged  (for [[k node->rec] by-stream
                            :let [floors (set (map (fn [[_ r]] (:floor r)) node->rec))]
                            :when (> (count floors) 1)]
                        {:key k
                         :node->floor-epoch
                         (into {} (map (fn [[n r]] [n [(:floor r) (:epoch r)]]) node->rec))})
            ;; Only meaningful when the floor itself converged (a divergent floor
            ;; is already reported above); compare that agreed floor to committed.
            stale     (for [[k node->rec] by-stream
                            :let [floors (set (map (fn [[_ r]] (:floor r)) node->rec))
                                  agreed (when (= 1 (count floors)) (first floors))
                                  c      (get committed k -1)]
                            :when (and agreed (> agreed (inc c)))]
                        {:key k :agreed-floor agreed :committed-offset c})
            leaked    (for [[k node->rec] by-stream
                            [node rec] node->rec
                            :when (and (not (:leader? rec)) (not (:ctx? rec)))]
                        {:key k :node node})
            problems  (cond-> []
                        (empty? per-node) (conj :no-replica-cache-collected)
                        (seq diverged)    (conj :replicas-diverged)
                        (seq stale)       (conj :stale-floor-served)
                        (seq leaked)      (conj :leaked-replica-row))]
        {:valid?            (empty? problems)
         :problems          problems
         :streams-cached    (count by-stream)
         :diverged          (vec diverged)
         :stale-floors      (vec stale)
         :leaked-rows       (vec leaked)}))))

(defn downgrade-when
  "Wraps a checker so that when (pred test) holds its result cannot invalidate
  the run: anomalies are kept for inspection but :valid? is forced true. Used for
  the kafka workload's offset analyzers under leader-move, where our client's send
  offsets are known to be unsound; the durability checker carries the real safety
  verdict there."
  [pred inner]
  (reify checker/Checker
    (check [_ test history opts]
      (let [r (checker/check inner test history opts)]
        (if (pred test)
          (assoc r :valid? true :downgraded-valid? (:valid? r))
          r)))))
