(ns jepsen.streams3.client
  "A jepsen client that drives RabbitMQ streams through the official Stream
  protocol Java client, mapped onto the jepsen.tests.kafka op contract
  (`assign` model, :sub-via #{:assign} — like the reference Kafka/redpanda
  test, consumers read by position and there is no server-side offset commit).

  Send offsets. The kafka workload needs the *offset* of each successful send
  (op->max-send-offsets ignores nil offsets, so the final-reads phase would not
  learn a key's true tail and its un-polled writes would all look :unseen). A
  stream publish confirm returns only a publishingId, not the committed offset
  (ConfirmationStatus has no offset; the offset is knowable only by consuming
  the message back). So we publish each stream through ONE shared producer, set
  the publishingId from a per-key counter, and serialize the send call under a
  per-key lock. With a single writer to a stream that starts empty and sends
  issued in counter order, the broker-assigned offset equals the publishingId,
  which is the offset we return. (PublishingId dedup also makes retries
  idempotent.)

  This equality holds only ABSENT a leader move: a leader move triggers producer
  recovery, after which the publishingId drifts from the true offset (a failed
  send still advances our counter), so under the leader-move nemesis our send
  offsets are wrong. That breaks the kafka checker's offset-consistency
  analyzers (a value appears at both its true offset and its drifted publishingId
  => false duplicate / inconsistent-offset) but NOT its loss analyzers (the value
  is still polled, just at two offsets). Under leader-move we therefore treat the
  kafka offset analyzers as non-authoritative and rely on the durability checker
  (checker.clj), which reads every stream end-to-end with a fresh Environment,
  using only true consumer offsets, and asserts no acknowledged write is lost or
  duplicated. See jepsen/README.md.

  Consumers. Persistent per key for a client's life, reading from the
  beginning into a per-key buffer; never rebuilt on re-assign (rebuilding
  re-read buffered offsets => duplicate deliveries). :assign waits for newly
  created consumers to catch up to the tail so the next poll returns their
  data. On a fault crash jepsen opens a fresh client whose consumers re-read
  from the beginning — a new process, so per-process monotonicity holds.

  Shared, long-lived objects (the Environment, producers, per-key counters)
  live in defonce atoms for the whole run: the Environment is a heavyweight
  (~cores-sized) thread pool meant to be shared, and one-per-client-per-reopen
  exhausts native threads.

  Crash rebuilds the Environment. The kafka final-reads phase periodically
  crashes the client to 'force a fresh one in case there's broken state inside
  the client'. Sharing one Environment for the whole run would subvert that: a
  topology-churning fault (the leader-move nemesis moves every stream's leader
  repeatedly) can wedge the Environment's consumer connections so that even
  freshly built consumers deliver nothing, and the final reads hang forever. So
  a :crash discards the shared Environment (closing it to reclaim threads) and
  the next open! builds a fresh one. A generation guard makes this idempotent
  across the concurrent crashes the final phase issues, and :crash is only ever
  emitted in the final phase, so live producers are never pulled out from under
  a send. This cannot mask a real bug: genuinely unreadable data would defeat a
  fresh Environment too, and the run would still hang (now bounded by the final
  phase's time limit) and fail."
  (:require [jepsen.client :as client])
  (:import (com.rabbitmq.stream Environment OffsetSpecification ByteCapacity)
           (java.util.concurrent ConcurrentLinkedQueue CountDownLatch TimeUnit)
           (java.util.concurrent.atomic AtomicBoolean AtomicLong)))

(def stream-port 5552)

;; Message bodies are padded so the tiny integer values produce enough volume
;; to fill stream segments and S3 fragments — otherwise nothing tiers to S3 and
;; the S3 nemeses have no traffic to disrupt. The body is "<value>|<padding>";
;; the consumer parses the value before the separator.
(def ^:private msg-padding (apply str (repeat 1000 \x)))
(defn- encode-value [v] (.getBytes (str v "|" msg-padding)))
(defn- decode-value [^bytes body]
  (let [s (String. body)] (Long/parseLong (subs s 0 (.indexOf s "|")))))

;; Small stream segment size: segments roll over and, once tiered, get trimmed
;; locally, forcing reads of older offsets to fall back to S3.
(def ^:private segment-size (ByteCapacity/kB 16))

;; How long to retry a transient stream operation (create / producer / consumer
;; build) before giving up and letting the op fail as indeterminate. Kept short
;; so that ops fail reasonably fast during a partition rather than masking it.
(def op-retry-ms 10000)

;; The Stream consumer is push-based and delivers asynchronously; mimic Kafka's
;; blocking poll(timeout) so a poll right after an :assign doesn't return empty.
(def poll-wait-ms 500)

;; :assign waits for freshly created consumers to catch up to their streams'
;; tails before returning, so the following poll returns their data.
(def catchup-max-ms 3000)   ; cap on waiting for a new consumer to reach the tail
(def catchup-quiet-ms 300)  ; buffers steady this long => caught up to the tail
(def catchup-grace-ms 1000) ; if buffers stay empty (empty stream), give up after this

(defn stream-name [k] (str "jepsen-" k))

;; ---------------------------------------------------------------------------
;; Shared, run-scoped state (never closed; the lein process exits at run end)
;; ---------------------------------------------------------------------------

(defonce ^:private shared-env (atom nil))
(defonce ^:private declared-streams (atom #{}))    ; keys whose stream exists
(defonce ^:private shared-producers (atom {}))     ; k -> Producer
(defonce ^:private pub-counters (atom {}))         ; k -> AtomicLong (next publishingId)
(defonce ^:private env-generation (atom 0))        ; bumped each Environment rebuild

(defn with-retry
  "Calls thunk, retrying on any exception until op-retry-ms elapses, then
  rethrows. For transient 'leader/stream not available' errors right after
  create or during a partition."
  [thunk]
  (let [deadline (+ (System/currentTimeMillis) op-retry-ms)]
    (loop []
      (let [r (try {:val (thunk)}
                   (catch Exception e
                     (if (< (System/currentTimeMillis) deadline) :retry (throw e))))]
        (if (= r :retry) (do (Thread/sleep 250) (recur)) (:val r))))))

(defn get-env ^Environment [test]
  (or @shared-env
      (locking shared-env
        (or @shared-env
            (let [uris (java.util.ArrayList.
                         (map #(str "rabbitmq-stream://guest:guest@" (name %) ":" stream-port)
                              (:nodes test)))
                  e (-> (Environment/builder) (.uris uris) (.build))]
              (reset! shared-env e)
              e)))))

(defn reset-env!
  "Discards the shared Environment (closing it to reclaim its thread pool) so
  the next get-env builds a fresh one with clean connections, along with the
  producers and counters bound to it. Idempotent across the concurrent crashes
  the final-reads phase issues: only the caller whose captured generation `g`
  still matches tears down; callers racing on an already-rebuilt Environment are
  no-ops. So a wave of crashes triggers exactly one rebuild."
  [g]
  (locking shared-env
    (when (= g @env-generation)
      (when-let [e @shared-env] (try (.close e) (catch Exception _)))
      (reset! shared-env nil)
      (reset! shared-producers {})
      (reset! pub-counters {})
      (swap! env-generation inc))))

(defn ensure-stream! [env k]
  (when-not (contains? @declared-streams k)
    ;; create() is idempotent for a matching stream.
    (with-retry #(-> env (.streamCreator) (.stream (stream-name k))
                     (.maxSegmentSizeBytes segment-size)
                     (.create)))
    (swap! declared-streams conj k)))

(defn get-producer [env k]
  (or (@shared-producers k)
      (locking shared-producers
        (or (@shared-producers k)
            (do (ensure-stream! env k)
                (let [p (with-retry
                          #(-> env (.producerBuilder) (.stream (stream-name k)) (.build)))]
                  (swap! shared-producers assoc k p)
                  p))))))

(defn ^AtomicLong get-counter [k]
  (or (@pub-counters k)
      (locking pub-counters
        (or (@pub-counters k)
            (let [c (AtomicLong. 0)]
              (swap! pub-counters assoc k c)
              c)))))

(defn publish-at-offset!
  "Publishes v to stream k through its shared producer at the next publishingId
  (== the stream offset absent a leader move). Serializes counter allocation and
  the send call per key so offsets stay in order; awaits the confirm. Returns the
  offset. Under the leader-move nemesis the publishingId drifts from the true
  offset, so the durability checker, not these send offsets, is authoritative for
  loss (see the ns docstring)."
  [env k v]
  (let [producer (get-producer env k)
        counter  (get-counter k)
        latch    (CountDownLatch. 1)
        ok       (AtomicBoolean. false)
        n        (locking counter
                   (let [n   (.getAndIncrement counter)
                         msg (-> producer (.messageBuilder)
                                 (.publishingId n)
                                 (.addData (encode-value v))
                                 (.build))]
                     (.send producer msg
                            (reify com.rabbitmq.stream.ConfirmationHandler
                              (handle [_ status]
                                (.set ok (.isConfirmed status))
                                (.countDown latch))))
                     n))]
    (.await latch 30 TimeUnit/SECONDS)
    (when-not (.get ok)
      (throw (ex-info "send not confirmed" {:k k :v v})))
    n))

;; ---------------------------------------------------------------------------
;; Per-client consumers
;; ---------------------------------------------------------------------------

(defn ensure-consumer!
  "Creates the persistent consumer for key k (once per client). It reads from
  the start of the stream and buffers [offset value] pairs into a per-key
  queue."
  [env consumers buffers k]
  (or (get @consumers k)
      (do (ensure-stream! env k)
          (let [q (ConcurrentLinkedQueue.)
                c (with-retry
                    #(-> env
                         (.consumerBuilder)
                         (.stream (stream-name k))
                         (.offset (OffsetSpecification/first))
                         (.messageHandler
                           (reify com.rabbitmq.stream.MessageHandler
                             (handle [_ ctx msg]
                               (.add q [(.offset ctx)
                                        (decode-value (.getBodyAsBinary msg))]))))
                         (.build)))]
            (swap! buffers assoc k q)
            (swap! consumers assoc k c)
            c))))

(defn await-caught-up!
  "Waits until the given consumer buffers have caught up to their streams'
  tails: the total buffered count must grow and then hold steady for a quiet
  period. Empty streams are handled by a grace timeout; bounded by
  catchup-max-ms."
  [queues]
  (when (seq queues)
    (let [total (fn [] (reduce (fn [a ^java.util.Collection q] (+ a (.size q))) 0 queues))
          start (System/currentTimeMillis)
          deadline (+ start catchup-max-ms)]
      (loop [last-total (total), last-change start]
        (let [now (System/currentTimeMillis)
              t   (total)]
          (cond
            (>= now deadline) nil
            (not= t last-total) (do (Thread/sleep 50) (recur t now))
            (and (> (- now last-change) catchup-quiet-ms)
                 (or (pos? t) (> (- now start) catchup-grace-ms))) nil
            :else (do (Thread/sleep 50) (recur t last-change))))))))

(defn drain!
  "Drains all currently-buffered [offset value] pairs from queue q in order."
  [q]
  (loop [acc []]
    (if-let [item (and q (.poll q))]
      (recur (conj acc item))
      acc)))

;; ---------------------------------------------------------------------------
;; Authoritative end-to-end read (for the durability checker)
;; ---------------------------------------------------------------------------

;; Reading the whole history of ~90 streams, some served from S3, needs a far
;; more generous quiescence wait than the steady-state catch-up above.
(def auth-quiet-ms 3000)    ; deliveries idle this long => every stream at its tail
(def auth-grace-ms 8000)    ; tolerate this long before any delivery (S3 warm-up)
(def auth-cap-ms 180000)    ; hard cap so a genuinely stuck stream cannot hang

;; The authoritative read of every jepsen stream, captured once after the run
;; while the brokers are still up (db/log-files), for the durability checker:
;; key -> vector of [offset value] in delivery order. Uses true consumer offsets
;; only, so it is sound even when the leader-move nemesis has made our send
;; offsets unreliable.
(defonce authoritative-reads (atom {}))

(defn- await-quiescent!
  "Waits until the total buffered count across queues holds steady for
  auth-quiet-ms (every stream reached its tail), bounded by auth-cap-ms, with an
  auth-grace-ms grace before the first delivery for streams served from S3."
  [queues]
  (let [total (fn [] (reduce (fn [a ^java.util.Collection q] (+ a (.size q))) 0 queues))
        start (System/currentTimeMillis)
        deadline (+ start auth-cap-ms)]
    (loop [last-total (total), last-change start]
      (let [now (System/currentTimeMillis)
            t   (total)]
        (cond
          (>= now deadline) nil
          (not= t last-total) (do (Thread/sleep 100) (recur t now))
          (and (> (- now last-change) auth-quiet-ms)
               (or (pos? t) (> (- now start) auth-grace-ms))) nil
          :else (do (Thread/sleep 100) (recur t last-change)))))))

(defn read-streams-fully
  "Authoritative end-to-end read of stream keys `ks` for the durability checker.
  Builds a FRESH Environment (the run's shared one may be wedged from topology
  churn), subscribes each stream from the beginning, waits for deliveries to go
  quiet (every stream at its tail), and returns {key -> vector of [offset value]
  in delivery order}. Closes the Environment before returning."
  [test ks]
  (let [uris (java.util.ArrayList.
               (map #(str "rabbitmq-stream://guest:guest@" (name %) ":" stream-port)
                    (:nodes test)))
        env    (-> (Environment/builder) (.uris uris) (.build))
        queues (reduce (fn [m k] (assoc m k (ConcurrentLinkedQueue.))) {} ks)]
    (try
      (doseq [[k q] queues]
        (-> env (.consumerBuilder) (.stream (stream-name k))
            (.offset (OffsetSpecification/first))
            (.messageHandler
              (reify com.rabbitmq.stream.MessageHandler
                (handle [_ ctx msg]
                  (.add q [(.offset ctx) (decode-value (.getBodyAsBinary msg))]))))
            (.build)))
      (await-quiescent! (vals queues))
      (into {} (map (fn [[k q]] [k (drain! q)]) queues))
      (finally (.close env)))))

(defn capture-authoritative-reads!
  "Reads keys `ks` end-to-end and stores the result in authoritative-reads."
  [test ks]
  (reset! authoritative-reads (read-streams-fully test ks)))

(defrecord Client [node ^Environment env env-gen consumers buffers assigned]
  client/Client
  (open! [this test node]
    (assoc this
           :node node
           :env (get-env test)
           ;; The Environment generation this client is bound to, so a crash
           ;; rebuilds only the Environment it actually observed (see reset-env!).
           :env-gen @env-generation
           :consumers (atom {})           ; k -> Consumer (persistent, per client)
           :buffers   (atom {})           ; k -> ConcurrentLinkedQueue of [off v]
           :assigned  (atom #{})))        ; currently-assigned keys

  (setup! [_ test])

  (invoke! [this test op]
    (case (:f op)
      (:assign :subscribe)
      (let [ks (:value op)
            new-ks (remove #(contains? @consumers %) ks)]
        (doseq [k ks] (ensure-consumer! env consumers buffers k))
        (reset! assigned (set ks))
        ;; Wait for the freshly created consumers to reach the tail so the next
        ;; poll returns their data (otherwise the key looks empty and is never
        ;; polled => :unseen).
        (await-caught-up! (keep #(get @buffers %) new-ks))
        (assoc op :type :ok))

      :send
      (let [results (mapv (fn [[_ k v]]
                            [:send k [(publish-at-offset! env k v) v]])
                          (:value op))]
        (assoc op :type :ok :value results))

      :poll
      (let [drain-assigned (fn []
                             (reduce (fn [m k]
                                       (let [items (drain! (get @buffers k))]
                                         (if (seq items) (assoc m k items) m)))
                                     {} @assigned))
            by-key (let [first-try (drain-assigned)]
                     (if (seq first-try)
                       first-try
                       (do (Thread/sleep poll-wait-ms) (drain-assigned))))]
        (assoc op :type :ok :value [[:poll by-key]]))

      ;; final-polls queries partition state for debugging; just acknowledge.
      :debug-topic-partitions
      (assoc op :type :ok)

      ;; :crash must throw: jepsen only closes and reopens the client when
      ;; invoke! throws, and final-polls relies on a fresh client (fresh
      ;; consumers re-reading each stream from the beginning). It also rebuilds
      ;; the shared Environment so the fresh consumers get clean connections,
      ;; not the wedged ones a topology-churning run can leave behind.
      :crash
      (do (reset-env! env-gen)
          (throw (ex-info "crash requested" {:type :crash})))))

  (teardown! [_ test])

  (close! [_ test]
    ;; Close this client's consumers; the shared env/producers persist.
    (doseq [[_ c] @consumers] (try (.close c) (catch Exception _)))))

(defn client [] (map->Client {}))
