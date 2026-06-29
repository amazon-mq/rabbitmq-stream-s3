(ns jepsen.streams3.client
  "A jepsen client that drives RabbitMQ streams through the official Stream
  protocol Java client, mapped onto the jepsen.tests.kafka op contract
  (`assign` model, :sub-via #{:assign} — like the reference Kafka/redpanda
  test, consumers read by position and there is no server-side offset commit).

  Getting a real send offset. The kafka workload needs the *offset* of each
  successful send (op->max-send-offsets ignores nil offsets, so poll-unseen
  would never re-poll a key and its writes would all look :unseen). But a
  stream publish confirm returns only a publishingId, not the offset. So we
  publish each stream through ONE shared producer, set the publishingId from a
  per-key counter, and serialize the send call under a per-key lock. With a
  single writer to a stream that starts empty and sends issued in counter
  order, the broker-assigned offset equals the publishingId — which is the
  offset we return. (PublishingId dedup also makes retries idempotent.)

  Consumers. Persistent per key for a client's life, reading from the
  beginning into a per-key buffer; never rebuilt on re-assign (rebuilding
  re-read buffered offsets => duplicate deliveries). :assign waits for newly
  created consumers to catch up to the tail so the next poll returns their
  data. On a fault crash jepsen opens a fresh client whose consumers re-read
  from the beginning — a new process, so per-process monotonicity holds.

  Shared, long-lived objects (the Environment, producers, per-key counters)
  live in defonce atoms for the whole run: the Environment is a heavyweight
  (~cores-sized) thread pool meant to be shared, and one-per-client-per-reopen
  exhausts native threads."
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
  (== the stream offset). Serializes counter allocation and the send call per
  key so offsets stay in order; awaits the confirm. Returns the offset."
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

(defrecord Client [node ^Environment env consumers buffers assigned]
  client/Client
  (open! [this test node]
    (assoc this
           :node node
           :env (get-env test)
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
      ;; consumers re-reading each stream from the beginning).
      :crash
      (throw (ex-info "crash requested" {:type :crash}))))

  (teardown! [_ test])

  (close! [_ test]
    ;; Close this client's consumers; the shared env/producers persist.
    (doseq [[_ c] @consumers] (try (.close c) (catch Exception _)))))

(defn client [] (map->Client {}))
