(ns jepsen.streams3.workload
  "Reuses the jepsen.tests.kafka ordered-log workload (generator + anomaly
  checkers: lost writes, duplicates, offset monotonicity, consumer/producer
  offset jumps, G0/G1c) and plugs in our Stream-protocol client.

  We run NON-transactional send/poll only (streams have no cross-stream
  transactions) with a single subscription mechanism, and rely on the stock
  'no acknowledged write is lost' assertion being sound because retention is
  configured wide (db.clj). A bounded-durability variant that only flags loss
  inside the durable range [f,n) is still to come, alongside aggressive
  retention faults."
  (:require [jepsen.tests.kafka :as kafka]
            [jepsen.streams3.client :as sc]))

(defn workload
  "Returns a Jepsen workload map {:client :generator :final-generator :checker}
  for the kafka log model, backed by our Stream client."
  [opts]
  (assoc (kafka/workload (merge {:sub-via #{:assign}
                                 :txn?   false
                                 :crash-clients? false}
                                opts))
         :client (sc/client)))
