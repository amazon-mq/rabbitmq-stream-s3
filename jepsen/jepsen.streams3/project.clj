(defproject jepsen.streams3 "0.1.0-SNAPSHOT"
  :description "Jepsen test for RabbitMQ streams with S3 tiered storage (rabbitmq_stream_s3)"
  :url "https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_stream_s3/jepsen"
  :license {:name "Apache 2.0 License"
            :url "https://www.apache.org/licenses/LICENSE-2.0.html"}
  :main jepsen.streams3.core
  :jvm-opts ["-Xmx8g"
             "-Djava.awt.headless=true"]
  :dependencies [[org.clojure/clojure "1.12.4"]
                 ;; Jepsen brings in jepsen.tests.kafka (the log/queue workload and
                 ;; its anomaly checkers) which we reuse for the ordered-log model.
                 [jepsen "0.3.11"]
                 ;; Official RabbitMQ Stream protocol client. The workload's :send/:poll
                 ;; ops are driven through this via Clojure interop (see client.clj).
                 [com.rabbitmq/stream-client "1.6.0"]
                 ;; Pin slf4j-api to 2.x: jepsen's logback 1.5 needs it, but
                 ;; transitive resolution otherwise pulls 1.7.36 and logging
                 ;; init crashes (ClassNotFoundException LoggingEventAware).
                 [org.slf4j/slf4j-api "2.0.12"]]
  :exclusions [org.slf4j/log4j-over-slf4j
               log4j/log4j]
  :repl-options {:init-ns jepsen.streams3.core})
