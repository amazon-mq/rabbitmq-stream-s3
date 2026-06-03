.DEFAULT_GOAL := package
.PHONY: clean compile package run

-include ~/env.mk

SRC_DIR := $(CURDIR)/src/main/java/com/amazon/mq/rabbitmq/stream/s3
SRCS := $(wildcard $(SRC_DIR)/*.java)
JAR := $(CURDIR)/target/stream-s3-integration-test-1.0.0.jar

STREAM_PORT ?= 5552
MGMT_PORT ?= 15672

ifdef RMQ_NODE0
STREAM_URIS := rabbitmq-stream://$(RMQ_NODE0):$(STREAM_PORT),rabbitmq-stream://$(RMQ_NODE1):$(STREAM_PORT),rabbitmq-stream://$(RMQ_NODE2):$(STREAM_PORT)
MGMT_URI := http://$(RMQ_NODE0):$(MGMT_PORT)
else
STREAM_URIS := rabbitmq-stream://localhost:$(STREAM_PORT)
MGMT_URI := http://localhost:$(MGMT_PORT)
endif

clean:
	$(CURDIR)/mvnw clean

compile:
	$(CURDIR)/mvnw compile -B -q

$(JAR): $(SRCS) pom.xml
	$(CURDIR)/mvnw package -B -q -DskipTests=true

package: $(JAR)

run: package
	java -jar $(JAR) $(ARGS)

# --- Integration tests ---

STREAM ?= s3-high-throughput
DURATION ?= 2700
PRODUCERS ?= 4
CONSUMERS ?= 2
MESSAGE_SIZE ?= 1024
MAX_LENGTH_BYTES ?= 10000000000
REPLAY_TIMEOUT ?= 3600

.PHONY: high-throughput remote-only-read multi-offset-consumer

high-throughput: package
	java -jar $(JAR) high-throughput \
		--uris $(STREAM_URIS) \
		--mgmt-uri $(MGMT_URI) \
		--stream $(STREAM) \
		--duration $(DURATION) \
		--producers $(PRODUCERS) \
		--consumers $(CONSUMERS) \
		--message-size $(MESSAGE_SIZE) \
		--max-length-bytes $(MAX_LENGTH_BYTES) \
		--replay-timeout $(REPLAY_TIMEOUT)

remote-only-read: package
	java -jar $(JAR) remote-only-read \
		--uris $(STREAM_URIS) \
		--mgmt-uri $(MGMT_URI) \
		--stream $(STREAM) \
		--expected-messages $(EXPECTED_MESSAGES)

multi-offset-consumer: package
	java -jar $(JAR) multi-offset-consumer \
		--uris $(STREAM_URIS) \
		--mgmt-uri $(MGMT_URI) \
		--stream $(STREAM) \
		--duration $(DURATION)

# --- Formatting ---

GJF_JAR := $(CURDIR)/target/google-java-format.jar

$(GJF_JAR):
	@mkdir -p $(CURDIR)/target
	gh release download --repo google/google-java-format \
		--pattern '*-all-deps.jar' \
		--output $(GJF_JAR)

.PHONY: format format-check

format: $(GJF_JAR)
	java -jar $(GJF_JAR) --replace $(SRCS)

format-check: $(GJF_JAR)
	java -jar $(GJF_JAR) --dry-run --set-exit-if-changed $(SRCS)
