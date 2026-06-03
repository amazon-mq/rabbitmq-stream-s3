.DEFAULT_GOAL := package
.PHONY: clean compile package run

-include ~/env.mk

SRC_DIR := $(CURDIR)/src/main/java/com/amazon/mq/rabbitmq/stream/s3
SRCS := $(wildcard $(SRC_DIR)/*.java)
JAR := $(CURDIR)/target/stream-s3-integration-test-1.0.0.jar

STREAM_URIS ?= $(if $(RMQ_STREAM_URIS),$(RMQ_STREAM_URIS),rabbitmq-stream://localhost:5552)
MGMT_URI ?= $(if $(RMQ_MGMT_URI0),$(RMQ_MGMT_URI0),http://localhost:15672)
S3_BUCKET ?= $(RMQ_S3_BUCKET)
S3_REGION ?= $(if $(RMQ_REGION),$(RMQ_REGION),us-west-2)

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

.PHONY: cleanup s3-cleanup high-throughput remote-only-read multi-offset-consumer

cleanup: package
	java -jar $(JAR) cleanup \
		--mgmt-uri $(MGMT_URI)

s3-cleanup:
ifndef S3_BUCKET
	$(error S3_BUCKET is not set - ensure ~/env.mk defines RMQ_S3_BUCKET)
endif
	aws s3 rm s3://$(S3_BUCKET)/rabbitmq/stream/ --recursive --region $(S3_REGION)

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
