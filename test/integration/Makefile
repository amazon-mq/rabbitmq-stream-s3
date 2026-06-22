.DEFAULT_GOAL := package
.PHONY: clean compile package run

-include ~/env.mk

SRC_DIR := $(CURDIR)/src/main/java/com/amazon/mq/rabbitmq/stream/s3
SRCS := $(wildcard $(SRC_DIR)/*.java)
JAR := $(CURDIR)/target/stream-s3-integration-test-1.0.0.jar

STREAM_URIS ?= $(if $(RMQ_STREAM_URIS),$(RMQ_STREAM_URIS),rabbitmq-stream://localhost:5552)
MGMT_URI ?= $(if $(RMQ_MGMT_URI0),$(RMQ_MGMT_URI0),http://localhost:15672)
METRICS_URIS ?= $(RMQ_METRICS_URI0),$(RMQ_METRICS_URI1),$(RMQ_METRICS_URI2)
S3_BUCKET ?= $(RMQ_S3_BUCKET)
S3_REGION ?= $(if $(RMQ_REGION),$(RMQ_REGION),us-west-2)

JAVA_OPTS ?= --sun-misc-unsafe-memory-access=allow --enable-final-field-mutation=ALL-UNNAMED
JAVA := java $(JAVA_OPTS)

clean:
	$(CURDIR)/mvnw clean

compile:
	$(CURDIR)/mvnw compile -B -q

$(JAR): $(SRCS) pom.xml
	$(CURDIR)/mvnw package -B -q -DskipTests=true

package: $(JAR)

run: package
	$(JAVA) -jar $(JAR) $(ARGS)

# --- Integration tests ---
#
# Every publishing test shares the same publish-phase flags, provided by the
# PublishOptions mixin: --publish-duration, --publish-messages, --publish-bytes
# (publishing stops on whichever is reached first; 0 disables a limit, the
# duration aside), --message-size, and --max-length-bytes. Each target passes
# explicit per-test values below so behavior is independent of the mixin's
# unified code defaults.

STREAM ?= s3-high-throughput
PRODUCERS ?= 4
CONSUMERS ?= 2
REPLAY_TIMEOUT ?= 3600

.PHONY: cleanup content-verification high-throughput multi-offset-consumer stream-deletion

cleanup: package
	$(JAVA) -jar $(JAR) cleanup \
		--mgmt-uri $(MGMT_URI) \
		$(if $(S3_BUCKET),--s3-bucket $(S3_BUCKET) --s3-region $(S3_REGION))

CONTENT_VERIFY_PUBLISH_DURATION ?= 300
# Amount-based publish limits (0 disables). Set a message or byte limit below
# max-length-bytes to publish a bounded amount with no retention, so the replay
# reads the whole stream back.
CONTENT_VERIFY_PUBLISH_MESSAGES ?= 0
CONTENT_VERIFY_PUBLISH_BYTES ?= 0
CONTENT_VERIFY_MESSAGE_SIZE ?= 256
CONTENT_VERIFY_PRODUCERS ?= 1
# This must exceed the peak un-uploaded backlog: when the publisher outruns the
# S3 upload, local retention trims un-uploaded segments once the backlog crosses
# this bound, which discards confirmed-but-not-yet-uploaded data and makes the
# replay phase report spurious gaps. See issue #233.
CONTENT_VERIFY_MAX_LENGTH_BYTES ?= 8000000000
CONTENT_VERIFY_REPLAY_CONSUMERS ?= 4

content-verification: package
	$(JAVA) -jar $(JAR) content-verification \
		--uris $(STREAM_URIS) \
		--mgmt-uri $(MGMT_URI) \
		--metrics-uris $(METRICS_URIS) \
		--stream s3-content-verify \
		--publish-duration $(CONTENT_VERIFY_PUBLISH_DURATION) \
		--publish-messages $(CONTENT_VERIFY_PUBLISH_MESSAGES) \
		--publish-bytes $(CONTENT_VERIFY_PUBLISH_BYTES) \
		--message-size $(CONTENT_VERIFY_MESSAGE_SIZE) \
		--producers $(CONTENT_VERIFY_PRODUCERS) \
		--max-length-bytes $(CONTENT_VERIFY_MAX_LENGTH_BYTES) \
		--replay-consumers $(CONTENT_VERIFY_REPLAY_CONSUMERS) \
		$(if $(S3_BUCKET),--s3-bucket $(S3_BUCKET) --s3-region $(S3_REGION))

HIGH_THROUGHPUT_PUBLISH_DURATION ?= 2700
HIGH_THROUGHPUT_PUBLISH_MESSAGES ?= 0
HIGH_THROUGHPUT_PUBLISH_BYTES ?= 0
HIGH_THROUGHPUT_MESSAGE_SIZE ?= 1024
HIGH_THROUGHPUT_MAX_LENGTH_BYTES ?= 10000000000

high-throughput: package
	$(JAVA) -jar $(JAR) high-throughput \
		--uris $(STREAM_URIS) \
		--mgmt-uri $(MGMT_URI) \
		--metrics-uris $(METRICS_URIS) \
		--stream $(STREAM) \
		--publish-duration $(HIGH_THROUGHPUT_PUBLISH_DURATION) \
		--publish-messages $(HIGH_THROUGHPUT_PUBLISH_MESSAGES) \
		--publish-bytes $(HIGH_THROUGHPUT_PUBLISH_BYTES) \
		--message-size $(HIGH_THROUGHPUT_MESSAGE_SIZE) \
		--producers $(PRODUCERS) \
		--consumers $(CONSUMERS) \
		--max-length-bytes $(HIGH_THROUGHPUT_MAX_LENGTH_BYTES) \
		--replay-timeout $(REPLAY_TIMEOUT) \
		$(if $(S3_BUCKET),--s3-bucket $(S3_BUCKET) --s3-region $(S3_REGION))

MULTI_OFFSET_PUBLISH_DURATION ?= 600
MULTI_OFFSET_PUBLISH_MESSAGES ?= 0
MULTI_OFFSET_PUBLISH_BYTES ?= 0
MULTI_OFFSET_MESSAGE_SIZE ?= 1024
MULTI_OFFSET_MAX_LENGTH_BYTES ?= 500000000
CONSUMER_RATE ?= 5000
WARMUP ?= 120

multi-offset-consumer: package
	$(JAVA) -jar $(JAR) multi-offset-consumer \
		--uris $(STREAM_URIS) \
		--mgmt-uri $(MGMT_URI) \
		--metrics-uris $(METRICS_URIS) \
		--stream s3-multi-offset \
		--publish-duration $(MULTI_OFFSET_PUBLISH_DURATION) \
		--publish-messages $(MULTI_OFFSET_PUBLISH_MESSAGES) \
		--publish-bytes $(MULTI_OFFSET_PUBLISH_BYTES) \
		--message-size $(MULTI_OFFSET_MESSAGE_SIZE) \
		--consumer-rate $(CONSUMER_RATE) \
		--warmup $(WARMUP) \
		--max-length-bytes $(MULTI_OFFSET_MAX_LENGTH_BYTES) \
		$(if $(S3_BUCKET),--s3-bucket $(S3_BUCKET) --s3-region $(S3_REGION))

STREAM_DELETION_PUBLISH_DURATION ?= 120
STREAM_DELETION_PUBLISH_MESSAGES ?= 0
STREAM_DELETION_PUBLISH_BYTES ?= 0
STREAM_DELETION_MESSAGE_SIZE ?= 1024
STREAM_DELETION_MAX_LENGTH_BYTES ?= 500000000
DELETE_TIMEOUT ?= 60

stream-deletion: package
	$(JAVA) -jar $(JAR) stream-deletion \
		--uris $(STREAM_URIS) \
		--mgmt-uri $(MGMT_URI) \
		--stream s3-stream-deletion \
		--publish-duration $(STREAM_DELETION_PUBLISH_DURATION) \
		--publish-messages $(STREAM_DELETION_PUBLISH_MESSAGES) \
		--publish-bytes $(STREAM_DELETION_PUBLISH_BYTES) \
		--message-size $(STREAM_DELETION_MESSAGE_SIZE) \
		--delete-timeout $(DELETE_TIMEOUT) \
		--max-length-bytes $(STREAM_DELETION_MAX_LENGTH_BYTES) \
		--s3-bucket $(S3_BUCKET) --s3-region $(S3_REGION)

# --- Dependency management ---

.PHONY: update-deps

update-deps:
	$(CURDIR)/mvnw versions:update-properties -B -q

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
