.DEFAULT_GOAL := package
.PHONY: clean compile package run

SRC_DIR := $(CURDIR)/src/main/java/com/amazon/mq/rabbitmq/stream/s3
SRCS := $(wildcard $(SRC_DIR)/*.java)
JAR := $(CURDIR)/target/stream-s3-integration-test-1.0.0.jar

clean:
	$(CURDIR)/mvnw clean

compile:
	$(CURDIR)/mvnw compile -B -q

$(JAR): $(SRCS) pom.xml
	$(CURDIR)/mvnw package -B -q -DskipTests=true

package: $(JAR)

run: package
	java -jar $(JAR) $(ARGS)

# formatting

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
