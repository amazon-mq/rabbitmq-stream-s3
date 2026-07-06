PROJECT = rabbitmq_stream_s3
PROJECT_DESCRIPTION = RabbitMQ S3 plugin
PROJECT_MOD = rabbitmq_stream_s3_app

DEPS = rabbit rabbit_common osiris khepri gun rabbitmq_prometheus
BUILD_DEPS =
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers rabbitmq_stream proper
LOCAL_DEPS = crypto public_key ssl xmerl

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk

# CT suites share a node name (erlang.mk limitation), so they cannot run in parallel.
.NOTPARALLEL:

CT_QUICK_SUITES = api_fs db replica_reader_core remote_reader_core replica_reader replica_reader_statem log_reader fragment_iterator fragment_assembly prop manifest_replica manifest_replica_statem api_aws_pool_statem

ERLFMT ?= erlfmt
ERLFMT_FILES = src/*.erl test/*.erl

.PHONY: fmt fmt-check
fmt:
	$(verbose) $(ERLFMT) -w $(ERLFMT_FILES)

fmt-check:
	$(verbose) $(ERLFMT) -c $(ERLFMT_FILES)

.PHONY: ct-quick
ct-quick: test-build
	$(verbose) mkdir -p $(CT_LOGS_DIR)
	$(gen_verbose) $(CT_RUN) -sname ct_$(PROJECT) -suite $(addsuffix _SUITE,$(CT_QUICK_SUITES)) $(CT_EXTRA) $(CT_OPTS)
