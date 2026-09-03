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

# Benchmarks (test/*_bench.erl). Not CI-gated; for before/after comparisons
# on a quiet machine. `make bench` runs all, `make bench-<module>` runs one.
# The harness itself matches the *_bench.erl glob; it is not a benchmark.
BENCH_MODULES = $(filter-out rabbitmq_stream_s3_bench,$(patsubst test/%.erl,%,$(wildcard test/*_bench.erl)))

.PHONY: bench
bench: $(addprefix bench-,$(BENCH_MODULES))

# ERL_LIBS puts the umbrella's deps on the path. read_buffer_bench needs none of
# them, but remote_reader_s3_bench drives the real S3 client and so needs gun,
# seshat and thoas.
bench-%: test-build
	$(gen_verbose) ERL_LIBS=$(CURDIR)/.. erl -noshell -pa ebin -pa test -eval '$*:run(), halt(0).'

# The object store remote_reader_s3_bench measures against: MinIO, with latency
# shaped by `tc netem`. Same shape as jepsen/docker/, minus TLS - the harness
# injects the pool's `open_fun` and dials by address, so no certificates or
# /etc/hosts entry are needed. `remote_reader_s3_bench:run/0` skips cleanly when
# this is not up.
#
# The network name and the MinIO image are the script's own, not settings here:
# it reads S3_BENCH_MINIO from the environment, and make passes a command-line
# assignment through to the recipe, so `make s3-bench-up S3_BENCH_MINIO=...`
# reaches it.
S3_BENCH_ENGINE ?= podman

.PHONY: s3-bench-up
s3-bench-up:
	$(gen_verbose) ./scripts/s3-bench-env.sh up $(S3_BENCH_ENGINE)

# Runs the stress-tested configurations and prints each beside its measured
# result. The harness's own regression test - see scripts/s3-bench-validate.sh.
.PHONY: s3-bench-validate
s3-bench-validate:
	$(gen_verbose) ./scripts/s3-bench-validate.sh

.PHONY: s3-bench-down
s3-bench-down:
	$(gen_verbose) ./scripts/s3-bench-env.sh down $(S3_BENCH_ENGINE)
