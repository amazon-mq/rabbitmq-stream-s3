#!/usr/bin/env bash
# CI entry point: bring the cluster up and run one test, failing the build if
# the checker reports anomalies. Intended to be called from a GitHub Actions
# job after the broker tarball has been built/staged.
#
# Repo-topology note: `make package-generic-unix` is an *umbrella* target, so a
# CI job needs the full server checkout (the plugin is developed standalone in
# deps/rabbitmq_stream_s3 but built within the umbrella). `up.sh` builds the
# tarball from the umbrella root that contains this checkout.
#
# Tunables (all overridable via the environment, consumed by run.sh):
#   TIME_LIMIT, RATE, CONCURRENCY  - workload size
#   FAULTS                         - comma-separated nemeses to inject
#
# This composes the same up.sh / run.sh / down.sh a developer uses locally; the
# only CI-specific behaviour is tearing down unconditionally and propagating the
# test's exit status as the build result.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
docker_dir="$here/../docker"

# Defaults for run.sh; export so the child script inherits them. Override per CI
# matrix entry to cover the storage-tier and writer-fencing scenarios separately.
export TIME_LIMIT="${TIME_LIMIT:-120}"
export RATE="${RATE:-100}"
export CONCURRENCY="${CONCURRENCY:-20}"
export FAULTS="${FAULTS:-partition,s3-outage,s3-latency,trim}"

"$docker_dir/up.sh"

set +e
"$docker_dir/run.sh"
status=$?
set -e

# Always tear down; never let a teardown hiccup mask the test verdict.
"$docker_dir/down.sh" || true
exit $status
