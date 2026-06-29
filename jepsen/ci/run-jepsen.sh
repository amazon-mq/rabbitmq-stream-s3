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
# Tunables (all overridable via the environment):
#   TIME_LIMIT, RATE, CONCURRENCY  - workload size
#   FAULTS                         - comma-separated nemeses to inject
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
docker_dir="$here/../docker"

TIME_LIMIT="${TIME_LIMIT:-120}"
RATE="${RATE:-100}"
CONCURRENCY="${CONCURRENCY:-20}"
# Which faults to inject. Override per CI matrix entry to cover the storage-tier
# and writer-fencing scenarios separately.
FAULTS="${FAULTS:-partition,s3-outage,s3-latency,trim}"

"$docker_dir/up.sh"

set +e
docker compose -f "$docker_dir/docker-compose.yml" exec -T control \
  lein run test --nodes-file /shared/nodes \
    --username root --ssh-private-key /shared/ssh_key \
    --time-limit "$TIME_LIMIT" --rate "$RATE" --concurrency "$CONCURRENCY" \
    --faults "$FAULTS"
status=$?
set -e

docker compose -f "$docker_dir/docker-compose.yml" down -v
exit $status
