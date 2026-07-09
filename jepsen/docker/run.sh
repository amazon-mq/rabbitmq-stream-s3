#!/usr/bin/env bash
# Runs one Jepsen test against the already-up cluster (see up.sh) and, unlike
# ci/run-jepsen.sh, does NOT tear it down afterwards, so you can iterate on the
# harness and inspect store/ between runs.
#
# Tunables (environment, all optional):
#   FAULTS           comma-separated nemeses (default: partition,s3-outage,s3-latency)
#   FINAL_READ_TIER  tier the final reads exercise: local or s3 (default: s3)
#   TIME_LIMIT       workload seconds     (default 120)
#   RATE             ops/sec              (default 100)
#   CONCURRENCY      workers              (default 20)
# Any extra arguments are passed through to `lein run test`.
#
#   ./run.sh
#   FAULTS=s3-outage,leader-move ./run.sh
#   FAULTS=leader-move TIME_LIMIT=300 ./run.sh --final-time-limit 240
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
compose="$here/docker-compose.yml"

FAULTS="${FAULTS:-partition,s3-outage,s3-latency}"
FINAL_READ_TIER="${FINAL_READ_TIER:-s3}"
TIME_LIMIT="${TIME_LIMIT:-120}"
RATE="${RATE:-100}"
CONCURRENCY="${CONCURRENCY:-20}"

echo "==> lein run test --faults $FAULTS --final-read-tier $FINAL_READ_TIER (time-limit ${TIME_LIMIT}s)"
exec docker compose -f "$compose" exec -T control \
  lein run test --nodes-file /shared/nodes \
    --username root --ssh-private-key /shared/ssh_key \
    --time-limit "$TIME_LIMIT" --rate "$RATE" --concurrency "$CONCURRENCY" \
    --faults "$FAULTS" --final-read-tier "$FINAL_READ_TIER" "$@"
