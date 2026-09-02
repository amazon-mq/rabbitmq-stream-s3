#!/usr/bin/env bash
# Sweeps one axis of the remote-reader S3 harness, one configuration per run.
#
# Each point gets a fresh VM. That is not caution, it is the finding: sweeping
# inside a single run contaminated its own results - a 64 MiB fragment case
# measured 7.8 MiB/s as the fourth row of an in-process sweep and 107.7 MiB/s
# run on its own - and every scenario inherited the previous one's warm
# connection pool and grown prefetch window.
#
# MinIO keeps its data across points, so a sweep re-seeds only when the fragment
# size changes. This script clears and reapplies the netem qdisc around each
# point; the harness does not, and cannot - see `netem` below.
#
#   ./scripts/s3-bench-sweep.sh depth 1 4 8 16 32
#   ./scripts/s3-bench-sweep.sh fragment 8 16 32 64 128
#   ./scripts/s3-bench-sweep.sh rate 100 200 400      # aggregate mbit
#   ./scripts/s3-bench-sweep.sh substrate 1 4 8 16 32
set -euo pipefail

cd "$(dirname "$0")/.."

AXIS="${1:?usage: $0 <depth|fragment|request|rate|substrate> <value>...}"
shift
VALUES=("$@")
[ ${#VALUES[@]} -gt 0 ] || { echo "no values given" >&2; exit 2; }

ENGINE="${S3_BENCH_ENGINE:-podman}"
NETSHOOT="${S3_BENCH_NETSHOOT:-docker.io/nicolaka/netshoot:latest}"

# MinIO's address on the podman bridge. The VM runs inside the rootless network
# namespace and talks to this directly: published ports are forwarded by a
# userspace proxy that absorbs traffic shaping, so a run through 127.0.0.1
# believes it is shaped and is not.
# `|| true` so a missing container reaches the check below rather than exiting
# here: `set -e` acts on the assignment's own status, which is the
# substitution's, so the guard would never get to say what was wrong.
S3B_HOST="$($ENGINE inspect s3bench-minio --format '{{.NetworkSettings.Networks.s3bench.IPAddress}}' 2>/dev/null || true)"
[ -n "$S3B_HOST" ] || { echo "s3bench-minio not running; try make s3-bench-up" >&2; exit 1; }
export S3B_HOST S3B_PORT=9000

# Latency is a qdisc on MinIO's interface, applied by us rather than by the
# harness: it has to go on *after* seeding (a 64 MiB upload across a delayed ACK
# path times out) and the VM cannot run nested podman from inside the namespace.
# $1 = delay ms (0 for none). S3B_AGG_MBIT optionally adds an aggregate rate
# limit, which is what netem's `rate` is - shared across all flows, not
# per-connection. That is the right shape for an instance-wide or
# broker-to-store ceiling.
netem() {
  $ENGINE run --rm --network container:s3bench-minio --cap-add NET_ADMIN "$NETSHOOT" \
    tc qdisc del dev eth0 root >/dev/null 2>&1 || true
  local delay="${1:-0}" rate="${S3B_AGG_MBIT:-0}" jitter="${S3B_JITTER_MS:-0}" args=""
  [ "$delay" = "0" ] && [ "$rate" = "0" ] && return 0
  [ "$delay" != "0" ] && args="delay ${delay}ms"
  [ "$delay" != "0" ] && [ "$jitter" != "0" ] && args="$args ${jitter}ms"
  [ "$rate" != "0" ] && args="$args rate ${rate}mbit"
  $ENGINE run --rm --network container:s3bench-minio --cap-add NET_ADMIN "$NETSHOOT" \
    tc qdisc add dev eth0 root netem $args limit 200000 >/dev/null 2>&1
}

# Run the VM inside the rootless network namespace.
in_netns() {
  $ENGINE unshare --rootless-netns env ERL_LIBS="$(cd .. && pwd)" \
    erl -noshell -pa ebin -pa test -eval 'remote_reader_s3_bench:run(), halt(0).'
}

# Held constant unless the axis being swept overrides it.
export S3B_DEPTH="${S3B_DEPTH:-8}"
export S3B_WINDOW_MIB="${S3B_WINDOW_MIB:-128}"
export S3B_REQUEST_MIB="${S3B_REQUEST_MIB:-4}"
export S3B_FRAGMENT_MIB="${S3B_FRAGMENT_MIB:-64}"
export S3B_BUDGET_MIB="${S3B_BUDGET_MIB:-128}"
# Same-region S3 round trip, near enough. Sweeps run shaped by default because
# an unshaped loopback hides what concurrency buys: with no round trip to hide,
# a reader gains nothing from having more requests in flight. Set 0 to measure
# the store's own ceiling instead (see netem/1 in the harness).
export S3B_LATENCY_MS="${S3B_LATENCY_MS:-22}"
export S3B_JITTER_MS="${S3B_JITTER_MS:-0}"
export S3B_POOL_MIN="${S3B_POOL_MIN:-2}"
# Wire profile:
#   latency  22 ms      same-region S3 round trip
#   recbuf   512 KB     sets per-connection rate to ~34 MiB/s (real S3: ~31)
#
# S3B_AGG_MBIT (an aggregate, all-flows rate limit) defaults to off. Setting it
# to 2530 reproduces the stock-defaults and depth-doubled scenarios to within
# 4%, but it is not a mechanism: the tuned-near-client-cap scenario reached
# 554.85 MiB/s on the same broker, which no 301 MiB/s ceiling permits. It is
# available for experiments, not as a default.
export S3B_RECBUF_KB="${S3B_RECBUF_KB:-512}"
export S3B_AGG_MBIT="${S3B_AGG_MBIT:-0}"


echo "sweeping $AXIS over: ${VALUES[*]}"
echo "fixed: depth=$S3B_DEPTH window=${S3B_WINDOW_MIB}M request=${S3B_REQUEST_MIB}M" \
     "fragment=${S3B_FRAGMENT_MIB}M latency=${S3B_LATENCY_MS}ms (0 = unmodelled)"
echo

# Repeats per point. A single run is not a measurement here: a stale pooled
# connection loses a request and it surfaces 15 s later as a request timeout
# (the `retry => 0` consequence documented in rabbitmq_stream_s3_api_aws_pool,
# issue #279), which costs ~3.5x throughput for the rest of the run. It happens
# on roughly half of runs, so the `timeouts=` column on each line is what says
# whether a number is comparable - not the throughput on its own.
REPS="${S3B_REPS:-3}"

for v in "${VALUES[@]}"; do
  case "$AXIS" in
    depth)     export S3B_DEPTH="$v"; unset S3B_SUBSTRATE || true ;;
    fragment)  export S3B_FRAGMENT_MIB="$v"; unset S3B_SUBSTRATE || true ;;
    request)   export S3B_REQUEST_MIB="$v"; unset S3B_SUBSTRATE || true ;;
    rate)      export S3B_AGG_MBIT="$v"; unset S3B_SUBSTRATE || true ;;
    substrate) export S3B_DEPTH="$v"; export S3B_SUBSTRATE=1 ;;
    *) echo "unknown axis: $AXIS" >&2; exit 2 ;;
  esac
  # Seed unshaped, then shape, then measure. The harness skips seeding when the
  # data is already present, so this costs nothing after the first point.
  netem 0
  S3B_SEED_ONLY=1 in_netns >/dev/null 2>&1 || true
  netem "$S3B_LATENCY_MS"
  for _rep in $(seq 1 "$REPS"); do
    in_netns 2>&1 | grep -E '^S3BENCH|^WARNING' || { echo "  $AXIS=$v FAILED"; }
  done
  netem 0
done
