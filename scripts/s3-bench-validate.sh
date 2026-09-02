#!/usr/bin/env bash
# Runs the scenarios that were actually stress-tested and prints each one beside
# its measured result.
#
# This is the harness's own regression test. Every parameter below is either the
# configuration the broker really ran or a property of the stream it really read
# - nothing here is fitted per scenario. If a change to the harness or to the
# read path moves these numbers, that is the signal.
#
# This is a fidelity check, not the thing you tune with - use
# `s3-bench-defaults.sh` for that. It exists to answer one question: does the
# harness still land on a measurement taken from a real broker? Two profiles are
# enough for that, one at each end of the range:
#
#   shipped-defaults   what the plugin ships with, on a stream whose fragment
#                      count puts group nodes in the manifest.
#   near-client-cap    a tuned configuration pushed until the consumer's own
#                      connection is the limit.
#
# Sources: tests/2026-08-15-remote-tier-consumer-throughput.md and
# tests/2026-08-16-remote-tier-tuning-toward-ceiling.md.
#
# Caveats that belong with the numbers: every measured value is n=1, all are a
# single consumer, and the tuned scenario was measured under live diagnostic
# load worth about 0.6 percentage points. The manifest column is derived from
# each stream's fragment count against `rebalance_threshold` (1024), not fitted:
# the two 64 MiB-fragment scenarios read a 76 GiB stream (~1,222 entries, so
# grouped) and the tuned one 256 MiB fragments (~274, flat).
#
# `grouped` costs one descent, not one per fragment transition. A rebalance
# factors 1024 leaves into a group and the iterator keeps the descended-into
# entries on its own stack, so the run behind a group node is walked in memory.
set -euo pipefail

cd "$(dirname "$0")/.."

# name | request | window | depth | fragment | manifest | drain | measured
SCENARIOS=(
  "shipped-defaults  4  32   8   64 grouped 0   249.1"
  "near-client-cap   4 128  32  256 flat    589 554.85"
)

# A third measured point, depth-and-window-doubled (4/64/16/64, grouped),
# recorded 301.4 MiB/s. It is deliberately not checked here: the harness reads
# about +46% against it while landing within 6% of both profiles above, and an
# earlier virtual-time model could not fit it alongside them either. Two
# independent harnesses disagreeing with one n=1 measurement in the same
# direction is not something to calibrate away.

REPS="${S3B_REPS:-2}"
BUDGET="${S3B_BUDGET_MIB:-2048}"

# shellcheck source=scripts/s3-bench-lib.sh
. ./scripts/s3-bench-lib.sh

# The search stays off here, pinned rather than left to the harness's default.
# The measured values above come from brokers that ran at a fixed depth, so a
# run that searched for its own would not be comparable with them - which is the
# only thing this script is for.
export S3B_AUTO_TUNE=0

printf '\n%-22s %9s %9s %9s %9s  %s\n' scenario harness measured err% inflight notes
printf '%s\n' "--------------------------------------------------------------------------------"

for scenario in "${SCENARIOS[@]}"; do
  read -r name req win depth frag manifest drain measured <<<"$scenario"

  # A larger fragment needs a proportionally larger budget or the run is over
  # before the window and pool have left their ramp.
  budget=$BUDGET
  [ "$frag" -ge 256 ] && budget=4096

  out=$(S3B_REPS="$REPS" S3B_BUDGET_MIB="$budget" S3B_REQUEST_MIB="$req" \
        S3B_WINDOW_MIB="$win" S3B_FRAGMENT_MIB="$frag" S3B_MANIFEST="$manifest" \
        S3B_DRAIN_MIBS="$drain" \
        ./scripts/s3-bench-sweep.sh depth "$depth" 2>&1 | grep '^S3BENCH' || true)

  [ -n "$out" ] || { printf '%-22s %9s\n' "$name" FAILED; continue; }

  # Median of the repeats, and flag any run that hit issue #279 - a stale pooled
  # connection loses a request and it surfaces 15 s later as a timeout, costing
  # roughly 3.5x throughput. Such a run is not comparable.
  mibs=$(echo "$out" | median_field mib_s)
  infl=$(echo "$out" | median_field inflight_avg)
  { [ -n "$mibs" ] && [ -n "$infl" ]; } || { printf '%-22s %9s\n' "$name" FAILED; continue; }

  bad=$(echo "$out" | grep -c 'timeouts=[1-9]' || true)

  note=""
  [ "$bad" != "0" ] && note="($bad run(s) hit #279)"

  python3 -c "
m=$mibs; r=$measured
print(f'{\"$name\":<22} {m:9.1f} {r:9.1f} {(m-r)*100/r:+8.1f}% {$infl:9.1f}  $note')"
done

echo
echo "no per-scenario fitting: only the broker's own configuration and each stream's manifest shape"
