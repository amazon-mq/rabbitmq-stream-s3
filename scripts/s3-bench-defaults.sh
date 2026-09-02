#!/usr/bin/env bash
# Compares candidate plugin defaults against the ceiling a consumer can reach.
#
# The question this answers is not "what did some past run measure" but "what
# should the plugin ship with". Every profile is run as a production consumer
# would see it: a single consumer reading remote-tier data, bounded by its own
# client connection, and the number that matters is how much of that ceiling the
# defaults let it reach.
#
# The manifest shape is derived rather than chosen. Past `rebalance_threshold`
# (1024) entries the manifest grows group nodes, so `fragment_target_size`
# decides it: a 76 GiB stream in 64 MiB fragments is ~1,222 entries and grouped,
# while the same stream in 256 MiB fragments is ~305 and flat.
#
# That shape is close to free on the read path. A rebalance factors 1024 leaves
# into one group, and the iterator's `descend/2` pushes the parent onto its
# stack and swaps the child's entries in, so the leaves behind a group node are
# walked in memory - a sequential reader pays one descent, not one per fragment
# transition. The column is kept because the shape is real, not because it is
# expected to separate the profiles.
#
#   ./scripts/s3-bench-defaults.sh
#   S3B_STREAM_GIB=150 ./scripts/s3-bench-defaults.sh
set -euo pipefail

cd "$(dirname "$0")/.."

# The consumer's own ceiling. Measured on the stress rig as a single TCP flow's
# per-flow cap; every percentage below is a percentage of this.
CLIENT_CAP="${S3B_CLIENT_CAP:-589}"

# How much data the stream holds, which is what decides the manifest shape for a
# given fragment size.
STREAM_GIB="${S3B_STREAM_GIB:-76}"
REBALANCE_THRESHOLD=1024

# READ-SIDE PROFILES. `fragment_target_size` stays at its shipped 64 MiB.
#
# Raising it is a write-side change, not a tuning knob: it coarsens retention,
# which only evaluates at fragment granularity, enlarges the single S3 object
# each upload produces, and lengthens time to first byte for a consumer
# attaching near a fragment's tail. Buying read throughput with it is a trade
# against the write path, so it does not belong in a comparison whose output is
# "what should the plugin ship with".
#
# The search is on, because that is what ships. It makes the depth column a
# ceiling rather than an operating point - the reader starts at one request and
# doubles until the rate stops answering - so what a profile ran at is the
# measured `inflight` column, not the one it asked for.
export S3B_AUTO_TUNE=1

# name | request MiB | window MiB | depth | fragment MiB | pool_min
PROFILES=(
  "shipped-defaults        4  128   64    64   2"
  "pre-4.4-defaults        4   32    8    64   2"
  "window-x4               4  128    8    64   2"
  "depth-x4                4   32   32    64   2"
  "window-and-depth-x4     4  128   32    64   2"
  "window-and-depth-x8     4  256   64    64   2"
  "warm-pool               4  128   32    64  40"
  "smaller-requests        2  128   32    64   2"
)

# WRITE-SIDE TRADES, run only with S3B_WRITE_TRADES=1. These raise
# `fragment_target_size`, which is what the one-fragment lookahead currently
# forces anyone to do to go faster: reach at a fragment tail is one fragment, so
# read throughput is bought with a write-side cost. Their value here is to
# measure what that limitation costs - the gap between these and the read-side
# profiles is the case for prefetching across more than one fragment.
if [ "${S3B_WRITE_TRADES:-0}" = "1" ]; then
  PROFILES+=(
    "larger-fragments-x4    4  128   32   256   2"
    "larger-fragments-x8    4  128   32   512   2"
  )
fi

REPS="${S3B_REPS:-2}"

# shellcheck source=scripts/s3-bench-lib.sh
. ./scripts/s3-bench-lib.sh

printf '\n%-24s %7s %7s %7s %7s %9s %9s %8s %s\n' \
  profile req win depth frag 'MiB/s' '% of cap' inflight manifest
printf '%s\n' "-------------------------------------------------------------------------------------------------"

for profile in "${PROFILES[@]}"; do
  read -r name req win depth frag pool <<<"$profile"

  entries=$(( STREAM_GIB * 1024 / frag ))
  if [ "$entries" -gt "$REBALANCE_THRESHOLD" ]; then manifest=grouped; else manifest=flat; fi

  # Budget scales with fragment size so every profile runs long enough to leave
  # the prefetch window's ramp, and so the seeded stream stays a sane size.
  budget=$(( frag * 32 ))
  [ "$budget" -lt 2048 ] && budget=2048
  [ "$budget" -gt 8192 ] && budget=8192

  out=$(S3B_REPS="$REPS" S3B_BUDGET_MIB="$budget" S3B_REQUEST_MIB="$req" \
        S3B_WINDOW_MIB="$win" S3B_FRAGMENT_MIB="$frag" S3B_MANIFEST="$manifest" \
        S3B_DRAIN_MIBS="$CLIENT_CAP" S3B_POOL_MIN="$pool" \
        ./scripts/s3-bench-sweep.sh depth "$depth" 2>&1 | grep '^S3BENCH' || true)

  [ -n "$out" ] || { printf '%-24s %7s\n' "$name" FAILED; continue; }

  mibs=$(echo "$out" | median_field mib_s)
  infl=$(echo "$out" | median_field inflight_avg)
  { [ -n "$mibs" ] && [ -n "$infl" ]; } || { printf '%-24s %7s\n' "$name" FAILED; continue; }

  bad=$(echo "$out" | grep -c 'timeouts=[1-9]' || true)
  note=""
  [ "$bad" != "0" ] && note="($bad run(s) hit #279 - not comparable)"

  # A cap of 0 means no client ceiling: the percentage column has no
  # denominator then, which is the point of running that way - it is how the
  # profiles are told apart once they all saturate.
  python3 -c "
cap = $CLIENT_CAP
pct = f'{$mibs*100/cap:8.1f}%' if cap else '       -'
print(f'{\"$name\":<24} {$req:7d} {$win:7d} {$depth:7d} {$frag:7d} {$mibs:9.1f} {pct} {$infl:8.1f} $manifest $note')"
done

echo
if [ "$CLIENT_CAP" = "0" ]; then
  echo "no client ceiling; ${STREAM_GIB} GiB stream (manifest shape follows fragment count)"
else
  echo "client ceiling ${CLIENT_CAP} MiB/s; ${STREAM_GIB} GiB stream (manifest shape follows fragment count)"
fi
