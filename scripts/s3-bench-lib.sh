# Shared helpers for the scripts that drive scripts/s3-bench-sweep.sh and
# summarise what it prints. Sourced, not run.

# Median of one field across a run's S3BENCH lines, read from stdin. `sed -n
# ...p` prints only what it matched, so a line that does not carry the field
# contributes nothing rather than passing itself through - a run that died after
# printing a partial line would otherwise reach the callers' python3 call as
# source text and, under `set -e`, take the whole sweep down with a SyntaxError.
# An empty result is what the callers check for.
#
# The upper of the two middles on an even count. Both callers default REPS to 2,
# and the failure this harness documents (a lost request surfacing as a timeout,
# on roughly half of runs, costing ~3.5x throughput) makes the lower of two the
# degraded run whenever it fires. `int(NR/2)+1` is 2 of 2 and 2 of 3.
median_field() {
  sed -n "s/.*$1=\([0-9.]*\).*/\1/p" | sort -n | awk '{a[NR]=$1} END{print a[int(NR/2)+1]}'
}
