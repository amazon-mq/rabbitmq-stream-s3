# Shared helpers for the scripts that drive scripts/s3-bench-sweep.sh and
# summarise what it prints. Sourced, not run.

# Median of one field across a run's S3BENCH lines, read from stdin. `sed -n
# ...p` prints only what it matched, so a line that does not carry the field
# contributes nothing rather than passing itself through - a run that died after
# printing a partial line would otherwise reach the callers' python3 call as
# source text and, under `set -e`, take the whole sweep down with a SyntaxError.
# An empty result is what the callers check for.
median_field() {
  sed -n "s/.*$1=\([0-9.]*\).*/\1/p" | sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'
}
