#!/usr/bin/env bash
# Builds everything the test needs and brings the cluster up.
#
#   1. stages the broker generic-unix tarball at shared/rabbitmq.tar.xz, either
#      by building it (make package-generic-unix) or from a prebuilt path
#   2. generates the throwaway S3 CA + server cert
#   3. docker compose build + up
#
# Tarball selection (in order):
#   TARBALL=/path/to/rmq-generic-unix.tar.xz ./up.sh   # stage this, skip build
#   SKIP_BUILD=1 ./up.sh                                # reuse already-staged one
#   ./up.sh                                             # build from the umbrella
#
# After this, run the test with ./run.sh (or by hand), and tear down with
# ./down.sh.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
shared="$here/shared"
# jepsen/docker -> jepsen -> rabbitmq_stream_s3 -> deps -> umbrella root
umbrella="$(cd "$here/../../../.." && pwd)"

mkdir -p "$shared"

if [ -n "${TARBALL:-}" ]; then
  echo "==> staging prebuilt tarball: $TARBALL"
  [ -f "$TARBALL" ] || { echo "up.sh: TARBALL not found: $TARBALL" >&2; exit 1; }
  cp "$TARBALL" "$shared/rabbitmq.tar.xz"
elif [ -n "${SKIP_BUILD:-}" ] && [ -f "$shared/rabbitmq.tar.xz" ]; then
  echo "==> reusing already-staged shared/rabbitmq.tar.xz (SKIP_BUILD)"
else
  echo "==> building broker tarball (make package-generic-unix) in $umbrella"
  # Build serially: the rabbitmq_cli escript-zip step races under a parallel
  # make (parallel recursive appends to the runtime-deps list duplicate a dep,
  # so zip is handed the same ebin/dep_built twice -> "Duplicate filename on
  # disk"). -j1 here overrides any inherited MAKEFLAGS=--jobs=N.
  if ! make -C "$umbrella" -j1 package-generic-unix; then
    cat >&2 <<'MSG'

up.sh: package-generic-unix failed. If the error above was
    ERROR: Duplicate filename on disk: dep_built
the umbrella is pre-compiled (e.g. by another checkout), so erlang.mk's
escript-zip dedup is skipped when rabbitmq_cli builds as a dependency and a
doubled dep entry breaks 7z. It is not a parallelism issue; -j1 does not help.
Fix by building from a clean tree (a fresh clone, as CI does), or stage a
prebuilt tarball instead:
    TARBALL=/path/to/rabbitmq-server-generic-unix-*.tar.xz ./up.sh
MSG
    exit 1
  fi
  tarball="$(ls -t "$umbrella"/PACKAGES/rabbitmq-server-generic-unix-*.tar.xz | head -n1)"
  cp "$tarball" "$shared/rabbitmq.tar.xz"
  echo "    staged $(basename "$tarball") -> shared/rabbitmq.tar.xz"
fi

echo "==> generating S3 certs"
"$here/gen-certs.sh"

echo "==> generating SSH key for control -> nodes"
# jepsen runs commands and downloads logs (scp) over SSH; key auth makes both
# work (password auth does not cover the scp log download). The node image
# installs ssh_key.pub as root's authorized_keys at startup.
if [ ! -f "$shared/ssh_key" ]; then
  ssh-keygen -t ed25519 -N '' -C jepsen-streams3 -f "$shared/ssh_key" >/dev/null
  chmod 600 "$shared/ssh_key"
fi

echo "==> writing nodes file"
printf 'n1\nn2\nn3\nn4\nn5\n' > "$shared/nodes"

echo "==> docker compose up"
docker compose -f "$here/docker-compose.yml" build
docker compose -f "$here/docker-compose.yml" up -d

cat <<'EOF'

Cluster is up. Run a test with:

  ./run.sh                                # default faults
  FAULTS=leader-move ./run.sh             # pick faults
  FINAL_READ_TIER=local ./run.sh          # final reads from the local tier

Tear down with:

  ./down.sh                               # keep shared/ for a fast re-up

Results land under jepsen.streams3/store/ (mounted on the host).
EOF
