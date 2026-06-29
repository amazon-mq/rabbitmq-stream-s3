#!/usr/bin/env bash
# Builds everything the test needs and brings the cluster up.
#
#   1. builds the broker generic-unix tarball (make package-generic-unix) and
#      stages it at shared/rabbitmq.tar.xz  (no release required)
#   2. generates the throwaway S3 CA + server cert
#   3. docker compose build + up
#
# After this, run the test:
#   docker compose exec control lein run test \
#     --nodes-file /shared/nodes --username root --password root \
#     --time-limit 300 --rate 200 --concurrency 20
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
shared="$here/shared"
# jepsen/docker -> jepsen -> rabbitmq_stream_s3 -> deps -> umbrella root
umbrella="$(cd "$here/../../../.." && pwd)"

mkdir -p "$shared"

echo "==> building broker tarball (make package-generic-unix) in $umbrella"
# Build serially: the rabbitmq_cli escript-zip step races under a parallel
# make (parallel recursive appends to the runtime-deps list duplicate a dep,
# so zip is handed the same ebin/dep_built twice -> "Duplicate filename on
# disk"). -j1 here overrides any inherited MAKEFLAGS=--jobs=N.
make -C "$umbrella" -j1 package-generic-unix
tarball="$(ls -t "$umbrella"/PACKAGES/rabbitmq-server-generic-unix-*.tar.xz | head -n1)"
cp "$tarball" "$shared/rabbitmq.tar.xz"
echo "    staged $(basename "$tarball") -> shared/rabbitmq.tar.xz"

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

Cluster is up. Run the test with:

  docker compose -f docker-compose.yml exec control \
    lein run test --nodes-file /shared/nodes \
      --username root --ssh-private-key /shared/ssh_key \
      --time-limit 300 --rate 200 --concurrency 20

Results land under jepsen.streams3/store/ (mounted on the host).
EOF
