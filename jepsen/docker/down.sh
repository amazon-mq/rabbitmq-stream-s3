#!/usr/bin/env bash
# Tears the cluster down: the opposite of up.sh.
#
# Kills any in-container test process FIRST. On podman, killing the host-side
# `docker compose exec` shell does NOT kill the in-container lein/JVM, so a hung
# `lein run test` keeps running and can contaminate the next run sharing this
# cluster. Then it brings the compose project down, removing its volumes and
# network.
#
#   ./down.sh            # stop the cluster, keep shared/ so a re-up is fast
#   ./down.sh --clean    # also remove generated shared/ (certs, keys, tarball)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
compose="$here/docker-compose.yml"

clean=0
for arg in "$@"; do
  case "$arg" in
    --clean) clean=1 ;;
    -h|--help) sed -n '2,11p' "$0"; exit 0 ;;
    *) echo "down.sh: unknown argument: $arg" >&2; exit 2 ;;
  esac
done

# Kill a possibly-hung in-container test process before tearing down (see above).
# Ignored if the control container is already gone or nothing is running.
echo "==> stopping any in-container test process"
docker compose -f "$compose" exec -T control pkill -f "lein run test" 2>/dev/null || true

echo "==> docker compose down -v"
docker compose -f "$compose" down -v

if [ "$clean" -eq 1 ]; then
  echo "==> removing generated shared/"
  rm -rf "$here/shared"
fi
