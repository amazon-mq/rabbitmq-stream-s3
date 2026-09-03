#!/usr/bin/env bash
# Brings up (or tears down) the object store `remote_reader_s3_bench` measures
# against: MinIO, published straight to the host.
#
# Same shape as jepsen/docker/, minus TLS: the harness injects the connection
# pool's `open_fun` and dials by address, so no certificate, /etc/hosts entry or
# privileged port is needed.
#
# Nothing proxies MinIO. Latency is `tc netem` inside its network namespace,
# applied by the harness; see the shaping section of remote_reader_s3_bench for
# why a proxy cannot carry a latency benchmark.
#
# MINIO_DOMAIN makes MinIO route virtual-hosted-style addressing, which is what
# the AWS client uses (Host: <bucket>.s3.<region>.<tld>). MINIO_KMS_SECRET_KEY
# enables SSE-S3, because the client always sends
# `x-amz-server-side-encryption: AES256` and MinIO answers 501 without it.
set -euo pipefail

ACTION="${1:-up}"
ENGINE="${2:-podman}"

NET=s3bench
MINIO=s3bench-minio
BUCKET=jepsen
REGION=jepsen
MINIO_IMAGE="${S3_BENCH_MINIO:-quay.io/minio/minio:latest}"

# Published to the host. The harness has this hardcoded; keep them in step.
S3_PORT=19000

down() {
  $ENGINE rm -f "$MINIO" >/dev/null 2>&1 || true
  $ENGINE network rm "$NET" >/dev/null 2>&1 || true
  echo "s3-bench: down"
}

up() {
  down
  # Idempotent: the network survives `down` if a container still references it.
  $ENGINE network create "$NET" >/dev/null 2>&1 || true

  # A throwaway key: this store holds generated benchmark bytes and nothing else.
  local kms_key
  kms_key="$(head -c 32 /dev/urandom | base64 | tr -d '\n')"

  # /data in RAM. On the container's overlayfs an uncached 4 MiB range read
  # sustains about 9 MiB/s per connection, which would make the store's disk the
  # ceiling rather than the wire. The point of this harness is to model S3's
  # *network* behaviour, so the store itself must never be the limit.
  $ENGINE run -d --name "$MINIO" --network "$NET" \
    --tmpfs /data:rw,size=${S3_BENCH_TMPFS:-64}g \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    -e MINIO_DOMAIN=s3.jepsen.local \
    -e MINIO_REGION="$REGION" \
    -e MINIO_KMS_SECRET_KEY="minio-default-key:${kms_key}" \
    -p "${S3_PORT}:9000" \
    "$MINIO_IMAGE" server /data --address ":9000" >/dev/null


  # Create the bucket through the proxy, which also proves the whole path.
  for i in $(seq 1 60); do
    if AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
       AWS_DEFAULT_REGION="$REGION" \
       aws --endpoint-url "http://127.0.0.1:${S3_PORT}" s3 mb "s3://${BUCKET}" >/dev/null 2>&1; then
      break
    fi
    # Already existing is success, not a retry.
    if AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
       AWS_DEFAULT_REGION="$REGION" \
       aws --endpoint-url "http://127.0.0.1:${S3_PORT}" s3 ls "s3://${BUCKET}" >/dev/null 2>&1; then
      break
    fi
    [ "$i" = 60 ] && { echo "s3-bench: bucket never became reachable" >&2; exit 1; }
    sleep 0.5
  done

  echo "s3-bench: up (S3 on 127.0.0.1:${S3_PORT})"
}

case "$ACTION" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 {up|down} [podman|docker]" >&2; exit 2 ;;
esac
