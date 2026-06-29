#!/usr/bin/env bash
# Generates a throwaway CA and a MinIO server certificate for the S3 endpoint.
#
# The broker's AWS backend connects with verify_peer against the system CA
# store and uses virtual-hosted-style addressing (<bucket>.<endpoint>), so the
# cert must be valid for both s3.jepsen.local and *.s3.jepsen.local, and the
# nodes must trust the CA (db.clj copies shared/ca.crt into the system store).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
out="$here/shared"
certs="$out/certs"
mkdir -p "$certs"

if [[ -f "$out/ca.crt" && -f "$certs/public.crt" ]]; then
  echo "certs already present in $out; delete them to regenerate"
  exit 0
fi

# --- CA ---
openssl req -x509 -nodes -newkey rsa:2048 -days 3650 \
  -keyout "$out/ca.key" -out "$out/ca.crt" \
  -subj "/CN=jepsen-stream-s3-ca"

# --- server cert (MinIO wants public.crt / private.key) ---
openssl req -nodes -newkey rsa:2048 \
  -keyout "$certs/private.key" -out "$certs/server.csr" \
  -subj "/CN=s3.jepsen.local"

openssl x509 -req -in "$certs/server.csr" \
  -CA "$out/ca.crt" -CAkey "$out/ca.key" -CAcreateserial \
  -days 3650 -out "$certs/public.crt" \
  -extfile <(printf "subjectAltName=DNS:s3.jepsen.local,DNS:*.s3.jepsen.local")

rm -f "$certs/server.csr"
echo "wrote $out/ca.crt and $certs/{public.crt,private.key}"
