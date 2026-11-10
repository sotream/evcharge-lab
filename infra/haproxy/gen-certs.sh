#!/usr/bin/env bash
# Lab CA + server certificate for the station edge (wss://station-lb:8443, localhost:28443).
# Idempotent: does nothing when the files exist. Production uses real certificates and OCPP security profile 2/3.
set -euo pipefail
dir="$(cd "$(dirname "$0")" && pwd)/certs"
if [ -f "$dir/ca.pem" ] && [ -f "$dir/server.pem" ]; then
  echo "certs exist in $dir"
  exit 0
fi
mkdir -p "$dir"
cd "$dir"
openssl req -x509 -newkey rsa:2048 -nodes -days 825 -subj "/CN=evcharge-lab-ca" -keyout ca.key -out ca.pem 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=station-lb" -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=DNS:station-lb,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > san.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 -extfile san.ext -out server.crt 2>/dev/null
cat server.crt server.key > server.pem # HAProxy wants certificate and key in one file
# ca.key and server.key stay private; server.pem is read by HAProxy as uid 99 through a bind mount, so it must be
# world-readable (a lab key; production keeps it in a secret store).
chmod 600 ca.key server.key
chmod 644 ca.pem server.pem server.crt
rm -f server.csr san.ext
echo "generated lab CA and server certificate in $dir"
