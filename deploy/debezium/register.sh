#!/usr/bin/env bash
# Creates or updates Debezium connectors through the Kafka Connect REST API.
#
#   register.sh outbox    # transactional outbox → orderbook.events.<aggregate>
#   register.sh tables    # row-level CDC of orders → orderbook.cdc.public.<table>
#
# There is one connector per Citus worker: shards (and their WAL) live on the
# workers, and logical replication slots are per node. When you add a worker,
# add a JSON file for it and re-run this script BEFORE rebalancing shards onto
# it, or the changes written there before the slot exists are never captured.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="$(cd "$(dirname "$0")" && pwd)"
kind="${1:-outbox}"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

shopt -s nullglob
files=("$DIR"/"$kind"-*.json)
if [ ${#files[@]} -eq 0 ]; then
  echo "no connector definitions match $DIR/$kind-*.json" >&2
  exit 1
fi

for i in $(seq 1 60); do
  curl -sf "$CONNECT_URL/connectors" >/dev/null && break
  echo "waiting for Kafka Connect at $CONNECT_URL ($i)..."
  sleep 2
done

for f in "${files[@]}"; do
  name="orderbook-$(basename "$f" .json)"
  # PUT /connectors/<name>/config is an upsert, so re-running is safe.
  echo "registering $name"
  code=$(curl -s -o "$tmp" -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
    --data @"$f" "$CONNECT_URL/connectors/$name/config")
  if [ "$code" -ge 300 ]; then
    echo "failed ($code):" >&2
    cat "$tmp" >&2
    echo >&2
    exit 1
  fi
done

sleep 3
for f in "${files[@]}"; do
  name="orderbook-$(basename "$f" .json)"
  echo "$name: $(curl -s "$CONNECT_URL/connectors/$name/status")"
done
