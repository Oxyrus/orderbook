#!/usr/bin/env bash
# Creates the application topics. Idempotent.
#
# Provision topics explicitly instead of relying on broker auto-creation:
# partition count bounds consumer parallelism and is painful to change later
# (adding partitions remaps keys, which breaks per-key ordering for in-flight
# data), and retention must outlive the longest consumer outage you plan to
# recover from without a replay. In production, manage these with Terraform,
# Strimzi KafkaTopic resources or your platform's equivalent.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
KT=/opt/kafka/bin/kafka-topics.sh
RF="${REPLICATION_FACTOR:-1}"

create() {
  local topic=$1 partitions=$2; shift 2
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$topic" \
    --partitions "$partitions" --replication-factor "$RF" "$@" 2>&1 \
    | grep -v "collide\|^WARNING: Due to limitations" || true
}

# Domain events from the outbox. Keyed by order id: per-order ordering.
create orderbook.events.order 12 --config retention.ms=1209600000 --config min.insync.replicas=1
# Messages the projection could not process; inspect and replay manually.
create orderbook.events.order.dlq 1 --config retention.ms=2592000000
# Row-level CDC topics (deploy/debezium/tables-*.json). Compacted: the
# latest row state per primary key is kept forever, so a new consumer can
# bootstrap the full table from the topic.
create orderbook.cdc.public.orders 12 --config cleanup.policy=compact
create orderbook.cdc.public.order_lines 12 --config cleanup.policy=compact

$KT --bootstrap-server "$BOOTSTRAP" --list | grep '^orderbook\.'
