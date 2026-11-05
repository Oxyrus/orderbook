# Change data capture with Debezium

Orderbook uses [Debezium](https://debezium.io/) to turn committed database
changes into Kafka messages. It reads PostgreSQL's write-ahead log (WAL)
through logical replication. The application never writes to Kafka
directly. This document covers what it is used for, how it works on a
sharded Citus cluster, the guarantees you get and how to operate it.

- [Use cases](#use-cases)
- [Architecture](#architecture)
- [Use case 1: the transactional outbox](#use-case-1-the-transactional-outbox)
- [Use case 2: row-level CDC for analytics and search](#use-case-2-row-level-cdc-for-analytics-and-search)
- [Citus specifics](#citus-specifics)
- [Delivery guarantees and ordering](#delivery-guarantees-and-ordering)
- [The consumer (`api worker`)](#the-consumer-api-worker)
- [Running it locally](#running-it-locally)
- [Operations](#operations)
- [Production checklist](#production-checklist)

## Use cases

| Use case | What gets published | Consumers | Here |
|---|---|---|---|
| **Domain events (transactional outbox)** | Events the service emits on purpose (`order.placed`, `order.cancelled`), in a stable, versioned contract | Other services, projections/read models, notifications, webhooks, workflow engines | `outbox` table → `orderbook.events.order` → `api worker` maintains `tenant_daily_sales` |
| **Row-level CDC (data replication)** | Every insert/update/delete of selected tables, as rows | Data warehouse / lakehouse (ClickHouse, BigQuery, Snowflake, Iceberg via a sink connector), search index (OpenSearch), cache invalidation, audit | `orders`, `order_lines` → `orderbook.cdc.public.*` (optional, `make cdc-register-tables`) |

Other common uses of the same machinery: zero-downtime database migrations
(stream the old database into the new one, then cut over), feeding
microservices extracted from a monolith during a strangler-fig migration,
and keeping caches/CQRS read models in sync without dual writes.

Why not just publish to Kafka from the request handler? Because
"write DB, then publish" is a **dual write**. If the process dies, or Kafka
is unavailable, between the two steps, the database and the event stream
disagree forever: a phantom event, or a lost one. Writing the event into an
outbox table in the same transaction, and letting Debezium ship it from the
WAL, makes publication atomic with the commit. It also keeps Kafka out of
the request path: order taking keeps working while Kafka is down, and the
events flow once it is back.

## Architecture

```
  POST /v1/orders
        │
        ▼
  ┌─────────────┐   one single-shard transaction:
  │  order svc  │── INSERT orders, order_lines, outbox ──┐
  └─────────────┘                                        │
                         ┌───────────── Citus ───────────▼──────────────┐
                         │ coordinator   worker1 (tenants A, C, ...)    │
                         │               worker2 (tenants B, D, ...)    │
                         └──────────────────┬───────────────┬───────────┘
                          slot debezium_outbox (per worker) │ WAL, pgoutput,
                                            │               │ shard names normalised
                         ┌──────────────────▼───────────────▼───────────┐
                         │ Kafka Connect: orderbook-outbox-worker1/2     │
                         │   EventRouter SMT: topic by aggregatetype,   │
                         │   key = aggregateid, headers id/type/tenant  │
                         └──────────────────┬───────────────────────────┘
                                            ▼
                               Kafka  orderbook.events.order
                                            │  consumer group orderbook-sales-projection
                                            ▼
                         ┌──────────────────────────────────────────────┐
                         │ api worker: dedupe on event id + apply delta │──▶ tenant_daily_sales
                         │ in one single-shard transaction; DLQ on fail │    (GET /v1/sales/daily)
                         └──────────────────────────────────────────────┘
```

## Use case 1: the transactional outbox

### Writing events

```go
err = s.tx.WithTx(ctx, func(ctx context.Context) error {
    // ... change state ...
    _, err := s.events.Add(ctx, outbox.Event{
        TenantID:      tenantID,
        AggregateType: "order",            // → topic orderbook.events.order
        AggregateID:   o.ID.String(),      // → Kafka key (per-order ordering)
        Type:          "order.placed",     // → event_type header
        Payload:       placedEvent(o),     // → message value (JSON)
    })
    return err
})
```

`outbox.Writer.Add` refuses to run outside a transaction
(`ErrNoTransaction`). It also records the W3C `traceparent` of the current
request, so the consumer's span continues the request's trace across Kafka.

### The table and the publication

`migrations/00004_create_outbox.sql`:

- columns follow the Debezium outbox router defaults (`id`, `aggregatetype`,
  `aggregateid`, `type`, `payload`), plus `tenant_id`, `traceparent` and
  `created_at`;
- distributed by `tenant_id` and **co-located** with the tenant's data, so
  the outbox insert never turns a single-shard transaction into a
  distributed one;
- the publication only carries inserts:
  `CREATE PUBLICATION orderbook_outbox FOR TABLE outbox WITH (publish = 'insert')`.
  Rows can therefore be deleted at any time without emitting anything.
  `api worker` prunes rows older than `OUTBOX_RETENTION` (24h). Debezium
  reads events from the WAL, not from the table; the rows stay around only
  for debugging and manual replays.

### The connector

`deploy/debezium/outbox-worker{1,2}.json` (essentials):

```json
"plugin.name": "pgoutput",
"publication.name": "orderbook_outbox",
"publication.autocreate.mode": "disabled",
"slot.name": "debezium_outbox",
"table.include.list": "public.outbox",
"snapshot.mode": "no_data",
"transforms": "outbox",
"transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
"transforms.outbox.route.by.field": "aggregatetype",
"transforms.outbox.route.topic.replacement": "orderbook.events.${routedByValue}",
"transforms.outbox.table.fields.additional.placement": "type:header:event_type,tenant_id:header:tenant_id,traceparent:header:traceparent"
```

- `snapshot.mode: no_data`: the outbox's history is irrelevant; stream from
  the slot's creation onwards.
- The router adds an `id` header (the outbox row id, a UUIDv7), which
  consumers use for deduplication.
- Values are passed through as the raw JSON payload (`StringConverter`).
  The contract lives in `internal/order/events.go`.
- A Kafka message looks like this (from `make events`):

  ```
  headers: id:01a1ad65-...,event_type:order.placed,tenant_id:acme,traceparent:00-4bf9...-01
  key:     01a1ad65-edfc-7a50-b899-904a2246796c
  value:   {"order_id":"01a1ad65-...","tenant_id":"acme","currency":"USD","total_minor":8749,...}
  ```

### Event contract rules

Events are a public API:

- add fields freely; never remove, rename or change the meaning of one;
- breaking changes get a new type (`order.placed.v2`), and you publish both
  until every consumer has migrated;
- consumers ignore unknown event types and unknown fields;
- put everything consumers need in the payload (avoid "fat callbacks" to the
  API: the data may have changed by the time they call).

## Use case 2: row-level CDC for analytics and search

`deploy/debezium/tables-worker{1,2}.json` streams every change of `orders`
and `order_lines` (publication `orderbook_tables`, migration 00006) to
`orderbook.cdc.public.orders` / `.order_lines`:

- `ExtractNewRecordState` flattens Debezium's envelope into the row itself,
  plus `__op` (c/u/d/r), `__source_ts_ms`, `__source_lsn` and `__deleted`;
- topics are **compacted** (`deploy/kafka/create-topics.sh`), keyed by the
  primary key `(tenant_id, id)`, so Kafka keeps the latest state of every
  row. A new consumer can bootstrap the whole table from the topic;
- deletes become a row with `__deleted: "true"`
  (`delete.tombstone.handling.mode: rewrite`), which sinks can turn into
  deletes or soft deletes.

```bash
make cdc-register-tables
make events topic=orderbook.cdc.public.orders
```

Typical sinks: the Debezium JDBC sink or ClickHouse Kafka engine for
analytics, the OpenSearch sink connector for search, or a small consumer
for cache invalidation.

**Initial load.** These connectors use `snapshot.mode: no_data`. A Debezium
snapshot taken through a worker would run a distributed query over the
whole cluster, which is not consistent with any single worker's slot
position. For a backfill:

1. register the connectors first (the slots now retain every change), then
2. copy the existing rows from the coordinator into the sink (e.g. `COPY
   (SELECT ...) TO` / a batch job), and
3. let the stream catch up. Applying changes as upserts keyed by primary
   key, using `__source_lsn`/`updated_at` to discard stale versions, makes
   the overlap harmless.

## Citus specifics

Logical decoding on a Citus cluster differs from a single PostgreSQL server
in four ways. All of them are reflected in the configuration.

1. **Changes are decoded where the shards are: on the workers.** Replication
   slots are per node, so there is **one connector per worker** (it would be
   one per node if the coordinator held shards). Each connector has its own
   slot and its own `topic.prefix`, which is its logical server name, and
   all of them publish to the same topics.

   > Two connectors must never share a `topic.prefix`. Debezium keys its
   > stored offsets (and JMX metrics) by it, so they would overwrite each
   > other's positions. The table connectors use per-worker prefixes and a
   > `RegexRouter` to merge their output into `orderbook.cdc.public.*`.

2. **Shard names are normalised.** With
   `citus.enable_change_data_capture = on` (set on every node, see
   `docker-compose.yml`), Citus' pgoutput/wal2json support reports changes
   to `outbox_102041` as changes to `public.outbox`, and suppresses the
   internal writes that shard moves and splits generate. Without it you
   would see shard tables and duplicates during rebalancing.

3. **Publications are created on the coordinator and propagated** to the
   workers by Citus (`CREATE PUBLICATION` in a migration). Never create
   publications `FOR ALL TABLES`: that includes Citus' metadata tables.

4. **Ordering is per node.** Changes from one worker arrive in commit order.
   Changes from different workers are interleaved arbitrarily. Because every
   table is distributed by tenant, all of a tenant's events come from one
   node, so **per-tenant (and per-order) order is preserved**, as long as
   the tenant's shard does not move. A shard move transfers the tenant to
   another worker's stream; events around the cut-over can then arrive out
   of order across the two connectors. Design consumers for it (below).

Citus labels CDC for distributed tables a preview feature (since 11.3), and
`alter_distributed_table` breaks the stream. Test upgrades of Citus and
Debezium together in staging.

## Delivery guarantees and ordering

| Stage | Guarantee |
|---|---|
| Commit → outbox row | Atomic with the business change |
| WAL → Kafka (Debezium) | At least once. After a crash or restart Debezium resumes from its last committed offset and may re-publish some events |
| Kafka → consumer | At least once. Offsets are committed after processing |
| Consumer effect | Exactly once, by deduplicating on the event `id` inside the same transaction as the effect |

So **every consumer must be idempotent.** The sales projector:

```go
p.tx.WithTx(ctx, func(ctx context.Context) error {
    isNew, err := store.MarkProcessed(ctx, tenantID, eventID) // INSERT ... ON CONFLICT DO NOTHING
    if err != nil || !isNew { return err }                    // duplicate → skip
    return store.Apply(ctx, delta)                            // commutative upsert
})
```

`processed_events` and `tenant_daily_sales` are co-located with the tenant,
so that transaction is single-shard too. Ids are kept for
`OUTBOX_DEDUP_RETENTION` (7 days), which must exceed the longest possible
redelivery delay.

Ordering:

- Kafka orders messages per partition. The key is the order id, so all
  events of one order go to one partition, in commit order, while they come
  from one worker.
- The projection uses **commutative** deltas (counters), so it is correct in
  any order. When a consumer needs "latest state wins", compare a version
  (`orders.version`, `__source_lsn`, `updated_at`) and ignore older ones.
  Don't assume arrival order.

## The consumer (`api worker`)

`internal/platform/kafkax` (franz-go) plus `internal/app/worker.go`:

- consumer group `KAFKA_GROUP_ID`. Run up to *partitions* replicas
  (12 here); partitions are spread over them;
- `BlockRebalanceOnPoll`: a partition is never revoked while one of its
  records is being processed, so two replicas never process the same record
  at the same time;
- records are processed in order per partition, with retries and
  exponential backoff (`KAFKA_MAX_ATTEMPTS`). Errors wrapped with
  `kafkax.Permanent` (undecodable payload, missing headers, tenant
  mismatch) skip the retries;
- messages that still fail go to the **dead-letter topic**
  (`orderbook.events.order.dlq`) with `dlq.error`, `dlq.attempts` and
  original topic/partition/offset headers, so one poison message cannot
  block a partition. If the DLQ itself is unavailable, the consumer keeps
  retrying rather than dropping the message;
- offsets are committed after each batch, and also on shutdown;
- each message gets a consumer span that continues the producing request's
  trace (from the `traceparent` header);
- metrics: `kafka_consumer_messages_total{outcome=processed|retried|dead_lettered}`,
  `kafka_consumer_handle_duration_seconds`; readiness checks Postgres and
  Kafka;
- housekeeping: prunes the outbox and old processed-event ids.

Replaying the DLQ: inspect with `make events topic=orderbook.events.order.dlq`,
fix the cause, then re-publish the messages to the main topic (for example
with `kcat`). The projector deduplicates, so replaying events that did
succeed is harmless. Rebuilding a projection from scratch: truncate it and
its `processed_events` rows, then reset the group's offsets to the earliest
offset (`kafka-consumer-groups.sh --reset-offsets --to-earliest`). This needs
topic retention covering the history you want to rebuild, or a backfill from
the source tables.

## Running it locally

```bash
make up            # Citus (coordinator + 2 workers), Kafka (KRaft), Kafka Connect + Debezium, ...
make run           # API on :8080 (migrates: creates the outbox and publications)
make cdc-register  # one outbox connector per worker
make run-worker    # consumer, admin on :9092
```

or everything in containers: `make up-app`. Then:

```bash
TOKEN=$(make -s token client=acme)
PRODUCT=$(curl -s localhost:8080/v1/products -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"sku":"W-1","name":"Widget","price_minor":1250,"currency":"USD"}' | jq -r .id)
curl -s localhost:8080/v1/orders -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"customer_ref\":\"c-1\",\"lines\":[{\"product_id\":\"$PRODUCT\",\"quantity\":2}]}"

make events                                  # the order.placed event, with headers
curl -s localhost:8080/v1/sales/daily -H "Authorization: Bearer $TOKEN"   # projection, ~1s later
make cdc-status                              # connectors + slot lag per worker
make kafka-topics                            # topics + consumer lag
make e2e                                     # automated end-to-end check
```

The Docker VM needs about 4 GB of memory for the whole stack (Kafka and
Kafka Connect are JVMs; heaps are capped in `docker-compose.yml`).

## Operations

### Replication slot lag: the one metric you must alert on

A replication slot makes PostgreSQL keep WAL until the consumer confirms it.
**If a connector stops, WAL accumulates on that worker until the disk
fills**, and the database stops accepting writes. So:

- alert on `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)` per
  slot on every worker (`make cdc-status` shows it;
  [postgres_exporter](https://github.com/prometheus-community/postgres_exporter)
  exposes `pg_replication_slots_*`), and on connector task state
  (Kafka Connect REST API or JMX);
- set `max_slot_wal_keep_size` (done in compose: 2GB) so an abandoned slot
  is invalidated instead of taking the node down. An invalidated slot means
  lost changes, and the connector must be re-created and the gap backfilled;
- when you remove a connector for good, drop its slot on every worker:
  `SELECT pg_drop_replication_slot('debezium_outbox');`.

Heartbeats (`heartbeat.interval.ms`) let Debezium confirm progress even when
the captured tables are idle, so a quiet tenant mix on one worker does not
hold WAL back.

### Adding a worker

Order matters: the slot must exist before any shard (and its writes) lands
on the new node.

1. `SELECT citus_add_node('worker3', 5432);`. The node joins, but holds no
   shards yet.
2. Make sure `wal_level=logical` and `citus.enable_change_data_capture=on`
   are set on it, and that the publications exist there (Citus propagates
   them when the node joins; verify with `\dRp` on the worker).
3. Add `deploy/debezium/outbox-worker3.json` (and `tables-worker3.json`)
   and run `make cdc-register`. Wait for the connector to be RUNNING and its
   slot to be active.
4. Rebalance: `make citus-rebalance`.

Removing a worker is the reverse: drain it (`citus_drain_node`), wait until
its connector has published everything (slot lag ≈ 0), then delete the
connector, drop the slot and remove the node.

### Failover of a worker

Logical replication slots exist only on the primary on PostgreSQL ≤ 16.
After a failover the new primary has no slot, and the changes committed
between the last confirmed position and the failover can be lost to CDC.
PostgreSQL 17 can synchronise failover slots to standbys
(`failover = true` slots plus `sync_replication_slots = on` on the standby).
Debezium 3.x supports creating failover slots (`slot.failover=true`). Use
both in production, or plan reconciliation (re-publish outbox rows
newer than the failover point: they are still in the table thanks to
`OUTBOX_RETENTION`).

### Schema changes

Adding nullable columns to captured tables is transparent. Renaming or
dropping columns of `orders` / `order_lines` changes the row-level CDC
output, so treat those topics as a contract with downstream data teams too.
The outbox decouples domain events from the table schema, which is one more
reason to prefer it for service-to-service integration.

## Production checklist

- [ ] Dedicated replication user with `REPLICATION` and `SELECT` on the
      captured tables (not the owner/superuser used here), credentials from
      a secret store (Kafka Connect config providers).
- [ ] Every Citus node: `wal_level=logical`,
      `citus.enable_change_data_capture=on`, `max_replication_slots` /
      `max_wal_senders` sized for your connectors, `max_slot_wal_keep_size`.
- [ ] One connector per shard-holding node, unique `topic.prefix` each.
- [ ] Topics pre-created (partitions, `min.insync.replicas=2`, replication
      factor 3, retention ≥ longest consumer outage you want to absorb).
- [ ] Kafka Connect in distributed mode with ≥ 2 workers. Its internal
      topics need replication factor 3.
- [ ] Alerts: slot lag per node, connector/task state, consumer group lag
      (e.g. kminion / Burrow / your platform), DLQ message rate.
- [ ] Failover slots (PostgreSQL 17 + `slot.failover`) or a reconciliation
      runbook.
- [ ] Consumers idempotent and order-tolerant. DLQ inspection and replay
      runbook.
- [ ] Event contract documented and versioned (consider a schema registry
      with JSON Schema/Avro/Protobuf once more teams consume the events).
