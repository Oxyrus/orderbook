-- +goose Up
-- Transactional outbox. Services insert an event row in the same
-- transaction as the state change it describes; Debezium tails the WAL and
-- publishes each row to Kafka (see deploy/debezium and docs/cdc.md). No
-- dual writes, no lost or phantom events.
--
-- The outbox is co-located with the tenant's data, so writing it never turns
-- a single-shard transaction into a distributed (2PC) one. Its rows are
-- therefore spread over every worker, and Debezium runs one connector per
-- worker. Citus' change data capture support
-- (citus.enable_change_data_capture) makes events from shard outbox_102xxx
-- appear as "public.outbox".
--
-- Column names follow Debezium's outbox event router defaults (id,
-- aggregatetype, aggregateid, type, payload), which keeps the connector
-- configuration minimal.
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

CREATE TABLE outbox (
    tenant_id     text        NOT NULL,
    id            uuid        NOT NULL,
    aggregatetype text        NOT NULL,
    aggregateid   text        NOT NULL,
    type          text        NOT NULL,
    payload       jsonb       NOT NULL,
    -- W3C trace context of the request that produced the event, forwarded to
    -- consumers as a Kafka header so traces continue across the hop.
    traceparent   text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);
SELECT create_distributed_table('outbox', 'tenant_id', colocate_with => 'tenants');

-- For the periodic prune of rows Debezium has already published.
CREATE INDEX outbox_created_idx ON outbox (created_at);

-- Debezium only needs inserts. Publishing nothing else means pruning the
-- table never emits delete events. Created on the coordinator, Citus
-- propagates the publication to every worker.
CREATE PUBLICATION orderbook_outbox FOR TABLE outbox WITH (publish = 'insert');

-- +goose Down
DROP PUBLICATION IF EXISTS orderbook_outbox;
DROP TABLE IF EXISTS outbox;
