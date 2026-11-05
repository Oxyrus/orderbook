-- +goose Up
-- Read model maintained by `api worker` from the order events Debezium
-- publishes (CQRS). Co-located with the tenant, so the consumer updates it
-- with single-shard transactions.
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

CREATE TABLE tenant_daily_sales (
    tenant_id        text    NOT NULL,
    day              date    NOT NULL,
    currency         char(3) NOT NULL,
    orders_placed    bigint  NOT NULL DEFAULT 0,
    orders_cancelled bigint  NOT NULL DEFAULT 0,
    gross_minor      bigint  NOT NULL DEFAULT 0,
    cancelled_minor  bigint  NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, day, currency)
);
SELECT create_distributed_table('tenant_daily_sales', 'tenant_id', colocate_with => 'tenants');

-- Kafka and Debezium deliver at least once. The consumer records every event
-- id it applied, in the same transaction as the projection update, and skips
-- ids it has already seen.
CREATE TABLE processed_events (
    tenant_id    text        NOT NULL,
    event_id     uuid        NOT NULL,
    consumer     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, consumer, event_id)
);
SELECT create_distributed_table('processed_events', 'tenant_id', colocate_with => 'tenants');

CREATE INDEX processed_events_processed_idx ON processed_events (processed_at);

-- +goose Down
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS tenant_daily_sales;
