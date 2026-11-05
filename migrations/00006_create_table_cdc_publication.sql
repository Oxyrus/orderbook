-- +goose Up
-- Row-level CDC of the orders tables for downstream systems that want the
-- current state of every order rather than domain events: a data warehouse
-- or lakehouse (ClickHouse, BigQuery, Snowflake, Iceberg), a search index
-- (OpenSearch) or a cache. deploy/debezium/tables-*.json streams it to
-- Kafka topics orderbook.cdc.public.<table>.
--
-- A publication by itself costs nothing: WAL is only retained once a
-- connector creates a replication slot for it.
CREATE PUBLICATION orderbook_tables FOR TABLE orders, order_lines;

-- +goose Down
DROP PUBLICATION IF EXISTS orderbook_tables;
