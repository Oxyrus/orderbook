package order

import (
	"time"

	"github.com/google/uuid"
)

// Event contract. These payloads are published to the Kafka topic
// orderbook.events.order (outbox aggregatetype "order"); the Kafka message
// key is the order id and the headers carry id (event id, for
// deduplication), event_type, tenant_id and traceparent.
//
// Treat the payloads as a public API: add fields freely, but never remove,
// rename or change the meaning of one. Breaking changes need a new event
// type (order.placed.v2).
const (
	AggregateType      = "order"
	EventOrderPlaced   = "order.placed"
	EventOrderCanceled = "order.cancelled"
)

// PlacedEvent is the payload of order.placed.
type PlacedEvent struct {
	OrderID     uuid.UUID         `json:"order_id"`
	TenantID    string            `json:"tenant_id"`
	CustomerRef string            `json:"customer_ref"`
	Currency    string            `json:"currency"`
	TotalMinor  int64             `json:"total_minor"`
	PlacedAt    time.Time         `json:"placed_at"`
	Lines       []PlacedEventLine `json:"lines"`
}

// PlacedEventLine is one line of PlacedEvent.
type PlacedEventLine struct {
	ProductID      uuid.UUID `json:"product_id"`
	SKU            string    `json:"sku"`
	Quantity       int       `json:"quantity"`
	UnitPriceMinor int64     `json:"unit_price_minor"`
}

// CancelledEvent is the payload of order.cancelled.
type CancelledEvent struct {
	OrderID     uuid.UUID `json:"order_id"`
	TenantID    string    `json:"tenant_id"`
	Currency    string    `json:"currency"`
	TotalMinor  int64     `json:"total_minor"`
	PlacedAt    time.Time `json:"placed_at"`
	CancelledAt time.Time `json:"cancelled_at"`
}
