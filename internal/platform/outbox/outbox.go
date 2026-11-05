// Package outbox implements the transactional outbox pattern on top of
// Debezium.
//
// A service that changes state and must tell the world about it writes the
// event into the outbox table in the same database transaction as the
// change. Debezium tails the WAL and publishes every committed outbox row to
// Kafka. The event is published if and only if the change commits, without
// two-phase commit and without the service talking to Kafka at all. That
// keeps the request path fast and available even while Kafka is down.
//
// With Citus the outbox is distributed by tenant_id and co-located with the
// tenant's data, so adding the event keeps the transaction on one shard.
// See docs/cdc.md for the Debezium side.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/propagation"

	"github.com/Oxyrus/orderbook/internal/platform/database"
)

// Event is a domain event to publish.
type Event struct {
	// TenantID is the shard key; it must be the tenant whose data the
	// surrounding transaction changes. It reaches Kafka as the tenant_id
	// header.
	TenantID string
	// AggregateType selects the Kafka topic: orderbook.events.<AggregateType>.
	AggregateType string
	// AggregateID becomes the Kafka message key, so all events of one
	// aggregate land on the same partition, in commit order.
	AggregateID string
	// Type is the event name, e.g. "order.placed" (event_type header).
	Type string
	// Payload is marshalled to JSON and becomes the Kafka message value.
	Payload any
}

// Writer appends events to the outbox. Services depend on it through their
// own narrow interface so they can be unit-tested with a fake.
type Writer struct {
	db *database.DB
}

// NewWriter returns a Writer backed by db.
func NewWriter(db *database.DB) *Writer { return &Writer{db: db} }

// ErrNoTransaction is returned when Add is called outside a transaction:
// an outbox write that is not atomic with the state change it describes
// defeats the purpose of the pattern.
var ErrNoTransaction = errors.New("outbox: Add must run inside database.WithTx")

// Add inserts the events into the outbox, inside the transaction carried by
// ctx. It returns the generated event ids (the id header consumers use for
// deduplication).
func (w *Writer) Add(ctx context.Context, events ...Event) ([]uuid.UUID, error) {
	if !database.InTx(ctx) {
		return nil, ErrNoTransaction
	}
	traceparent := traceParent(ctx)
	ids := make([]uuid.UUID, 0, len(events))
	for _, e := range events {
		if e.TenantID == "" || e.AggregateType == "" || e.AggregateID == "" || e.Type == "" {
			return nil, fmt.Errorf("outbox: incomplete event %+v", e)
		}
		payload, err := json.Marshal(e.Payload)
		if err != nil {
			return nil, fmt.Errorf("outbox: marshal %s payload: %w", e.Type, err)
		}
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("outbox: generate id: %w", err)
		}
		const q = `
			INSERT INTO outbox (tenant_id, id, aggregatetype, aggregateid, type, payload, traceparent)
			VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''))`
		if _, err := w.db.Q(ctx).Exec(ctx, q, e.TenantID, id, e.AggregateType, e.AggregateID, e.Type, payload, traceparent); err != nil {
			return nil, fmt.Errorf("outbox: insert %s: %w", e.Type, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// traceParent returns the W3C traceparent of the span in ctx ("" without an
// active, sampled-or-not trace).
func traceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Prune deletes outbox rows older than retention and returns how many were
// removed. Debezium has published them long before (it reads the WAL, not
// the table), and the publication only carries inserts, so deleting emits
// no events. This is a multi-shard statement: Citus runs it on every shard
// in parallel.
func Prune(ctx context.Context, db *database.DB, retention time.Duration) (int64, error) {
	tag, err := db.Pool.Exec(ctx, `DELETE FROM outbox WHERE created_at < now() - make_interval(secs => $1)`, retention.Seconds())
	if err != nil {
		return 0, fmt.Errorf("outbox: prune: %w", err)
	}
	return tag.RowsAffected(), nil
}
