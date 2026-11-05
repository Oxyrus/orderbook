package sales

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/Oxyrus/orderbook/internal/order"
	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/kafkax"
)

// Projector applies order events to the read model. Its Handle method is a
// kafkax.Handler.
type Projector struct {
	store Store
	tx    database.Transactor
}

// NewProjector returns a Projector.
func NewProjector(store Store, tx database.Transactor) *Projector {
	return &Projector{store: store, tx: tx}
}

// Handle applies one event exactly once: the event id is recorded in
// processed_events in the same single-shard transaction as the projection
// update, so a redelivered event is detected and skipped.
func (p *Projector) Handle(ctx context.Context, m kafkax.Message) error {
	eventID, err := uuid.Parse(m.Header("id"))
	if err != nil {
		return kafkax.Permanent(fmt.Errorf("missing or invalid id header: %w", err))
	}
	tenantID := m.Header("tenant_id")
	if tenantID == "" {
		return kafkax.Permanent(errors.New("missing tenant_id header"))
	}

	delta, ok, err := toDelta(m.Header("event_type"), m.Value)
	if err != nil {
		return kafkax.Permanent(err)
	}
	if !ok {
		zerolog.Ctx(ctx).Debug().Msg("ignoring event type")
		return nil
	}
	if delta.TenantID != tenantID {
		return kafkax.Permanent(fmt.Errorf("payload tenant %q does not match header tenant %q", delta.TenantID, tenantID))
	}

	return p.tx.WithTx(ctx, func(ctx context.Context) error {
		isNew, err := p.store.MarkProcessed(ctx, tenantID, eventID)
		if err != nil || !isNew {
			if err == nil {
				zerolog.Ctx(ctx).Debug().Msg("duplicate event skipped")
			}
			return err
		}
		return p.store.Apply(ctx, delta)
	})
}

// toDelta decodes an event. ok is false for event types the projection does
// not care about (forward compatibility: producers may add new types).
func toDelta(eventType string, payload []byte) (Delta, bool, error) {
	switch eventType {
	case order.EventOrderPlaced:
		var e order.PlacedEvent
		if err := json.Unmarshal(payload, &e); err != nil {
			return Delta{}, false, fmt.Errorf("decode %s: %w", eventType, err)
		}
		if e.TenantID == "" || e.Currency == "" || e.PlacedAt.IsZero() {
			return Delta{}, false, fmt.Errorf("incomplete %s payload", eventType)
		}
		return Delta{TenantID: e.TenantID, Day: day(e.PlacedAt), Currency: e.Currency,
			OrdersPlaced: 1, GrossMinor: e.TotalMinor}, true, nil
	case order.EventOrderCanceled:
		var e order.CancelledEvent
		if err := json.Unmarshal(payload, &e); err != nil {
			return Delta{}, false, fmt.Errorf("decode %s: %w", eventType, err)
		}
		if e.TenantID == "" || e.Currency == "" || e.PlacedAt.IsZero() {
			return Delta{}, false, fmt.Errorf("incomplete %s payload", eventType)
		}
		// Attributed to the day the order was placed, so a day's net
		// revenue reflects what was eventually kept.
		return Delta{TenantID: e.TenantID, Day: day(e.PlacedAt), Currency: e.Currency,
			OrdersCancelled: 1, CancelledMinor: e.TotalMinor}, true, nil
	default:
		return Delta{}, false, nil
	}
}

func day(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}
