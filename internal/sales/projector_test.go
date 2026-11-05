package sales

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/order"
	"github.com/Oxyrus/orderbook/internal/platform/kafkax"
)

type memStore struct {
	seen   map[uuid.UUID]bool
	deltas []Delta
}

func (m *memStore) MarkProcessed(_ context.Context, _ string, id uuid.UUID) (bool, error) {
	if m.seen[id] {
		return false, nil
	}
	m.seen[id] = true
	return true, nil
}
func (m *memStore) Apply(_ context.Context, d Delta) error {
	m.deltas = append(m.deltas, d)
	return nil
}
func (m *memStore) Daily(context.Context, string, time.Time, time.Time) ([]DailySales, error) {
	return nil, nil
}
func (m *memStore) TopTenants(context.Context, time.Time, time.Time, string, int) ([]TenantTotal, error) {
	return nil, nil
}

type noTx struct{}

func (noTx) WithTx(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

func msg(t *testing.T, eventType, tenantID string, payload any) kafkax.Message {
	t.Helper()
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	return kafkax.Message{Value: b, Headers: map[string]string{"id": uuid.NewString(), "tenant_id": tenantID, "event_type": eventType}}
}

func TestProjector(t *testing.T) {
	store := &memStore{seen: map[uuid.UUID]bool{}}
	p := NewProjector(store, noTx{})
	ctx := context.Background()
	placedAt := time.Date(2026, 5, 1, 23, 30, 0, 0, time.FixedZone("UTC-5", -5*3600)) // 2026-05-02 UTC

	cancelled := msg(t, order.EventOrderCanceled, "acme", order.CancelledEvent{
		OrderID: uuid.New(), TenantID: "acme", Currency: "USD", TotalMinor: 700, PlacedAt: placedAt, CancelledAt: placedAt.Add(time.Hour),
	})
	require.NoError(t, p.Handle(ctx, cancelled))
	require.NoError(t, p.Handle(ctx, cancelled), "duplicate is skipped, not an error")
	require.Len(t, store.deltas, 1)
	assert.Equal(t, Delta{TenantID: "acme", Day: time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC), Currency: "USD",
		OrdersCancelled: 1, CancelledMinor: 700}, store.deltas[0], "attributed to the UTC day the order was placed")

	require.NoError(t, p.Handle(ctx, msg(t, "order.shipped", "acme", map[string]any{})), "unknown types are ignored")
	assert.Len(t, store.deltas, 1)

	permanent := []kafkax.Message{
		{Value: []byte(`{}`), Headers: map[string]string{"tenant_id": "acme", "event_type": order.EventOrderPlaced}},
		{Value: []byte(`{}`), Headers: map[string]string{"id": uuid.NewString(), "event_type": order.EventOrderPlaced}},
		{Value: []byte(`{nope`), Headers: map[string]string{"id": uuid.NewString(), "tenant_id": "acme", "event_type": order.EventOrderPlaced}},
		msg(t, order.EventOrderPlaced, "acme", order.PlacedEvent{TenantID: "acme"}), // incomplete
		msg(t, order.EventOrderPlaced, "acme", order.PlacedEvent{TenantID: "globex", Currency: "USD", PlacedAt: placedAt}),
	}
	for i, m := range permanent {
		err := p.Handle(ctx, m)
		assert.True(t, kafkax.IsPermanent(err), "case %d: %v", i, err)
	}
}

func TestDateRange(t *testing.T) {
	h := NewHandler(&memStore{})
	h.now = func() time.Time { return time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC) }
	parse := func(q string) (string, string, bool) {
		r := httptest.NewRequest(http.MethodGet, "/sales/daily"+q, nil)
		from, to, err := h.dateRange(r)
		return from.Format(time.DateOnly), to.Format(time.DateOnly), err == nil
	}
	from, to, ok := parse("")
	assert.True(t, ok)
	assert.Equal(t, [2]string{"2026-06-01", "2026-06-30"}, [2]string{from, to})
	from, _, ok = parse("?to=2026-02-10")
	assert.True(t, ok)
	assert.Equal(t, "2026-01-12", from)
	for _, q := range []string{"?from=x", "?from=2026-07-01&to=2026-06-01", "?from=2024-01-01&to=2026-01-01"} {
		_, _, ok := parse(q)
		assert.False(t, ok, q)
	}
}
