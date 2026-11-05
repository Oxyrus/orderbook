package order_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/catalog"
	"github.com/Oxyrus/orderbook/internal/order"
	"github.com/Oxyrus/orderbook/internal/platform/outbox"
)

type fakeRepo struct{ created []*order.Order }

func (f *fakeRepo) Create(_ context.Context, o *order.Order) error {
	o.PlacedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o.Version = 1
	f.created = append(f.created, o)
	return nil
}
func (f *fakeRepo) Get(context.Context, string, uuid.UUID) (*order.Order, error) {
	return nil, order.ErrNotFound
}
func (f *fakeRepo) List(context.Context, order.ListParams) (order.Page, error) {
	return order.Page{}, nil
}
func (f *fakeRepo) Cancel(context.Context, string, uuid.UUID, int) (*order.Order, error) {
	return nil, order.ErrNotFound
}

type fakeProducts []catalog.Product

func (f fakeProducts) LockForOrder(_ context.Context, tenantID string, ids []uuid.UUID) ([]catalog.Product, error) {
	var out []catalog.Product
	for _, p := range f {
		for _, id := range ids {
			if p.ID == id && p.TenantID == tenantID {
				out = append(out, p)
			}
		}
	}
	return out, nil
}

type fakeEvents struct{ events []outbox.Event }

func (f *fakeEvents) Add(_ context.Context, ev ...outbox.Event) ([]uuid.UUID, error) {
	f.events = append(f.events, ev...)
	return nil, nil
}

type fakeTenants struct{ ensured []string }

func (f *fakeTenants) Ensure(_ context.Context, id string) error {
	f.ensured = append(f.ensured, id)
	return nil
}

type fakeTx struct{ n int }

func (f *fakeTx) WithTx(ctx context.Context, fn func(context.Context) error) error {
	f.n++
	return fn(ctx)
}

func TestPlacePricesAndEmits(t *testing.T) {
	a := catalog.Product{TenantID: "acme", ID: uuid.New(), SKU: "A", Name: "Apple", PriceMinor: 150, Currency: "EUR", Active: true}
	b := catalog.Product{TenantID: "acme", ID: uuid.New(), SKU: "B", Name: "Banana", PriceMinor: 99, Currency: "EUR", Active: true}
	repo, events, tenants, tx := &fakeRepo{}, &fakeEvents{}, &fakeTenants{}, &fakeTx{}
	svc := order.NewService(repo, fakeProducts{a, b}, events, tenants, tx)

	o, err := svc.Place(context.Background(), "acme", order.PlaceInput{CustomerRef: "c", Lines: []order.LineInput{
		{ProductID: a.ID, Quantity: 2}, {ProductID: b.ID, Quantity: 3},
	}})
	require.NoError(t, err)
	assert.EqualValues(t, 2*150+3*99, o.TotalMinor)
	assert.Equal(t, "EUR", o.Currency)
	assert.Equal(t, []string{"acme"}, tenants.ensured)
	assert.Equal(t, 1, tx.n)

	require.Len(t, events.events, 1)
	ev := events.events[0]
	assert.Equal(t, "acme", ev.TenantID)
	assert.Equal(t, order.AggregateType, ev.AggregateType)
	assert.Equal(t, order.EventOrderPlaced, ev.Type)
	assert.Equal(t, o.ID.String(), ev.AggregateID)
	payload := ev.Payload.(order.PlacedEvent)
	assert.Len(t, payload.Lines, 2)
	assert.False(t, payload.PlacedAt.IsZero(), "placed_at comes from the stored row")
}

func TestPlaceRejectsBadInput(t *testing.T) {
	p := catalog.Product{TenantID: "acme", ID: uuid.New(), PriceMinor: math.MaxInt64 / 2, Currency: "USD", Active: true}
	svc := order.NewService(&fakeRepo{}, fakeProducts{p}, &fakeEvents{}, &fakeTenants{}, &fakeTx{})
	ctx := context.Background()

	_, err := svc.Place(ctx, "acme", order.PlaceInput{CustomerRef: "c"})
	assert.ErrorIs(t, err, order.ErrInvalidLineRequest)
	_, err = svc.Place(ctx, "acme", order.PlaceInput{CustomerRef: "c", Lines: []order.LineInput{{ProductID: p.ID, Quantity: 0}}})
	assert.ErrorIs(t, err, order.ErrInvalidLineRequest)
	_, err = svc.Place(ctx, "acme", order.PlaceInput{CustomerRef: "c", Lines: []order.LineInput{{ProductID: p.ID, Quantity: 3}}})
	assert.ErrorIs(t, err, order.ErrTotalOutOfRange)
	_, err = svc.Place(ctx, "globex", order.PlaceInput{CustomerRef: "c", Lines: []order.LineInput{{ProductID: p.ID, Quantity: 1}}})
	assert.ErrorIs(t, err, order.ErrUnknownProduct)
}
