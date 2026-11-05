package order

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/google/uuid"

	"github.com/Oxyrus/orderbook/internal/catalog"
	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/outbox"
)

// Service contains the order business logic and owns transaction
// boundaries: an order change and its event commit atomically.
type Service struct {
	repo     Repository
	products Products
	events   Events
	tenants  TenantProvisioner
	tx       database.Transactor
}

// NewService wires a Service.
func NewService(repo Repository, products Products, events Events, tenants TenantProvisioner, tx database.Transactor) *Service {
	return &Service{repo: repo, products: products, events: events, tenants: tenants, tx: tx}
}

// LineInput is a requested order line.
type LineInput struct {
	ProductID uuid.UUID
	Quantity  int
}

// PlaceInput is a new order.
type PlaceInput struct {
	CustomerRef string
	Lines       []LineInput
}

// Place prices and stores a new order and emits order.placed.
func (s *Service) Place(ctx context.Context, tenantID string, in PlaceInput) (*Order, error) {
	if len(in.Lines) == 0 || len(in.Lines) > MaxLines {
		return nil, ErrInvalidLineRequest
	}
	ids := make([]uuid.UUID, 0, len(in.Lines))
	for _, l := range in.Lines {
		if l.Quantity < 1 || l.Quantity > MaxQuantity {
			return nil, ErrInvalidLineRequest
		}
		if slices.Contains(ids, l.ProductID) {
			return nil, &ProductError{Err: ErrDuplicateProduct, ProductID: l.ProductID}
		}
		ids = append(ids, l.ProductID)
	}
	if err := s.tenants.Ensure(ctx, tenantID); err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate id: %w", err)
	}

	var o *Order
	err = s.tx.WithTx(ctx, func(ctx context.Context) error {
		products, err := s.products.LockForOrder(ctx, tenantID, ids)
		if err != nil {
			return err
		}
		o, err = price(tenantID, id, in, products)
		if err != nil {
			return err
		}
		if err := s.repo.Create(ctx, o); err != nil {
			return err
		}
		_, err = s.events.Add(ctx, outbox.Event{
			TenantID:      tenantID,
			AggregateType: AggregateType,
			AggregateID:   o.ID.String(),
			Type:          EventOrderPlaced,
			Payload:       placedEvent(o),
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return o, nil
}

// price builds the order from the locked catalog rows.
func price(tenantID string, id uuid.UUID, in PlaceInput, products []catalog.Product) (*Order, error) {
	byID := make(map[uuid.UUID]catalog.Product, len(products))
	for _, p := range products {
		byID[p.ID] = p
	}
	o := &Order{TenantID: tenantID, ID: id, CustomerRef: in.CustomerRef, Status: StatusPlaced}
	for i, l := range in.Lines {
		p, ok := byID[l.ProductID]
		switch {
		case !ok:
			return nil, &ProductError{Err: ErrUnknownProduct, ProductID: l.ProductID}
		case !p.Active:
			return nil, &ProductError{Err: ErrInactiveProduct, ProductID: l.ProductID}
		case o.Currency == "":
			o.Currency = p.Currency
		case o.Currency != p.Currency:
			return nil, &ProductError{Err: ErrMixedCurrencies, ProductID: l.ProductID}
		}
		if p.PriceMinor > 0 && int64(l.Quantity) > (math.MaxInt64-o.TotalMinor)/p.PriceMinor {
			return nil, ErrTotalOutOfRange
		}
		o.TotalMinor += int64(l.Quantity) * p.PriceMinor
		o.Lines = append(o.Lines, Line{
			LineNo: i + 1, ProductID: p.ID, SKU: p.SKU, Name: p.Name,
			Quantity: l.Quantity, UnitPriceMinor: p.PriceMinor,
		})
	}
	return o, nil
}

// Get returns one order with its lines.
func (s *Service) Get(ctx context.Context, tenantID string, id uuid.UUID) (*Order, error) {
	return s.repo.Get(ctx, tenantID, id)
}

// List returns a page of the tenant's orders, newest first.
func (s *Service) List(ctx context.Context, p ListParams) (Page, error) {
	if p.Limit <= 0 {
		p.Limit = DefaultPageSize
	}
	p.Limit = min(p.Limit, MaxPageSize)
	return s.repo.List(ctx, p)
}

// Cancel cancels a placed order and emits order.cancelled. When
// expectedVersion is non-zero (If-Match) the order must still be at that
// version.
func (s *Service) Cancel(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int) (*Order, error) {
	var o *Order
	err := s.tx.WithTx(ctx, func(ctx context.Context) error {
		var err error
		o, err = s.repo.Cancel(ctx, tenantID, id, expectedVersion)
		if err != nil {
			return err
		}
		_, err = s.events.Add(ctx, outbox.Event{
			TenantID:      tenantID,
			AggregateType: AggregateType,
			AggregateID:   o.ID.String(),
			Type:          EventOrderCanceled,
			Payload: CancelledEvent{
				OrderID: o.ID, TenantID: o.TenantID, Currency: o.Currency, TotalMinor: o.TotalMinor,
				PlacedAt: o.PlacedAt.UTC(), CancelledAt: o.CancelledAt.UTC(),
			},
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return o, nil
}

func placedEvent(o *Order) PlacedEvent {
	ev := PlacedEvent{
		OrderID: o.ID, TenantID: o.TenantID, CustomerRef: o.CustomerRef, Currency: o.Currency,
		TotalMinor: o.TotalMinor, PlacedAt: o.PlacedAt.UTC(), Lines: make([]PlacedEventLine, 0, len(o.Lines)),
	}
	for _, l := range o.Lines {
		ev.Lines = append(ev.Lines, PlacedEventLine{ProductID: l.ProductID, SKU: l.SKU, Quantity: l.Quantity, UnitPriceMinor: l.UnitPriceMinor})
	}
	return ev
}
