// Package order places and cancels orders.
//
// Placing an order prices it against the tenant's catalog, stores the order
// and its lines, and records an order.placed event in the outbox, all in
// one transaction. Every table involved is distributed by tenant_id and
// co-located, so that transaction runs on a single Citus shard. Debezium
// then publishes the event to Kafka (see docs/cdc.md).
package order

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/Oxyrus/orderbook/internal/catalog"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
	"github.com/Oxyrus/orderbook/internal/platform/outbox"
)

// Statuses.
const (
	StatusPlaced    = "placed"
	StatusCancelled = "cancelled"
)

// Limits.
const (
	MaxLines        = 100
	MaxQuantity     = 10_000
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// Domain errors.
var (
	ErrNotFound           = errors.New("order not found")
	ErrVersionConflict    = errors.New("order was modified concurrently")
	ErrAlreadyCancelled   = errors.New("order is already cancelled")
	ErrUnknownProduct     = errors.New("unknown product")
	ErrInactiveProduct    = errors.New("product is not active")
	ErrMixedCurrencies    = errors.New("products are priced in different currencies")
	ErrDuplicateProduct   = errors.New("product appears in more than one line")
	ErrTotalOutOfRange    = errors.New("order total is out of range")
	ErrInvalidLineRequest = errors.New("invalid order lines")
)

// ProductError carries the product that made an order invalid.
type ProductError struct {
	Err       error
	ProductID uuid.UUID
}

func (e *ProductError) Error() string { return e.Err.Error() + ": " + e.ProductID.String() }
func (e *ProductError) Unwrap() error { return e.Err }

// Order is the aggregate root.
type Order struct {
	TenantID    string
	ID          uuid.UUID
	CustomerRef string
	Status      string
	Currency    string
	TotalMinor  int64
	Version     int
	PlacedAt    time.Time
	CancelledAt *time.Time
	UpdatedAt   time.Time
	Lines       []Line
}

// Line is one product in an order. Name, SKU and price are copied from the
// catalog when the order is placed, so later catalog edits don't rewrite
// history.
type Line struct {
	LineNo         int
	ProductID      uuid.UUID
	SKU            string
	Name           string
	Quantity       int
	UnitPriceMinor int64
}

// ListParams filters and paginates listings.
type ListParams struct {
	TenantID string
	Status   string // optional
	Limit    int
	After    *httpx.Cursor
}

// Page is a page of orders (without lines).
type Page struct {
	Orders     []Order
	NextCursor *httpx.Cursor
}

// Repository persists orders.
type Repository interface {
	// Create inserts the order and its lines.
	Create(ctx context.Context, o *Order) error
	// Get returns the order with its lines.
	Get(ctx context.Context, tenantID string, id uuid.UUID) (*Order, error)
	List(ctx context.Context, p ListParams) (Page, error)
	// Cancel moves a placed order to cancelled if its version matches
	// (expectedVersion 0 skips the check) and returns the updated order.
	Cancel(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int) (*Order, error)
}

// Products prices and locks the products of a new order.
type Products interface {
	LockForOrder(ctx context.Context, tenantID string, ids []uuid.UUID) ([]catalog.Product, error)
}

// Events appends to the transactional outbox.
type Events interface {
	Add(ctx context.Context, events ...outbox.Event) ([]uuid.UUID, error)
}

// TenantProvisioner creates the tenant row on first use.
type TenantProvisioner interface {
	Ensure(ctx context.Context, tenantID string) error
}
