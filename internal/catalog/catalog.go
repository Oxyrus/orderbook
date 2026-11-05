// Package catalog manages each tenant's products.
//
//	catalog.go     domain model, errors and ports
//	repository.go  PostgreSQL/Citus persistence
//	service.go     business logic
//	handler.go     HTTP transport
//
// Products are sharded by tenant (see docs/sharding.md). Every repository
// method takes the tenant id and filters on it, so each statement is a
// single-shard "router" query on Citus.
package catalog

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/Oxyrus/orderbook/internal/platform/httpx"
)

// Domain errors. Handlers map them to HTTP status codes.
var (
	ErrNotFound        = errors.New("product not found")
	ErrSKUTaken        = errors.New("sku already in use")
	ErrVersionConflict = errors.New("product was modified concurrently")
	ErrUnknownCurrency = errors.New("unknown currency")
)

// Product is something a tenant sells.
type Product struct {
	TenantID   string
	ID         uuid.UUID
	SKU        string
	Name       string
	PriceMinor int64 // price in the currency's minor unit (cents)
	Currency   string
	Active     bool
	Version    int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ListParams filters and paginates listings.
type ListParams struct {
	TenantID string
	Limit    int
	After    *httpx.Cursor
}

// Page is a page of products.
type Page struct {
	Products   []Product
	NextCursor *httpx.Cursor
}

// Repository persists products.
type Repository interface {
	Create(ctx context.Context, p *Product) error
	Get(ctx context.Context, tenantID string, id uuid.UUID) (*Product, error)
	List(ctx context.Context, p ListParams) (Page, error)
	// Update persists p if p.Version matches the stored version, then
	// increments p.Version (optimistic concurrency control).
	Update(ctx context.Context, p *Product) error
	// LockForOrder returns the requested products of one tenant and takes a
	// FOR SHARE lock on them until the surrounding transaction ends, so a
	// concurrent price change can't slip in between pricing and placing an
	// order. Missing ids are simply absent from the result.
	LockForOrder(ctx context.Context, tenantID string, ids []uuid.UUID) ([]Product, error)
}

// TenantProvisioner creates the tenant row on first use.
type TenantProvisioner interface {
	Ensure(ctx context.Context, tenantID string) error
}
