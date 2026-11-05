package catalog

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/Oxyrus/orderbook/internal/platform/httpx"
)

// Pagination bounds.
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// Service contains the catalog business logic.
type Service struct {
	repo    Repository
	tenants TenantProvisioner
}

// NewService wires a Service.
func NewService(repo Repository, tenants TenantProvisioner) *Service {
	return &Service{repo: repo, tenants: tenants}
}

// CreateInput holds the fields of a new product.
type CreateInput struct {
	SKU        string
	Name       string
	PriceMinor int64
	Currency   string
	Active     bool
}

// UpdateInput is a full replacement of the mutable fields.
type UpdateInput struct {
	CreateInput
	Version int
}

// Create adds a product to the tenant's catalog.
func (s *Service) Create(ctx context.Context, tenantID string, in CreateInput) (*Product, error) {
	if err := s.tenants.Ensure(ctx, tenantID); err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate id: %w", err)
	}
	p := &Product{
		TenantID: tenantID, ID: id, SKU: in.SKU, Name: in.Name,
		PriceMinor: in.PriceMinor, Currency: in.Currency, Active: in.Active,
	}
	if err := s.repo.Create(ctx, p); err != nil {
		return nil, err
	}
	return p, nil
}

// Get returns one product.
func (s *Service) Get(ctx context.Context, tenantID string, id uuid.UUID) (*Product, error) {
	return s.repo.Get(ctx, tenantID, id)
}

// List returns a page of the tenant's products, newest first.
func (s *Service) List(ctx context.Context, tenantID string, limit int, after *httpx.Cursor) (Page, error) {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	return s.repo.List(ctx, ListParams{TenantID: tenantID, Limit: min(limit, MaxPageSize), After: after})
}

// Update replaces a product's mutable fields using optimistic locking.
// Orders already placed keep the price they were placed at (order lines
// copy it).
func (s *Service) Update(ctx context.Context, tenantID string, id uuid.UUID, in UpdateInput) (*Product, error) {
	p := &Product{
		TenantID: tenantID, ID: id, SKU: in.SKU, Name: in.Name,
		PriceMinor: in.PriceMinor, Currency: in.Currency, Active: in.Active, Version: in.Version,
	}
	if err := s.repo.Update(ctx, p); err != nil {
		return nil, err
	}
	return p, nil
}
