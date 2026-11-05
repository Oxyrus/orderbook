package catalog

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/Oxyrus/orderbook/internal/platform/auth"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
	"github.com/Oxyrus/orderbook/internal/platform/tenant"
)

// OAuth 2.0 scopes required by the catalog endpoints.
const (
	ScopeRead  = "catalog:read"
	ScopeWrite = "catalog:write"
)

// Handler exposes the catalog over HTTP.
type Handler struct {
	svc *Service
}

// NewHandler returns a Handler.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Routes mounts the catalog endpoints. The router must already run
// auth.Authenticate and tenant.Middleware.
func (h *Handler) Routes(r chi.Router) {
	r.Route("/products", func(r chi.Router) {
		r.With(auth.RequireScopes(ScopeRead)).Get("/", httpx.Handle(h.list))
		r.With(auth.RequireScopes(ScopeWrite)).Post("/", httpx.Handle(h.create))
		r.With(auth.RequireScopes(ScopeRead)).Get("/{id}", httpx.Handle(h.get))
		r.With(auth.RequireScopes(ScopeWrite)).Put("/{id}", httpx.Handle(h.update))
	})
}

type productResponse struct {
	ID         uuid.UUID `json:"id"`
	SKU        string    `json:"sku"`
	Name       string    `json:"name"`
	PriceMinor int64     `json:"price_minor"`
	Currency   string    `json:"currency"`
	Active     bool      `json:"active"`
	Version    int       `json:"version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func toResponse(p *Product) productResponse {
	return productResponse{
		ID: p.ID, SKU: p.SKU, Name: p.Name, PriceMinor: p.PriceMinor, Currency: p.Currency,
		Active: p.Active, Version: p.Version, CreatedAt: p.CreatedAt.UTC(), UpdatedAt: p.UpdatedAt.UTC(),
	}
}

type listResponse struct {
	Data       []productResponse `json:"data"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type writeRequest struct {
	SKU        string `json:"sku" validate:"required,notblank,max=64"`
	Name       string `json:"name" validate:"required,notblank,max=200"`
	PriceMinor *int64 `json:"price_minor" validate:"required,min=0"`
	Currency   string `json:"currency" validate:"required,len=3,uppercase"`
	Active     *bool  `json:"active"`
	// Version is only used by PUT, for clients that can't send If-Match.
	Version int `json:"version" validate:"omitempty,min=1"`
}

func (req writeRequest) input() CreateInput {
	active := true
	if req.Active != nil {
		active = *req.Active
	}
	return CreateInput{SKU: req.SKU, Name: req.Name, PriceMinor: *req.PriceMinor, Currency: req.Currency, Active: active}
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) error {
	var req writeRequest
	if err := httpx.Decode(r, &req); err != nil {
		return err
	}
	p, err := h.svc.Create(r.Context(), tenant.MustFromContext(r.Context()), req.input())
	if err != nil {
		return mapError(err)
	}
	w.Header().Set("Location", "/v1/products/"+p.ID.String())
	w.Header().Set("ETag", httpx.ETag(p.Version))
	httpx.JSON(w, r, http.StatusCreated, toResponse(p))
	return nil
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	p, err := h.svc.Get(r.Context(), tenant.MustFromContext(r.Context()), id)
	if err != nil {
		return mapError(err)
	}
	w.Header().Set("ETag", httpx.ETag(p.Version))
	if httpx.NotModified(r, p.Version) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	httpx.JSON(w, r, http.StatusOK, toResponse(p))
	return nil
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) error {
	limit, after, err := httpx.PageParams(r, DefaultPageSize, MaxPageSize)
	if err != nil {
		return err
	}
	page, err := h.svc.List(r.Context(), tenant.MustFromContext(r.Context()), limit, after)
	if err != nil {
		return mapError(err)
	}
	resp := listResponse{Data: make([]productResponse, 0, len(page.Products))}
	for i := range page.Products {
		resp.Data = append(resp.Data, toResponse(&page.Products[i]))
	}
	if page.NextCursor != nil {
		resp.NextCursor = httpx.EncodeCursor(*page.NextCursor)
	}
	httpx.JSON(w, r, http.StatusOK, resp)
	return nil
}

func (h *Handler) update(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	var req writeRequest
	if err := httpx.Decode(r, &req); err != nil {
		return err
	}
	v, fromHeader, err := httpx.IfMatchVersion(r)
	if err != nil {
		return err
	}
	switch {
	case fromHeader && req.Version != 0 && req.Version != v:
		return httpx.BadRequest("version_mismatch", "The If-Match header and the body version disagree.")
	case !fromHeader && req.Version == 0:
		return httpx.Validation(map[string]string{"version": "is required unless an If-Match header is sent"})
	case !fromHeader:
		v = req.Version
	}
	p, err := h.svc.Update(r.Context(), tenant.MustFromContext(r.Context()), id, UpdateInput{CreateInput: req.input(), Version: v})
	switch {
	case errors.Is(err, ErrVersionConflict) && fromHeader:
		return httpx.PreconditionFailed("product_version_conflict", "The product was modified by someone else; fetch it again and retry.")
	case err != nil:
		return mapError(err)
	}
	w.Header().Set("ETag", httpx.ETag(p.Version))
	httpx.JSON(w, r, http.StatusOK, toResponse(p))
	return nil
}

func mapError(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return httpx.NotFound("product_not_found", "The product does not exist.")
	case errors.Is(err, ErrSKUTaken):
		return httpx.Conflict("product_sku_taken", "A product with this SKU already exists.")
	case errors.Is(err, ErrVersionConflict):
		return httpx.Conflict("product_version_conflict", "The product was modified by someone else; fetch it again and retry.")
	case errors.Is(err, ErrUnknownCurrency):
		return httpx.Validation(map[string]string{"currency": "is not a supported currency"})
	default:
		return err
	}
}

func pathID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		return uuid.Nil, httpx.NotFound("product_not_found", "The product does not exist.")
	}
	return id, nil
}
