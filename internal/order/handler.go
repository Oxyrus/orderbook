package order

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

// OAuth 2.0 scopes required by the order endpoints.
const (
	ScopeRead  = "orders:read"
	ScopeWrite = "orders:write"
)

// Handler exposes orders over HTTP.
type Handler struct {
	svc *Service
}

// NewHandler returns a Handler.
func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Routes mounts the order endpoints. The router must already run
// auth.Authenticate and tenant.Middleware.
func (h *Handler) Routes(r chi.Router) {
	r.Route("/orders", func(r chi.Router) {
		r.With(auth.RequireScopes(ScopeRead)).Get("/", httpx.Handle(h.list))
		r.With(auth.RequireScopes(ScopeWrite)).Post("/", httpx.Handle(h.place))
		r.With(auth.RequireScopes(ScopeRead)).Get("/{id}", httpx.Handle(h.get))
		r.With(auth.RequireScopes(ScopeWrite)).Post("/{id}/cancel", httpx.Handle(h.cancel))
	})
}

type lineResponse struct {
	LineNo         int       `json:"line_no"`
	ProductID      uuid.UUID `json:"product_id"`
	SKU            string    `json:"sku"`
	Name           string    `json:"name"`
	Quantity       int       `json:"quantity"`
	UnitPriceMinor int64     `json:"unit_price_minor"`
}

type orderResponse struct {
	ID          uuid.UUID      `json:"id"`
	CustomerRef string         `json:"customer_ref"`
	Status      string         `json:"status"`
	Currency    string         `json:"currency"`
	TotalMinor  int64          `json:"total_minor"`
	Version     int            `json:"version"`
	PlacedAt    time.Time      `json:"placed_at"`
	CancelledAt *time.Time     `json:"cancelled_at,omitempty"`
	UpdatedAt   time.Time      `json:"updated_at"`
	Lines       []lineResponse `json:"lines,omitempty"`
}

func toResponse(o *Order) orderResponse {
	resp := orderResponse{
		ID: o.ID, CustomerRef: o.CustomerRef, Status: o.Status, Currency: o.Currency,
		TotalMinor: o.TotalMinor, Version: o.Version, PlacedAt: o.PlacedAt.UTC(), UpdatedAt: o.UpdatedAt.UTC(),
	}
	if o.CancelledAt != nil {
		t := o.CancelledAt.UTC()
		resp.CancelledAt = &t
	}
	for _, l := range o.Lines {
		resp.Lines = append(resp.Lines, lineResponse(l))
	}
	return resp
}

type listResponse struct {
	Data       []orderResponse `json:"data"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type placeRequest struct {
	CustomerRef string `json:"customer_ref" validate:"required,notblank,max=200"`
	Lines       []struct {
		ProductID uuid.UUID `json:"product_id" validate:"required"`
		Quantity  int       `json:"quantity" validate:"required,min=1,max=10000"`
	} `json:"lines" validate:"required,min=1,max=100,dive"`
}

func (h *Handler) place(w http.ResponseWriter, r *http.Request) error {
	var req placeRequest
	if err := httpx.Decode(r, &req); err != nil {
		return err
	}
	in := PlaceInput{CustomerRef: req.CustomerRef}
	for _, l := range req.Lines {
		in.Lines = append(in.Lines, LineInput{ProductID: l.ProductID, Quantity: l.Quantity})
	}
	o, err := h.svc.Place(r.Context(), tenant.MustFromContext(r.Context()), in)
	if err != nil {
		return mapError(err)
	}
	w.Header().Set("Location", "/v1/orders/"+o.ID.String())
	w.Header().Set("ETag", httpx.ETag(o.Version))
	httpx.JSON(w, r, http.StatusCreated, toResponse(o))
	return nil
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	o, err := h.svc.Get(r.Context(), tenant.MustFromContext(r.Context()), id)
	if err != nil {
		return mapError(err)
	}
	w.Header().Set("ETag", httpx.ETag(o.Version))
	if httpx.NotModified(r, o.Version) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	httpx.JSON(w, r, http.StatusOK, toResponse(o))
	return nil
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) error {
	limit, after, err := httpx.PageParams(r, DefaultPageSize, MaxPageSize)
	if err != nil {
		return err
	}
	status := r.URL.Query().Get("status")
	if status != "" && status != StatusPlaced && status != StatusCancelled {
		return httpx.Validation(map[string]string{"status": "must be placed or cancelled"})
	}
	page, err := h.svc.List(r.Context(), ListParams{
		TenantID: tenant.MustFromContext(r.Context()), Status: status, Limit: limit, After: after,
	})
	if err != nil {
		return mapError(err)
	}
	resp := listResponse{Data: make([]orderResponse, 0, len(page.Orders))}
	for i := range page.Orders {
		resp.Data = append(resp.Data, toResponse(&page.Orders[i]))
	}
	if page.NextCursor != nil {
		resp.NextCursor = httpx.EncodeCursor(*page.NextCursor)
	}
	httpx.JSON(w, r, http.StatusOK, resp)
	return nil
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) error {
	id, err := pathID(r)
	if err != nil {
		return err
	}
	version, conditional, err := httpx.IfMatchVersion(r)
	if err != nil {
		return err
	}
	o, err := h.svc.Cancel(r.Context(), tenant.MustFromContext(r.Context()), id, version)
	switch {
	case errors.Is(err, ErrVersionConflict) && conditional:
		return httpx.PreconditionFailed("order_version_conflict", "The order was modified by someone else; fetch it again and retry.")
	case err != nil:
		return mapError(err)
	}
	w.Header().Set("ETag", httpx.ETag(o.Version))
	httpx.JSON(w, r, http.StatusOK, toResponse(o))
	return nil
}

func mapError(err error) error {
	var pe *ProductError
	if errors.As(err, &pe) {
		code, detail := "", ""
		switch {
		case errors.Is(err, ErrUnknownProduct):
			code, detail = "unknown_product", "A line references a product that does not exist."
		case errors.Is(err, ErrInactiveProduct):
			code, detail = "inactive_product", "A line references a product that is not for sale."
		case errors.Is(err, ErrMixedCurrencies):
			code, detail = "mixed_currencies", "All products of an order must be priced in the same currency."
		case errors.Is(err, ErrDuplicateProduct):
			code, detail = "duplicate_product", "A product may appear in only one line; use the quantity."
		}
		return &httpx.Error{Status: http.StatusUnprocessableEntity, Code: code, Detail: detail,
			Fields: map[string]string{"product_id": pe.ProductID.String()}}
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return httpx.NotFound("order_not_found", "The order does not exist.")
	case errors.Is(err, ErrAlreadyCancelled):
		return httpx.Conflict("order_already_cancelled", "The order is already cancelled.")
	case errors.Is(err, ErrVersionConflict):
		return httpx.Conflict("order_version_conflict", "The order was modified by someone else; fetch it again and retry.")
	case errors.Is(err, ErrInvalidLineRequest):
		return httpx.Validation(map[string]string{"lines": "must contain 1-100 lines with quantities between 1 and 10000"})
	case errors.Is(err, ErrTotalOutOfRange):
		return httpx.Validation(map[string]string{"lines": "order total is too large"})
	default:
		return err
	}
}

func pathID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		return uuid.Nil, httpx.NotFound("order_not_found", "The order does not exist.")
	}
	return id, nil
}
