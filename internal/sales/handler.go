package sales

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Oxyrus/orderbook/internal/platform/auth"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
	"github.com/Oxyrus/orderbook/internal/platform/tenant"
)

// OAuth 2.0 scopes.
const (
	// ScopeRead reads the caller's own tenant's sales.
	ScopeRead = "sales:read"
	// ScopePlatformRead reads cross-tenant reports. Grant it to internal
	// operator clients only.
	ScopePlatformRead = "platform:read"
)

// MaxRangeDays bounds report ranges.
const MaxRangeDays = 366

// Handler serves the read model.
type Handler struct {
	store Store
	now   func() time.Time
}

// NewHandler returns a Handler.
func NewHandler(store Store) *Handler { return &Handler{store: store, now: time.Now} }

// Routes mounts the tenant endpoints (behind tenant.Middleware).
func (h *Handler) Routes(r chi.Router) {
	r.With(auth.RequireScopes(ScopeRead)).Get("/sales/daily", httpx.Handle(h.daily))
}

// PlatformRoutes mounts cross-tenant endpoints (NOT behind
// tenant.Middleware).
func (h *Handler) PlatformRoutes(r chi.Router) {
	r.With(auth.RequireScopes(ScopePlatformRead)).Get("/sales/top-tenants", httpx.Handle(h.topTenants))
}

type dailyResponse struct {
	Day             string `json:"day"`
	Currency        string `json:"currency"`
	OrdersPlaced    int64  `json:"orders_placed"`
	OrdersCancelled int64  `json:"orders_cancelled"`
	GrossMinor      int64  `json:"gross_minor"`
	CancelledMinor  int64  `json:"cancelled_minor"`
	NetMinor        int64  `json:"net_minor"`
}

func (h *Handler) daily(w http.ResponseWriter, r *http.Request) error {
	from, to, err := h.dateRange(r)
	if err != nil {
		return err
	}
	rows, err := h.store.Daily(r.Context(), tenant.MustFromContext(r.Context()), from, to)
	if err != nil {
		return err
	}
	out := make([]dailyResponse, 0, len(rows))
	for _, d := range rows {
		out = append(out, dailyResponse{
			Day: d.Day.Format(time.DateOnly), Currency: d.Currency, OrdersPlaced: d.OrdersPlaced,
			OrdersCancelled: d.OrdersCancelled, GrossMinor: d.GrossMinor, CancelledMinor: d.CancelledMinor, NetMinor: d.NetMinor(),
		})
	}
	httpx.JSON(w, r, http.StatusOK, map[string]any{
		"from": from.Format(time.DateOnly), "to": to.Format(time.DateOnly), "data": out,
	})
	return nil
}

type tenantTotalResponse struct {
	TenantID     string `json:"tenant_id"`
	Currency     string `json:"currency"`
	OrdersPlaced int64  `json:"orders_placed"`
	NetMinor     int64  `json:"net_minor"`
}

func (h *Handler) topTenants(w http.ResponseWriter, r *http.Request) error {
	from, to, err := h.dateRange(r)
	if err != nil {
		return err
	}
	q := r.URL.Query()
	currency := q.Get("currency")
	if currency == "" {
		currency = "USD"
	}
	if len(currency) != 3 {
		return httpx.Validation(map[string]string{"currency": "must be a 3-letter ISO 4217 code"})
	}
	limit := 10
	if v := q.Get("limit"); v != "" {
		n, convErr := strconv.Atoi(v)
		if convErr != nil || n < 1 || n > 100 {
			return httpx.Validation(map[string]string{"limit": "must be an integer between 1 and 100"})
		}
		limit = n
	}
	rows, err := h.store.TopTenants(r.Context(), from, to, currency, limit)
	if err != nil {
		return err
	}
	out := make([]tenantTotalResponse, 0, len(rows))
	for _, t := range rows {
		out = append(out, tenantTotalResponse(t))
	}
	httpx.JSON(w, r, http.StatusOK, map[string]any{
		"from": from.Format(time.DateOnly), "to": to.Format(time.DateOnly), "currency": currency, "data": out,
	})
	return nil
}

// dateRange parses ?from=YYYY-MM-DD&to=YYYY-MM-DD, defaulting to the last
// 30 days (UTC).
func (h *Handler) dateRange(r *http.Request) (from, to time.Time, err error) {
	q := r.URL.Query()
	to = day(h.now())
	from = to.AddDate(0, 0, -29)
	fields := map[string]string{}
	if v := q.Get("to"); v != "" {
		if to, err = time.Parse(time.DateOnly, v); err != nil {
			fields["to"] = "must be a date (YYYY-MM-DD)"
		}
	}
	if v := q.Get("from"); v != "" {
		if from, err = time.Parse(time.DateOnly, v); err != nil {
			fields["from"] = "must be a date (YYYY-MM-DD)"
		}
	} else if q.Get("to") != "" {
		from = to.AddDate(0, 0, -29)
	}
	if len(fields) == 0 {
		switch {
		case from.After(to):
			fields["from"] = "must not be after to"
		case to.Sub(from) > MaxRangeDays*24*time.Hour:
			fields["to"] = "range must not exceed " + strconv.Itoa(MaxRangeDays) + " days"
		}
	}
	if len(fields) > 0 {
		return time.Time{}, time.Time{}, httpx.Validation(fields)
	}
	return from, to, nil
}
