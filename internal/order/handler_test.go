package order_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/catalog"
	"github.com/Oxyrus/orderbook/internal/order"
	"github.com/Oxyrus/orderbook/internal/platform/auth"
	"github.com/Oxyrus/orderbook/internal/platform/tenant"
)

func newRouter(products ...catalog.Product) http.Handler {
	svc := order.NewService(&fakeRepo{}, fakeProducts(products), &fakeEvents{}, &fakeTenants{}, &fakeTx{})
	r := chi.NewRouter()
	r.Use(auth.Static(&auth.Principal{
		Subject: "u", Scopes: []string{order.ScopeRead, order.ScopeWrite}, Claims: map[string]any{"tenant_id": "acme"},
	}))
	r.Use(tenant.Middleware("tenant_id"))
	order.NewHandler(svc).Routes(r)
	return r
}

func call(t *testing.T, h http.Handler, method, path, body string, headers ...string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out), rec.Body.String())
	}
	return rec.Code, out
}

func TestHandlerPlace(t *testing.T) {
	p := catalog.Product{TenantID: "acme", ID: uuid.New(), SKU: "A", PriceMinor: 100, Currency: "USD", Active: true}
	h := newRouter(p)

	code, body := call(t, h, http.MethodPost, "/orders",
		`{"customer_ref":"c","lines":[{"product_id":"`+p.ID.String()+`","quantity":2}]}`)
	require.Equal(t, http.StatusCreated, code, body)
	assert.EqualValues(t, 200, body["total_minor"])

	unknown := uuid.NewString()
	code, body = call(t, h, http.MethodPost, "/orders", `{"customer_ref":"c","lines":[{"product_id":"`+unknown+`","quantity":1}]}`)
	assert.Equal(t, http.StatusUnprocessableEntity, code)
	assert.Equal(t, "unknown_product", body["code"])
	assert.Equal(t, map[string]any{"product_id": unknown}, body["errors"])

	code, body = call(t, h, http.MethodPost, "/orders", `{"customer_ref":"c","lines":[]}`)
	assert.Equal(t, http.StatusUnprocessableEntity, code)
	assert.Equal(t, "validation_failed", body["code"])

	code, _ = call(t, h, http.MethodPost, "/orders", `{"customer_ref":"c","lines":[{"product_id":"`+p.ID.String()+`","quantity":0}]}`)
	assert.Equal(t, http.StatusUnprocessableEntity, code)
}

func TestHandlerGetAndCancelErrors(t *testing.T) {
	h := newRouter()
	code, body := call(t, h, http.MethodGet, "/orders/not-a-uuid", "")
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "order_not_found", body["code"])

	code, _ = call(t, h, http.MethodPost, "/orders/"+uuid.NewString()+"/cancel", "", "If-Match", "nope")
	assert.Equal(t, http.StatusBadRequest, code)

	code, _ = call(t, h, http.MethodGet, "/orders?status=shipped", "")
	assert.Equal(t, http.StatusUnprocessableEntity, code)
	code, _ = call(t, h, http.MethodGet, "/orders?status=placed&limit=5", "")
	assert.Equal(t, http.StatusOK, code)
}
