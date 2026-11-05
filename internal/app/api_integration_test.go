//go:build integration

package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/app"
	"github.com/Oxyrus/orderbook/internal/config"
	"github.com/Oxyrus/orderbook/internal/health"
	"github.com/Oxyrus/orderbook/internal/platform/auth"
	"github.com/Oxyrus/orderbook/internal/platform/database"
	"github.com/Oxyrus/orderbook/internal/platform/logger"
	"github.com/Oxyrus/orderbook/internal/testutil/oauthtest"
	"github.com/Oxyrus/orderbook/internal/testutil/pgtest"
)

var pg *pgtest.Server

func TestMain(m *testing.M) { pgtest.Main(m, &pg) }

// apiClient drives the real router over HTTP with real JWTs and a real DB.
type apiClient struct {
	t      *testing.T
	srv    *httptest.Server
	issuer *oauthtest.Issuer
	db     *database.DB
}

func newAPI(t *testing.T, mutate ...func(*config.Config)) *apiClient {
	t.Helper()
	t.Parallel()

	db := pg.NewDB(t)
	iss := oauthtest.NewIssuer(t)

	cfg := config.Defaults()
	cfg.Environment = config.EnvTest
	cfg.Auth = iss.AuthConfig()
	for _, m := range mutate {
		m(&cfg)
	}

	verifier, err := auth.NewJWTVerifier(context.Background(), cfg.Auth)
	require.NoError(t, err)

	hc := health.New(time.Second)
	hc.Register("postgres", db.Ping)
	features := app.NewFeatureHandlers(db)

	handler := app.NewRouter(app.RouterDeps{
		Config:   cfg,
		Logger:   logger.Nop(),
		Registry: prometheus.NewRegistry(),
		Health:   hc,
		Auth:     auth.Authenticate(verifier),
		Catalog:  features.Catalog,
		Orders:   features.Orders,
		Sales:    features.Sales,
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &apiClient{t: t, srv: srv, issuer: iss, db: db}
}

const allScopes = "catalog:read catalog:write orders:read orders:write sales:read"

// token mints a token for a user of tenant (no tenant claim when tenant is "").
func (c *apiClient) token(tenant, scope string) string {
	claims := oauthtest.Claims{Subject: "user-of-" + tenant, Scope: scope}
	if tenant != "" {
		claims.Extra = map[string]any{"tenant_id": tenant}
	}
	return c.issuer.Token(c.t, claims)
}

func (c *apiClient) do(method, path, token string, body any) (*http.Response, map[string]any) {
	c.t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(c.t, err)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.srv.URL+path, rdr)
	require.NoError(c.t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.srv.Client().Do(req)
	require.NoError(c.t, err)
	defer resp.Body.Close()

	var out map[string]any
	if b, _ := io.ReadAll(resp.Body); len(b) > 0 {
		require.NoError(c.t, json.Unmarshal(b, &out), string(b))
	}
	return resp, out
}

func TestAPIOrderLifecycle(t *testing.T) {
	c := newAPI(t)
	tok := c.token("acme", allScopes)

	resp, body := c.do(http.MethodPost, "/v1/products", tok, map[string]any{
		"sku": "W-1", "name": "Widget", "price_minor": 1250, "currency": "USD",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, body)
	productID := body["id"].(string)
	assert.NotEmpty(t, resp.Header.Get("X-Request-ID"))

	resp, body = c.do(http.MethodPost, "/v1/products", tok, map[string]any{
		"sku": "W-1", "name": "Dup", "price_minor": 1, "currency": "USD",
	})
	assert.Equal(t, http.StatusConflict, resp.StatusCode, "unique (tenant_id, sku) is enforced on the shard")
	assert.Equal(t, "product_sku_taken", body["code"])

	resp, body = c.do(http.MethodPost, "/v1/products", tok, map[string]any{
		"sku": "W-2", "name": "Bad", "price_minor": 1, "currency": "XXX",
	})
	assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode, "FK to the currencies reference table")
	assert.Equal(t, "validation_failed", body["code"])

	resp, body = c.do(http.MethodPost, "/v1/orders", tok, map[string]any{
		"customer_ref": "cust-1",
		"lines":        []map[string]any{{"product_id": productID, "quantity": 3}},
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, body)
	orderID := body["id"].(string)
	assert.EqualValues(t, 3750, body["total_minor"])
	assert.Equal(t, "placed", body["status"])

	resp, body = c.do(http.MethodGet, "/v1/orders/"+orderID, tok, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, body["lines"], 1)

	resp, body = c.do(http.MethodPost, "/v1/orders/"+orderID+"/cancel", tok, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Equal(t, "cancelled", body["status"])
	assert.Equal(t, `"2"`, resp.Header.Get("ETag"))

	resp, body = c.do(http.MethodPost, "/v1/orders/"+orderID+"/cancel", tok, nil)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.Equal(t, "order_already_cancelled", body["code"])

	// Both changes were recorded in the co-located outbox, in their
	// transactions.
	var events []string
	rows, err := c.db.Pool.Query(context.Background(),
		`SELECT type FROM outbox WHERE tenant_id = 'acme' AND aggregateid = $1 ORDER BY id`, orderID)
	require.NoError(t, err)
	for rows.Next() {
		var typ string
		require.NoError(t, rows.Scan(&typ))
		events = append(events, typ)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"order.placed", "order.cancelled"}, events)
}

func TestAPITenantIsolation(t *testing.T) {
	c := newAPI(t)
	acme, globex := c.token("acme", allScopes), c.token("globex", allScopes)

	resp, body := c.do(http.MethodPost, "/v1/products", acme, map[string]any{
		"sku": "SHARED-SKU", "name": "Acme's", "price_minor": 100, "currency": "USD",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, body)
	acmeProduct := body["id"].(string)

	// Same SKU in another tenant is fine: uniqueness is per tenant.
	resp, _ = c.do(http.MethodPost, "/v1/products", globex, map[string]any{
		"sku": "SHARED-SKU", "name": "Globex's", "price_minor": 100, "currency": "USD",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	resp, _ = c.do(http.MethodGet, "/v1/products/"+acmeProduct, globex, nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "globex cannot read acme's product")

	resp, body = c.do(http.MethodPost, "/v1/orders", globex, map[string]any{
		"customer_ref": "c", "lines": []map[string]any{{"product_id": acmeProduct, "quantity": 1}},
	})
	assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode, "globex cannot order acme's product")
	assert.Equal(t, "unknown_product", body["code"])

	resp, body = c.do(http.MethodGet, "/v1/products", globex, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, body["data"], 1)
}

func TestAPIAuth(t *testing.T) {
	c := newAPI(t)

	resp, body := c.do(http.MethodGet, "/v1/orders", "", nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, "unauthorized", body["code"])

	resp, _ = c.do(http.MethodGet, "/v1/orders", c.issuer.ForeignToken(t), nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	resp, _ = c.do(http.MethodPost, "/v1/products", c.token("acme", "catalog:read"), map[string]any{"sku": "x"})
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	resp, body = c.do(http.MethodGet, "/v1/orders", c.token("", "orders:read"), nil)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode, "a valid token without a tenant can't use the tenant API")
	assert.Equal(t, "tenant_required", body["code"])

	resp, body = c.do(http.MethodGet, "/v1/orders", c.token("../ACME", "orders:read"), nil)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, "tenant_invalid", body["code"])

	resp, _ = c.do(http.MethodGet, "/v1/platform/sales/top-tenants", c.token("acme", allScopes), nil)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode, "tenant tokens lack platform:read")

	resp, body = c.do(http.MethodGet, "/v1/platform/sales/top-tenants", c.token("", "platform:read"), nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, body)

	resp, body = c.do(http.MethodGet, "/v1/me", c.token("acme", "orders:read"), nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "user-of-acme", body["sub"])
	assert.Equal(t, "acme", body["tenant"])
}

func TestAPIProbes(t *testing.T) {
	c := newAPI(t)
	resp, body := c.do(http.MethodGet, "/readyz", "", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]any{"status": "ok"}, body, "public probe must not expose check details or build info")

	resp, _ = c.do(http.MethodGet, "/livez", "", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAPIRateLimit(t *testing.T) {
	c := newAPI(t, func(cfg *config.Config) {
		cfg.RateLimit.Requests = 3
		cfg.RateLimit.Window = time.Minute
	})
	tok := c.token("acme", "orders:read")
	for range 3 {
		resp, _ := c.do(http.MethodGet, "/v1/orders", tok, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
	resp, body := c.do(http.MethodGet, "/v1/orders", tok, nil)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "rate_limited", body["code"])
}
