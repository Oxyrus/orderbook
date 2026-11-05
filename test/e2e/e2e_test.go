//go:build e2e

// Package e2e drives the full local stack (make up-app, or make up + make run
// + make run-worker + make cdc-register):
//
//	API ──▶ Citus coordinator ──▶ shards on worker1/worker2 ──WAL──▶ Debezium
//	    ──▶ Kafka ──▶ api worker ──▶ tenant_daily_sales ──▶ API
//
// Run with `make e2e`. It uses fresh random tenants on every run, so it can
// run repeatedly against the same stack.
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	apiURL   = env("E2E_API_URL", "http://localhost:8080")
	tokenURL = env("E2E_TOKEN_URL", "http://localhost:8081/default/token")
	dbURL    = env("E2E_DB_URL", "postgres://orderbook:orderbook@localhost:5432/orderbook?sslmode=disable")
)

func token(t *testing.T, client, scope string) string {
	t.Helper()
	resp, err := http.PostForm(tokenURL, url.Values{
		"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {"secret"}, "scope": {scope},
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.NotEmpty(t, out.AccessToken)
	return out.AccessToken
}

func call(t *testing.T, method, path, tok string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, apiURL+path, rdr)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	if b, _ := io.ReadAll(resp.Body); len(b) > 0 {
		require.NoError(t, json.Unmarshal(b, &out), string(b))
	}
	return resp.StatusCode, out
}

// tenantsOnEveryWorker returns fresh tenant ids, at least one stored on each
// node that holds shards.
func tenantsOnEveryWorker(t *testing.T, conn *pgx.Conn) map[string]string {
	t.Helper()
	ctx := context.Background()
	var nodes int
	require.NoError(t, conn.QueryRow(ctx,
		`SELECT count(DISTINCT nodename) FROM citus_shards WHERE table_name = 'tenants'::regclass`).Scan(&nodes))

	byNode := map[string]string{} // node -> tenant
	for i := 0; len(byNode) < nodes && i < 200; i++ {
		b := make([]byte, 4)
		_, _ = rand.Read(b)
		id := "e2e-" + hex.EncodeToString(b)
		var node string
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT nodename FROM citus_shards
			WHERE table_name = 'tenants'::regclass
			  AND shardid = get_shard_id_for_distribution_column('tenants', $1::text)`, id).Scan(&node))
		if _, ok := byNode[node]; !ok {
			byNode[node] = id
		}
	}
	require.Len(t, byNode, nodes)
	return byNode
}

func TestEndToEnd(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dbURL)
	require.NoError(t, err, "is the stack up? (make up-app)")
	defer conn.Close(ctx)

	tenants := tenantsOnEveryWorker(t, conn)
	t.Logf("tenants per node: %v", tenants)
	today := time.Now().UTC().Format(time.DateOnly)

	for node, tn := range tenants {
		t.Run(node, func(t *testing.T) {
			tok := token(t, tn, "all")

			code, p := call(t, http.MethodPost, "/v1/products", tok, map[string]any{
				"sku": "SKU-1", "name": "Thing", "price_minor": 1999, "currency": "USD",
			})
			require.Equal(t, http.StatusCreated, code, p)

			var cancelID string
			for i, qty := range []int{1, 2, 3} {
				code, o := call(t, http.MethodPost, "/v1/orders", tok, map[string]any{
					"customer_ref": fmt.Sprintf("cust-%d", i),
					"lines":        []map[string]any{{"product_id": p["id"], "quantity": qty}},
				})
				require.Equal(t, http.StatusCreated, code, o)
				if i == 0 {
					cancelID = o["id"].(string)
				}
			}
			code, o := call(t, http.MethodPost, "/v1/orders/"+cancelID+"/cancel", tok, nil)
			require.Equal(t, http.StatusOK, code, o)

			// The projection is eventually consistent: wait for the CDC
			// pipeline to deliver all four events.
			var day map[string]any
			require.Eventually(t, func() bool {
				code, body := call(t, http.MethodGet, "/v1/sales/daily?from="+today+"&to="+today, tok, nil)
				if code != http.StatusOK {
					return false
				}
				data, _ := body["data"].([]any)
				if len(data) != 1 {
					return false
				}
				day = data[0].(map[string]any)
				return day["orders_placed"] == float64(3) && day["orders_cancelled"] == float64(1)
			}, 60*time.Second, time.Second, "projection never caught up; check `make cdc-status` and the worker logs")
			assert.EqualValues(t, 6*1999, day["gross_minor"])
			assert.EqualValues(t, 5*1999, day["net_minor"])
		})
	}

	// The platform report merges every shard on every worker.
	ops := token(t, "ops", "platform:read")
	code, body := call(t, http.MethodGet, "/v1/platform/sales/top-tenants?limit=100&from="+today+"&to="+today, ops, nil)
	require.Equal(t, http.StatusOK, code, body)
	found := 0
	for _, row := range body["data"].([]any) {
		if strings.HasPrefix(row.(map[string]any)["tenant_id"].(string), "e2e-") {
			for _, tn := range tenants {
				if row.(map[string]any)["tenant_id"] == tn {
					found++
				}
			}
		}
	}
	assert.Equal(t, len(tenants), found, "every tenant, whichever worker it lives on, appears in the cross-tenant report")
}
