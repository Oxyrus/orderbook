//go:build integration

package app_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/app"
	"github.com/Oxyrus/orderbook/internal/config"
	"github.com/Oxyrus/orderbook/internal/platform/logger"
	"github.com/Oxyrus/orderbook/internal/testutil/oauthtest"
)

// freeAddr returns a loopback address with a currently free port.
func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func probe(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body.Status
}

// TestRunLifecycle drives app.Run end to end: probes answer while
// initialisation is blocked on a slow identity provider, the API starts once
// it completes, and cancelling the context shuts everything down cleanly.
func TestRunLifecycle(t *testing.T) {
	t.Parallel()
	iss := oauthtest.NewIssuer(t)
	release := iss.HoldDiscovery(t)

	cfg := config.Defaults()
	cfg.Environment = config.EnvTest
	cfg.DB.URL = pg.NewDatabase(t)
	cfg.Auth = iss.AuthConfig()
	cfg.HTTP.Addr = freeAddr(t)
	cfg.Admin.Addr = freeAddr(t)
	cfg.HTTP.ShutdownTimeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx, cfg, logger.Nop()) }()

	admin := "http://" + cfg.Admin.Addr
	api := "http://" + cfg.HTTP.Addr

	// While discovery is held: alive, not ready, API not listening.
	require.Eventually(t, func() bool {
		code, _ := probe(t, admin+"/livez")
		return code == http.StatusOK
	}, 10*time.Second, 20*time.Millisecond, "admin /livez must answer during startup")
	code, status := probe(t, admin+"/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "starting", status)
	code, _ = probe(t, api+"/livez")
	assert.Zero(t, code, "the API port must not accept traffic before initialisation completes")

	// Initialisation completes.
	release()
	require.Eventually(t, func() bool {
		code, _ := probe(t, admin+"/readyz")
		return code == http.StatusOK
	}, 10*time.Second, 20*time.Millisecond)
	code, status = probe(t, api+"/readyz")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ok", status)

	// SIGTERM equivalent.
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	code, _ = probe(t, admin+"/livez")
	assert.Zero(t, code, "admin server must be stopped")
}

// TestRunInitFailureStopsAdmin checks that a failed startup returns the error
// and doesn't leave the admin server running.
func TestRunInitFailureStopsAdmin(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Environment = config.EnvTest
	cfg.DB.URL = "postgres://nobody:x@127.0.0.1:1/nothing?sslmode=disable"
	cfg.DB.ConnectTimeout = time.Second
	cfg.Admin.Addr = freeAddr(t)
	cfg.HTTP.Addr = freeAddr(t)

	err := app.Run(context.Background(), cfg, logger.Nop())
	require.ErrorContains(t, err, "ping database")
	code, _ := probe(t, "http://"+cfg.Admin.Addr+"/livez")
	assert.Zero(t, code, "admin server must be stopped after a failed startup")
}
