// Package health implements Kubernetes-style liveness and readiness probes.
//
//   - /livez  : the process is alive and serving HTTP. Never checks
//     dependencies (a DB outage must not cause restarts).
//   - /readyz : the instance can serve traffic. Runs all registered checks
//     and fails while shutting down so load balancers drain the instance.
//
// Live and Ready include per-check results and build info and are meant for
// the internal admin server. PublicLive and PublicReady answer with the
// status only, so probes on the public port don't leak dependency errors
// (hostnames, users, database names) or the deployed commit.
package health

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Oxyrus/orderbook/internal/buildinfo"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
)

// CheckFunc returns nil when the dependency is healthy.
type CheckFunc func(ctx context.Context) error

// Checker aggregates readiness checks.
type Checker struct {
	timeout  time.Duration
	mu       sync.RWMutex
	checks   map[string]CheckFunc
	starting atomic.Bool
	draining atomic.Bool
}

// New returns a Checker whose checks each get timeout to complete.
func New(timeout time.Duration) *Checker {
	return &Checker{timeout: timeout, checks: map[string]CheckFunc{}}
}

// Register adds a named readiness check.
func (c *Checker) Register(name string, fn CheckFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checks[name] = fn
}

// SetStarting marks the instance as not ready because it is still
// initialising. Liveness is unaffected, so orchestrators don't restart a pod
// that is busy running migrations or waiting for a dependency.
func (c *Checker) SetStarting() { c.starting.Store(true) }

// SetStarted ends the starting state set by SetStarting.
func (c *Checker) SetStarted() { c.starting.Store(false) }

// SetDraining marks the instance as not ready (called at shutdown start).
func (c *Checker) SetDraining() { c.draining.Store(true) }

type response struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
	buildinfo.Info
}

type publicResponse struct {
	Status string `json:"status"`
}

// Live handles /livez on the admin server.
func (c *Checker) Live(w http.ResponseWriter, r *http.Request) {
	httpx.JSON(w, r, http.StatusOK, response{Status: "ok", Info: buildinfo.Get()})
}

// PublicLive handles /livez on the public server.
func (c *Checker) PublicLive(w http.ResponseWriter, r *http.Request) {
	httpx.JSON(w, r, http.StatusOK, publicResponse{Status: "ok"})
}

// Ready handles /readyz on the admin server. Checks run concurrently and the
// response includes each check's result.
func (c *Checker) Ready(w http.ResponseWriter, r *http.Request) {
	code, resp := c.check(r.Context())
	httpx.JSON(w, r, code, resp)
}

// PublicReady handles /readyz on the public server. It runs the same checks
// as Ready but only reports the overall status.
func (c *Checker) PublicReady(w http.ResponseWriter, r *http.Request) {
	code, resp := c.check(r.Context())
	httpx.JSON(w, r, code, publicResponse{Status: resp.Status})
}

func (c *Checker) check(ctx context.Context) (int, response) {
	if c.draining.Load() {
		return http.StatusServiceUnavailable, response{Status: "draining", Info: buildinfo.Get()}
	}
	if c.starting.Load() {
		return http.StatusServiceUnavailable, response{Status: "starting", Info: buildinfo.Get()}
	}

	c.mu.RLock()
	checks := make(map[string]CheckFunc, len(c.checks))
	for k, v := range c.checks {
		checks[k] = v
	}
	c.mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make(map[string]string, len(checks))
		healthy = true
	)
	for name, fn := range checks {
		wg.Go(func() {
			status := "ok"
			if err := fn(ctx); err != nil {
				status = "fail: " + err.Error()
			}
			mu.Lock()
			defer mu.Unlock()
			results[name] = status
			if status != "ok" {
				healthy = false
			}
		})
	}
	wg.Wait()

	resp := response{Status: "ok", Checks: results, Info: buildinfo.Get()}
	code := http.StatusOK
	if !healthy {
		resp.Status = "unavailable"
		code = http.StatusServiceUnavailable
	}
	return code, resp
}
