package health_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Oxyrus/orderbook/internal/health"
)

func ready(c *health.Checker) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	c.Ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return rec
}

func TestReady(t *testing.T) {
	c := health.New(time.Second)
	c.Register("db", func(context.Context) error { return nil })
	assert.Equal(t, http.StatusOK, ready(c).Code)

	c.Register("cache", func(context.Context) error { return errors.New("down") })
	rec := ready(c)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "fail: down")
}

func TestReadyTimeout(t *testing.T) {
	c := health.New(20 * time.Millisecond)
	c.Register("slow", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	assert.Equal(t, http.StatusServiceUnavailable, ready(c).Code)
}

func TestDraining(t *testing.T) {
	c := health.New(time.Second)
	c.SetDraining()
	rec := ready(c)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "draining")

	live := httptest.NewRecorder()
	c.Live(live, httptest.NewRequest(http.MethodGet, "/livez", nil))
	assert.Equal(t, http.StatusOK, live.Code, "liveness must not depend on draining")
}

func TestPublicProbesHideDetails(t *testing.T) {
	c := health.New(time.Second)
	c.Register("postgres", func(context.Context) error {
		return errors.New("dial tcp 10.0.0.5:5432: user=orderbook database=prod")
	})

	rec := httptest.NewRecorder()
	c.PublicReady(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.JSONEq(t, `{"status":"unavailable"}`, rec.Body.String())

	rec = httptest.NewRecorder()
	c.PublicLive(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"ok"}`, rec.Body.String())

	c.SetDraining()
	rec = httptest.NewRecorder()
	c.PublicReady(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.JSONEq(t, `{"status":"draining"}`, rec.Body.String())
}

func TestStarting(t *testing.T) {
	c := health.New(time.Second)
	c.SetStarting()
	rec := ready(c)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), `"starting"`)

	live := httptest.NewRecorder()
	c.Live(live, httptest.NewRequest(http.MethodGet, "/livez", nil))
	assert.Equal(t, http.StatusOK, live.Code, "liveness must not depend on startup")

	c.SetStarted()
	assert.Equal(t, http.StatusOK, ready(c).Code)
}
