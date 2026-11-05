package app

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/platform/logger"
)

func TestForceShutdownCancelsStuckRequests(t *testing.T) {
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	srv := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			close(entered)
			<-r.Context().Done() // a handler stuck on a slow query
			close(cancelled)
		}),
	}
	ln, err := listen(context.Background(), "test", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = serve(logger.Nop(), "test", srv, ln) }()

	go func() {
		req, _ := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String(), nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = forceShutdown(ctx, logger.Nop(), "test", srv)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("request context was not cancelled after the shutdown timeout")
	}
}

func TestForceShutdownGraceful(t *testing.T) {
	srv := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.NotFoundHandler()}
	ln, err := listen(context.Background(), "test", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = serve(logger.Nop(), "test", srv, ln) }()
	assert.NoError(t, forceShutdown(context.Background(), logger.Nop(), "test", srv))
}
