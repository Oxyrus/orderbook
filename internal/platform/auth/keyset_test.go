package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/testutil/oauthtest"
)

// fakeClock is a manually advanced clock for the key set.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestVerifier(t *testing.T) (*oauthtest.Issuer, *JWTVerifier, *fakeClock) {
	t.Helper()
	iss := oauthtest.NewIssuer(t)
	cfg := iss.AuthConfig()
	clock := &fakeClock{t: time.Now()}
	ks := newRemoteKeySet(iss.URL()+"/jwks", cfg.SupportedAlgs)
	ks.now = clock.Now
	return iss, NewJWTVerifierWithKeySet(cfg, ks), clock
}

func TestKeySetFetchesLazilyAndCaches(t *testing.T) {
	iss, v, _ := newTestVerifier(t)
	ctx := context.Background()
	assert.Equal(t, 0, iss.JWKSRequests(), "no fetch before the first token")

	for range 5 {
		_, err := v.Verify(ctx, iss.Token(t, oauthtest.Claims{}))
		require.NoError(t, err)
	}
	assert.Equal(t, 1, iss.JWKSRequests())
}

func TestKeySetBadSignatureWithKnownKIDDoesNotRefetch(t *testing.T) {
	iss, v, clock := newTestVerifier(t)
	ctx := context.Background()
	_, err := v.Verify(ctx, iss.Token(t, oauthtest.Claims{}))
	require.NoError(t, err)

	clock.Advance(time.Hour - time.Minute) // well past minRefresh, before maxAge
	for range 20 {
		_, err := v.Verify(ctx, iss.ForeignToken(t))
		require.ErrorIs(t, err, ErrInvalidToken)
	}
	assert.Equal(t, 1, iss.JWKSRequests(), "forged tokens for a known kid must not reach the issuer")
}

func TestKeySetUnknownKIDRefetchIsThrottled(t *testing.T) {
	iss, v, clock := newTestVerifier(t)
	ctx := context.Background()
	_, err := v.Verify(ctx, iss.Token(t, oauthtest.Claims{}))
	require.NoError(t, err)

	for range 20 {
		_, err := v.Verify(ctx, iss.UnknownKeyToken(t))
		require.ErrorIs(t, err, ErrInvalidToken)
	}
	assert.Equal(t, 1, iss.JWKSRequests(), "refetch within minRefresh is throttled")

	clock.Advance(jwksMinRefresh)
	for range 20 {
		_, err := v.Verify(ctx, iss.UnknownKeyToken(t))
		require.ErrorIs(t, err, ErrInvalidToken)
	}
	assert.Equal(t, 2, iss.JWKSRequests(), "at most one refetch per minRefresh")

	// Valid tokens keep working while refreshes are throttled.
	_, err = v.Verify(ctx, iss.Token(t, oauthtest.Claims{}))
	require.NoError(t, err)
}

func TestKeySetPicksUpRotatedKey(t *testing.T) {
	iss, v, clock := newTestVerifier(t)
	ctx := context.Background()
	_, err := v.Verify(ctx, iss.Token(t, oauthtest.Claims{}))
	require.NoError(t, err)

	iss.Rotate(t)
	clock.Advance(jwksMinRefresh)
	_, err = v.Verify(ctx, iss.Token(t, oauthtest.Claims{}))
	require.NoError(t, err, "a new kid triggers a refresh")
	assert.Equal(t, 2, iss.JWKSRequests())
}

func TestKeySetExpiresKeysAfterMaxAge(t *testing.T) {
	iss, v, clock := newTestVerifier(t)
	ctx := context.Background()
	oldToken := iss.Token(t, oauthtest.Claims{TTL: 3 * time.Hour})
	_, err := v.Verify(ctx, oldToken)
	require.NoError(t, err)

	// The issuer drops the old key. Once the cache is older than maxAge it is
	// refreshed and tokens signed with the removed key stop verifying.
	iss.Rotate(t)
	clock.Advance(jwksMaxAge)
	_, err = v.Verify(ctx, oldToken)
	require.ErrorIs(t, err, ErrInvalidToken)
	assert.Equal(t, 2, iss.JWKSRequests())
}

func TestKeySetConcurrentRefreshSharesOneRequest(t *testing.T) {
	iss, v, _ := newTestVerifier(t)
	tok := iss.Token(t, oauthtest.Claims{})

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			_, err := v.Verify(context.Background(), tok)
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	assert.Equal(t, 1, iss.JWKSRequests())
}

func TestKeySetFetchErrors(t *testing.T) {
	tests := map[string]http.HandlerFunc{
		"server error": func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
		"not json":     func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("<html>")) },
		"no sig keys": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"keys":[{"kty":"oct","k":"c2VjcmV0"}]}`))
		},
	}
	for name, h := range tests {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(h)
			t.Cleanup(srv.Close)
			iss := oauthtest.NewIssuer(t)
			v := NewJWTVerifierWithKeySet(iss.AuthConfig(), newRemoteKeySet(srv.URL, []string{"RS256"}))
			_, err := v.Verify(context.Background(), iss.Token(t, oauthtest.Claims{}))
			require.ErrorIs(t, err, ErrInvalidToken)
		})
	}
}
