package auth_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/platform/auth"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
	"github.com/Oxyrus/orderbook/internal/testutil/oauthtest"
)

func newVerifier(t *testing.T) (*oauthtest.Issuer, *auth.JWTVerifier) {
	t.Helper()
	iss := oauthtest.NewIssuer(t)
	v, err := auth.NewJWTVerifier(context.Background(), iss.AuthConfig())
	require.NoError(t, err)
	return iss, v
}

func TestVerify(t *testing.T) {
	iss, v := newVerifier(t)
	ctx := context.Background()

	t.Run("valid token", func(t *testing.T) {
		tok := iss.Token(t, oauthtest.Claims{Subject: "alice", Scope: "items:read items:write", Extra: map[string]any{"azp": "web-app"}})
		p, err := v.Verify(ctx, tok)
		require.NoError(t, err)
		assert.Equal(t, "alice", p.Subject)
		assert.Equal(t, "web-app", p.ClientID)
		assert.Equal(t, []string{"items:read", "items:write"}, p.Scopes)
		assert.WithinDuration(t, time.Now().Add(time.Hour), p.ExpiresAt, time.Minute)
	})

	rejects := map[string]string{
		"expired":        iss.Token(t, oauthtest.Claims{TTL: -time.Minute}),
		"wrong audience": iss.Token(t, oauthtest.Claims{Audience: []string{"other-api"}}),
		"wrong issuer":   iss.Token(t, oauthtest.Claims{Issuer: "https://evil.example"}),
		"bad signature":  iss.ForeignToken(t),
		"garbage":        "not.a.jwt",
	}
	for name, tok := range rejects {
		t.Run(name, func(t *testing.T) {
			_, err := v.Verify(ctx, tok)
			require.ErrorIs(t, err, auth.ErrInvalidToken)
		})
	}
}

func TestRequireATJWT(t *testing.T) {
	iss := oauthtest.NewIssuer(t)
	ctx := context.Background()
	cfg := iss.AuthConfig()

	lenient, err := auth.NewJWTVerifier(ctx, cfg)
	require.NoError(t, err)
	cfg.RequireATJWT = true
	strict, err := auth.NewJWTVerifier(ctx, cfg)
	require.NoError(t, err)

	for _, typ := range []string{"at+jwt", "application/at+jwt", "AT+JWT"} {
		_, err := strict.Verify(ctx, iss.Token(t, oauthtest.Claims{Type: typ}))
		require.NoError(t, err, typ)
	}

	idToken := iss.Token(t, oauthtest.Claims{Type: "JWT"})
	_, err = strict.Verify(ctx, idToken)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
	assert.Contains(t, err.Error(), "typ")

	_, err = lenient.Verify(ctx, idToken)
	require.NoError(t, err, "typ is not checked unless AUTH_REQUIRE_AT_JWT is set")
}

func TestScopeFormats(t *testing.T) {
	iss, v := newVerifier(t)
	tests := map[string]struct {
		extra map[string]any
		want  []string
	}{
		"scp array (Okta/Entra)":      {map[string]any{"scp": []string{"b", "a"}}, []string{"a", "b"}},
		"scp string":                  {map[string]any{"scp": "a b"}, []string{"a", "b"}},
		"permissions (Auth0 RBAC)":    {map[string]any{"permissions": []string{"x:read"}}, []string{"x:read"}},
		"scope + permissions deduped": {map[string]any{"scope": "a", "permissions": []string{"a", "c"}}, []string{"a", "c"}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			p, err := v.Verify(context.Background(), iss.Token(t, oauthtest.Claims{Extra: tt.extra}))
			require.NoError(t, err)
			assert.Equal(t, tt.want, p.Scopes)
		})
	}
}

func TestMiddleware(t *testing.T) {
	iss, v := newVerifier(t)

	h := auth.Authenticate(v)(auth.RequireScopes("items:write")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := auth.PrincipalFrom(r.Context())
		require.True(t, ok)
		_, _ = w.Write([]byte(p.Subject))
	})))

	tests := []struct {
		name       string
		header     string
		wantStatus int
		wantChall  string
	}{
		{"missing header", "", 401, `Bearer realm="api"`},
		{"wrong scheme", "Basic dXNlcjpwYXNz", 401, `Bearer realm="api"`},
		{"invalid token", "Bearer nope", 401, `error="invalid_token"`},
		{"insufficient scope", "Bearer " + iss.Token(t, oauthtest.Claims{Scope: "items:read"}), 403, `error="insufficient_scope"`},
		{"ok", "Bearer " + iss.Token(t, oauthtest.Claims{Subject: "bob", Scope: "items:write"}), 200, ""},
		{"scheme is case-insensitive", "bearer " + iss.Token(t, oauthtest.Claims{Scope: "items:write"}), 200, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.header != "" {
				r.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantChall != "" {
				assert.Contains(t, rec.Header().Get("WWW-Authenticate"), tt.wantChall)
			}
		})
	}
}

func TestAccessLogIncludesIdentity(t *testing.T) {
	iss, v := newVerifier(t)
	var buf bytes.Buffer
	h := httpx.Logger(zerolog.New(&buf))(auth.Authenticate(v)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer "+iss.Token(t, oauthtest.Claims{Subject: "alice", Extra: map[string]any{"azp": "web-app"}}))
	h.ServeHTTP(httptest.NewRecorder(), r)

	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line), buf.String())
	assert.Equal(t, "http request", line["message"])
	assert.Equal(t, "alice", line["sub"])
	assert.Equal(t, "web-app", line["client_id"])
}

func TestDiscoveryFailure(t *testing.T) {
	cfg := oauthtest.NewIssuer(t).AuthConfig()
	cfg.Issuer = "http://127.0.0.1:1" // nothing listens here
	_, err := auth.NewJWTVerifier(context.Background(), cfg)
	require.Error(t, err)
}

func TestWildcardScope(t *testing.T) {
	assert.True(t, auth.DevPrincipal.HasScope("anything"))
	p := &auth.Principal{Scopes: []string{"a"}}
	assert.True(t, p.HasScope("a"))
	assert.False(t, p.HasScope("b"))
}
