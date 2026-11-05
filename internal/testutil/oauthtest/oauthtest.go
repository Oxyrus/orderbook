// Package oauthtest provides an in-process OAuth 2.0 / OIDC issuer for tests.
// It serves a real discovery document and JWKS over HTTP, so tests exercise
// the exact same verification path as production (discovery -> JWKS fetch
// -> signature/claims validation).
package oauthtest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/Oxyrus/orderbook/internal/config"
)

// DefaultAudience is the audience used by Issuer.Token unless overridden.
const DefaultAudience = "test-api"

// Issuer is a fake authorization server.
type Issuer struct {
	Server *httptest.Server

	mu         sync.RWMutex
	key        *rsa.PrivateKey
	kid        string
	rotations  int
	jwksServed atomic.Int64
	gate       chan struct{} // non-nil while discovery is held
}

// NewIssuer starts an issuer that is shut down when the test ends.
func NewIssuer(t testing.TB) *Issuer {
	t.Helper()
	iss := &Issuer{}
	iss.Rotate(t)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		iss.mu.RLock()
		gate := iss.gate
		iss.mu.RUnlock()
		if gate != nil {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		writeJSON(w, map[string]any{
			"issuer":                                iss.URL(),
			"jwks_uri":                              iss.URL() + "/jwks",
			"token_endpoint":                        iss.URL() + "/token",
			"authorization_endpoint":                iss.URL() + "/authorize",
			"id_token_signing_alg_values_supported": []string{"RS256"},
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
		})
	})
	mux.HandleFunc("GET /jwks", func(w http.ResponseWriter, r *http.Request) {
		iss.jwksServed.Add(1)
		key, kid := iss.current()
		writeJSON(w, jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
			Key: &key.PublicKey, KeyID: kid, Algorithm: string(jose.RS256), Use: "sig",
		}}})
	})
	iss.Server = httptest.NewServer(mux)
	t.Cleanup(iss.Server.Close)
	return iss
}

// Rotate replaces the signing key with a new one under a new key ID. The JWKS
// publishes only the new key from then on.
func (i *Issuer) Rotate(t testing.TB) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.rotations++
	i.key, i.kid = key, "test-key-"+strconv.Itoa(i.rotations)
}

// HoldDiscovery makes discovery requests block until release is called,
// simulating a slow identity provider. release is safe to call more than
// once and is called automatically when the test ends.
func (i *Issuer) HoldDiscovery(t testing.TB) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	i.mu.Lock()
	i.gate = gate
	i.mu.Unlock()
	var once sync.Once
	release = func() {
		once.Do(func() {
			i.mu.Lock()
			i.gate = nil
			i.mu.Unlock()
			close(gate)
		})
	}
	t.Cleanup(release)
	return release
}

// KeyID returns the ID of the current signing key.
func (i *Issuer) KeyID() string {
	_, kid := i.current()
	return kid
}

// JWKSRequests returns how many times the JWKS has been fetched.
func (i *Issuer) JWKSRequests() int { return int(i.jwksServed.Load()) }

func (i *Issuer) current() (*rsa.PrivateKey, string) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.key, i.kid
}

// URL returns the issuer identifier ("iss").
func (i *Issuer) URL() string { return i.Server.URL }

// AuthConfig returns a config.Auth pointing at this issuer.
func (i *Issuer) AuthConfig() config.Auth {
	return config.Auth{
		Enabled:       true,
		Issuer:        i.URL(),
		Audience:      DefaultAudience,
		SupportedAlgs: []string{"RS256"},
	}
}

// Claims customises a token. Zero values get sensible defaults.
type Claims struct {
	Subject  string
	Audience []string
	Scope    string
	Issuer   string
	TTL      time.Duration // negative => already expired
	// Type is the JOSE "typ" header. Defaults to "at+jwt" (RFC 9068).
	Type  string
	Extra map[string]any
}

// Token mints a signed access token.
func (i *Issuer) Token(t testing.TB, c Claims) string {
	t.Helper()
	if c.Subject == "" {
		c.Subject = "user-123"
	}
	if c.Audience == nil {
		c.Audience = []string{DefaultAudience}
	}
	if c.Issuer == "" {
		c.Issuer = i.URL()
	}
	if c.TTL == 0 {
		c.TTL = time.Hour
	}
	now := time.Now()
	std := jwt.Claims{
		Issuer:   c.Issuer,
		Subject:  c.Subject,
		Audience: jwt.Audience(c.Audience),
		IssuedAt: jwt.NewNumericDate(now.Add(-time.Minute)),
		Expiry:   jwt.NewNumericDate(now.Add(c.TTL)),
	}
	extra := map[string]any{}
	for k, v := range c.Extra {
		extra[k] = v
	}
	if c.Scope != "" {
		extra["scope"] = c.Scope
	}
	if c.Type == "" {
		c.Type = "at+jwt"
	}
	key, kid := i.current()
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: kid}},
		(&jose.SignerOptions{}).WithType(jose.ContentType(c.Type)),
	)
	if err != nil {
		t.Fatalf("create signer: %v", err)
	}
	raw, err := jwt.Signed(signer).Claims(std).Claims(extra).Serialize()
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return raw
}

// ForeignToken mints a token signed by a key the issuer does not publish but
// carrying the current key ID, i.e. a forged token that must fail signature
// verification.
func (i *Issuer) ForeignToken(t testing.TB) string {
	t.Helper()
	return i.foreignToken(t, i.KeyID())
}

// UnknownKeyToken mints a token signed by a key the issuer does not publish,
// under a key ID the issuer has never used.
func (i *Issuer) UnknownKeyToken(t testing.TB) string {
	t.Helper()
	return i.foreignToken(t, "unknown-key")
}

func (i *Issuer) foreignToken(t testing.TB, kid string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	s, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: kid}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jwt.Signed(s).Claims(jwt.Claims{
		Issuer: i.URL(), Subject: "attacker", Audience: jwt.Audience{DefaultAudience},
		Expiry: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
