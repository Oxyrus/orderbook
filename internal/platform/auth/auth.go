// Package auth implements the OAuth 2.0 resource-server role: it validates
// JWT bearer access tokens (RFC 6750 / RFC 9068) issued by any standards
// compliant authorization server (Keycloak, Auth0, Okta, Entra ID, Cognito,
// Zitadel, Ory Hydra, ...) and enforces scopes.
//
// Signing keys are fetched from the issuer's JWKS endpoint and cached; key
// rotation is handled automatically (an unknown "kid" triggers a refresh,
// rate-limited so forged tokens can't flood the issuer; see keyset.go).
package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/Oxyrus/orderbook/internal/config"
)

// ErrInvalidToken is returned for any token that fails verification.
var ErrInvalidToken = errors.New("invalid access token")

// Principal is the authenticated caller.
type Principal struct {
	// Subject is the "sub" claim: the user (or client, for machine tokens).
	Subject string
	// ClientID is the OAuth client that obtained the token ("azp"/"client_id").
	ClientID string
	// Scopes granted to the token.
	Scopes []string
	// ExpiresAt is the token expiry.
	ExpiresAt time.Time
	// Claims holds all raw claims for application-specific needs.
	Claims map[string]any
}

// WildcardScope grants every scope. Only the development principal (auth
// disabled) carries it.
const WildcardScope = "*"

// HasScope reports whether the principal was granted scope.
func (p *Principal) HasScope(scope string) bool {
	return slices.Contains(p.Scopes, scope) || slices.Contains(p.Scopes, WildcardScope)
}

type principalKey struct{}

// WithPrincipal stores p in ctx.
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom returns the authenticated principal. ok is false on
// unauthenticated routes.
func PrincipalFrom(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(*Principal)
	return p, ok && p != nil
}

// Verifier validates raw bearer tokens.
type Verifier interface {
	Verify(ctx context.Context, rawToken string) (*Principal, error)
}

// JWTVerifier verifies JWT access tokens with go-oidc.
type JWTVerifier struct {
	v            *oidc.IDTokenVerifier
	requireATJWT bool
}

// NewJWTVerifier builds a verifier from configuration. ctx is only used for
// OIDC discovery at startup.
//
// When cfg.JWKSURL is empty, the issuer's discovery document is fetched once
// at startup to locate the JWKS endpoint. The JWKS itself is fetched lazily
// on the first token.
func NewJWTVerifier(ctx context.Context, cfg config.Auth) (*JWTVerifier, error) {
	jwksURL := cfg.JWKSURL
	if jwksURL == "" {
		provider, err := oidc.NewProvider(ctx, cfg.Issuer)
		if err != nil {
			return nil, fmt.Errorf("oidc discovery for %q: %w", cfg.Issuer, err)
		}
		var meta struct {
			JWKSURI string `json:"jwks_uri"`
		}
		if err := provider.Claims(&meta); err != nil || meta.JWKSURI == "" {
			return nil, fmt.Errorf("issuer %q has no jwks_uri", cfg.Issuer)
		}
		jwksURL = meta.JWKSURI
	}
	return NewJWTVerifierWithKeySet(cfg, newRemoteKeySet(jwksURL, cfg.SupportedAlgs)), nil
}

// NewJWTVerifierWithKeySet builds a verifier with a custom key set (handy for
// tests: oidc.StaticKeySet).
func NewJWTVerifierWithKeySet(cfg config.Auth, keySet oidc.KeySet) *JWTVerifier {
	return &JWTVerifier{
		v: oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{
			ClientID:             cfg.Audience, // checked against "aud"
			SupportedSigningAlgs: cfg.SupportedAlgs,
		}),
		requireATJWT: cfg.RequireATJWT,
	}
}

// Verify validates signature, issuer, audience and expiry and extracts the
// principal.
func (j *JWTVerifier) Verify(ctx context.Context, raw string) (*Principal, error) {
	tok, err := j.v.Verify(ctx, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	if j.requireATJWT {
		if typ := tokenType(raw); !isATJWT(typ) {
			return nil, fmt.Errorf("%w: typ header is %q, want \"at+jwt\"", ErrInvalidToken, typ)
		}
	}
	claims := map[string]any{}
	if err := tok.Claims(&claims); err != nil {
		return nil, fmt.Errorf("%w: decode claims: %w", ErrInvalidToken, err)
	}
	if tok.Subject == "" {
		return nil, fmt.Errorf("%w: missing sub claim", ErrInvalidToken)
	}
	return &Principal{
		Subject:   tok.Subject,
		ClientID:  firstString(claims, "client_id", "azp"),
		Scopes:    extractScopes(claims),
		ExpiresAt: tok.Expiry,
		Claims:    claims,
	}, nil
}

// tokenType returns the JOSE "typ" header of a compact JWT, or "" when it is
// absent or unreadable. Call it only after the signature has been verified.
func tokenType(raw string) string {
	seg, _, _ := strings.Cut(raw, ".")
	b, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		return ""
	}
	var h struct {
		Typ string `json:"typ"`
	}
	if json.Unmarshal(b, &h) != nil {
		return ""
	}
	return h.Typ
}

// isATJWT reports whether typ denotes an RFC 9068 access token. Media types
// compare case-insensitively and may omit the "application/" prefix
// (RFC 7515 §4.1.9).
func isATJWT(typ string) bool {
	return strings.EqualFold(typ, "at+jwt") || strings.EqualFold(typ, "application/at+jwt")
}

// extractScopes supports the common encodings:
//   - "scope": "a b c"            (RFC 8693 / RFC 9068, Keycloak, Auth0)
//   - "scp":   ["a","b"] or "a b" (Okta, Entra ID)
//   - "permissions": ["a","b"]    (Auth0 RBAC)
func extractScopes(claims map[string]any) []string {
	var out []string
	for _, key := range []string{"scope", "scp", "permissions"} {
		switch v := claims[key].(type) {
		case string:
			out = append(out, strings.Fields(v)...)
		case []any:
			for _, s := range v {
				if str, ok := s.(string); ok {
					out = append(out, str)
				}
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func firstString(claims map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := claims[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
