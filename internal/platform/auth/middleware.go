package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/rs/zerolog"

	"github.com/Oxyrus/orderbook/internal/platform/httpx"
)

// Authenticate requires a valid "Authorization: Bearer <token>" header and
// stores the resulting Principal in the request context. Failures produce a
// 401 with an RFC 6750 WWW-Authenticate challenge.
func Authenticate(v Verifier) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := bearerToken(r)
			if !ok {
				w.Header().Set("WWW-Authenticate", `Bearer realm="api"`)
				httpx.WriteError(w, r, httpx.Unauthorized("Missing bearer token."))
				return
			}
			p, err := v.Verify(r.Context(), raw)
			if err != nil {
				w.Header().Set("WWW-Authenticate", `Bearer realm="api", error="invalid_token"`)
				e := httpx.Unauthorized("The access token is invalid or expired.")
				e.Err = err
				httpx.WriteError(w, r, e)
				return
			}

			next.ServeHTTP(w, r.WithContext(withIdentity(r.Context(), p)))
		})
	}
}

// Static always authenticates as p. It is used when AUTH_ENABLED=false in
// development and in handler unit tests. Never use it in production.
func Static(p *Principal) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(withIdentity(r.Context(), p)))
		})
	}
}

// DevPrincipal is injected when authentication is disabled.
// Its tenant is "dev" (see tenant.Middleware).
var DevPrincipal = &Principal{Subject: "dev-user", ClientID: "dev", Scopes: []string{WildcardScope}}

// RequireScopes rejects requests whose principal lacks any of the scopes
// with 403 insufficient_scope. Must be mounted after Authenticate.
func RequireScopes(scopes ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, ok := PrincipalFrom(r.Context())
			if !ok {
				httpx.WriteError(w, r, httpx.Unauthorized("Authentication required."))
				return
			}
			for _, s := range scopes {
				if !p.HasScope(s) {
					w.Header().Set("WWW-Authenticate",
						fmt.Sprintf(`Bearer realm="api", error="insufficient_scope", scope=%q`, strings.Join(scopes, " ")))
					httpx.WriteError(w, r, httpx.Forbidden(fmt.Sprintf("Missing required scope %q.", s)))
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// withIdentity stores p in ctx and records the caller identity in the request
// logger and in the access-log line.
func withIdentity(ctx context.Context, p *Principal) context.Context {
	ctx = WithPrincipal(ctx, p)
	log := zerolog.Ctx(ctx).With().Str("sub", p.Subject).Str("client_id", p.ClientID).Logger()
	httpx.AddAccessLogField(ctx, "sub", p.Subject)
	httpx.AddAccessLogField(ctx, "client_id", p.ClientID)
	return log.WithContext(ctx)
}

func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	scheme, token, ok := strings.Cut(h, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != ""
}
