// Package tenant resolves the tenant of a request and carries it through the
// context.
//
// Orderbook is multi-tenant and every tenant-owned table is sharded by
// tenant id (Citus, see docs/sharding.md). The tenant id is therefore both
// the isolation boundary and the shard key: every query a feature runs for a
// request must filter on it, which also lets Citus route the query to the
// single shard that holds the tenant's data.
//
// The tenant always comes from the verified access token, never from a
// header, path or body the caller controls.
package tenant

import (
	"context"
	"net/http"
	"regexp"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Oxyrus/orderbook/internal/platform/auth"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
)

// DevTenant is the tenant of auth.DevPrincipal (AUTH_ENABLED=false).
const DevTenant = "dev"

// The same rule is enforced by the CHECK constraint on tenants.id.
var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// Valid reports whether id is a well-formed tenant id.
func Valid(id string) bool { return idPattern.MatchString(id) }

type ctxKey struct{}

// WithID stores the tenant id in ctx.
func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext returns the tenant id stored in ctx.
func FromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(ctxKey{}).(string)
	return id, ok && id != ""
}

// MustFromContext returns the tenant id or panics. Use it in handlers that
// are only reachable through Middleware, where a missing tenant is a
// programming error.
func MustFromContext(ctx context.Context) string {
	id, ok := FromContext(ctx)
	if !ok {
		panic("tenant: handler mounted without tenant.Middleware")
	}
	return id
}

// Middleware reads the tenant id from the given access-token claim and
// stores it in the request context, the request logger, the access log and
// the active span. Requests without a valid tenant are rejected with 403:
// the token is valid, it just isn't allowed to act on tenant data (platform
// tokens, for instance). It must be mounted after auth.Authenticate.
func Middleware(claim string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, ok := auth.PrincipalFrom(r.Context())
			if !ok {
				httpx.WriteError(w, r, httpx.Unauthorized("Authentication required."))
				return
			}
			id := fromPrincipal(p, claim)
			switch {
			case id == "":
				httpx.WriteError(w, r, &httpx.Error{Status: http.StatusForbidden, Code: "tenant_required",
					Detail: "The access token does not carry a tenant."})
				return
			case !Valid(id):
				httpx.WriteError(w, r, &httpx.Error{Status: http.StatusForbidden, Code: "tenant_invalid",
					Detail: "The access token carries a malformed tenant id."})
				return
			}

			ctx := WithID(r.Context(), id)
			httpx.AddAccessLogField(ctx, "tenant", id)
			trace.SpanFromContext(ctx).SetAttributes(attribute.String("tenant.id", id))
			log := zerolog.Ctx(ctx).With().Str("tenant", id).Logger()
			next.ServeHTTP(w, r.WithContext(log.WithContext(ctx)))
		})
	}
}

func fromPrincipal(p *auth.Principal, claim string) string {
	if p == auth.DevPrincipal {
		return DevTenant
	}
	v, _ := p.Claims[claim].(string)
	return v
}
