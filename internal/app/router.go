package app

import (
	"net/http"
	"net/http/pprof"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/httprate"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/Oxyrus/orderbook/internal/buildinfo"
	"github.com/Oxyrus/orderbook/internal/catalog"
	"github.com/Oxyrus/orderbook/internal/config"
	"github.com/Oxyrus/orderbook/internal/health"
	"github.com/Oxyrus/orderbook/internal/order"
	"github.com/Oxyrus/orderbook/internal/platform/auth"
	"github.com/Oxyrus/orderbook/internal/platform/httpx"
	"github.com/Oxyrus/orderbook/internal/platform/tenant"
	"github.com/Oxyrus/orderbook/internal/sales"
)

// RouterDeps are the dependencies of the public API router. Tests build it
// with fakes; production wiring lives in app.go.
type RouterDeps struct {
	Config   config.Config
	Logger   zerolog.Logger
	Registry prometheus.Registerer
	Health   *health.Checker
	// Auth is the authentication middleware (auth.Authenticate or auth.Static).
	Auth    func(http.Handler) http.Handler
	Catalog *catalog.Handler
	Orders  *order.Handler
	Sales   *sales.Handler
}

// NewRouter builds the public API handler.
//
// Middleware order matters (outermost first):
//
//	otelhttp      start/continue trace span (wraps the router)
//	RequestID     correlation id
//	ClientIP      resolve the real client IP per HTTP_CLIENT_IP_SOURCE
//	Logger        request-scoped logger + access log
//	Metrics       RED metrics per route
//	TraceRoute    name span after the matched route
//	Recoverer     panic -> 500 (inside Logger/Metrics so they see the 500)
//	SecureHeaders, CORS, MaxBodyBytes, Timeout
//	/v1: RateLimit -> Auth -> Tenant -> RequireScopes (per route)
//	/v1/platform: RateLimit -> Auth -> RequireScopes (cross-tenant, no tenant)
func NewRouter(d RouterDeps) http.Handler {
	r := chi.NewRouter()
	cfg := d.Config

	r.Use(httpx.RequestID)
	r.Use(clientIPMiddleware(cfg.HTTP.ClientIP))
	r.Use(httpx.Logger(d.Logger))
	r.Use(httpx.NewMetrics(d.Registry).Middleware)
	r.Use(httpx.TraceRoute)
	r.Use(httpx.Recoverer)
	r.Use(httpx.SecureHeaders)
	if len(cfg.CORS.AllowedOrigins) > 0 {
		r.Use(cors.New(cors.Options{
			AllowedOrigins:   cfg.CORS.AllowedOrigins,
			AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete},
			AllowedHeaders:   []string{"Authorization", "Content-Type", "If-Match", "If-None-Match", httpx.HeaderRequestID},
			ExposedHeaders:   []string{"ETag", "Location", httpx.HeaderRequestID},
			AllowCredentials: cfg.CORS.AllowCredentials,
			MaxAge:           600,
		}).Handler)
	}
	r.Use(httpx.MaxBodyBytes(cfg.HTTP.MaxBodyBytes))
	r.Use(httpx.Timeout(cfg.HTTP.RequestTimeout))

	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.NotFound("route_not_found", "No route matches the request."))
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, &httpx.Error{Status: http.StatusMethodNotAllowed, Code: "method_not_allowed", Detail: "Method not allowed for this route."})
	})

	// Unauthenticated probes for load balancers that can only reach the
	// public port. They report the status only; the admin port serves the
	// detailed versions.
	r.Get("/livez", d.Health.PublicLive)
	r.Get("/readyz", d.Health.PublicReady)

	r.Route("/v1", func(r chi.Router) {
		if cfg.RateLimit.Enabled {
			r.Use(httprate.LimitBy(cfg.RateLimit.Requests, cfg.RateLimit.Window,
				func(r *http.Request) (string, error) {
					// IPv6 addresses are bucketed by /64.
					return httprate.CanonicalizeIP(middleware.GetClientIP(r.Context())), nil
				},
				httprate.WithLimitHandler(func(w http.ResponseWriter, r *http.Request) {
					httpx.WriteError(w, r, &httpx.Error{Status: http.StatusTooManyRequests, Code: "rate_limited", Detail: "Too many requests, slow down."})
				}),
			))
		}
		r.Use(d.Auth)

		r.Get("/me", httpx.Handle(me(cfg.Tenancy.Claim)))

		// Tenant-scoped API: every handler acts on the caller's tenant,
		// which is also the shard key.
		r.Group(func(r chi.Router) {
			r.Use(tenant.Middleware(cfg.Tenancy.Claim))
			d.Catalog.Routes(r)
			d.Orders.Routes(r)
			d.Sales.Routes(r)
			// Mount additional tenant-scoped feature handlers here.
		})

		// Platform (operator) API: cross-tenant, multi-shard queries.
		r.Route("/platform", func(r chi.Router) {
			d.Sales.PlatformRoutes(r)
		})
	})

	return otelhttp.NewHandler(r, "http.server",
		otelhttp.WithFilter(func(r *http.Request) bool {
			// Do not trace probe traffic.
			return r.URL.Path != "/livez" && r.URL.Path != "/readyz"
		}),
	)
}

// clientIPMiddleware resolves the client IP according to the configured
// trust model. Read it with middleware.GetClientIP(ctx).
func clientIPMiddleware(c config.ClientIP) func(http.Handler) http.Handler {
	switch c.Source {
	case config.ClientIPHeader:
		return middleware.ClientIPFromHeader(c.Header, c.TrustedProxies...)
	case config.ClientIPXFF:
		return middleware.ClientIPFromXFF(c.TrustedProxies...)
	case config.ClientIPXFFCount:
		return middleware.ClientIPFromXFFTrustedProxies(c.ProxyCount)
	default:
		return middleware.ClientIPFromRemoteAddr
	}
}

// me returns the authenticated principal; handy for debugging tokens.
func me(tenantClaim string) httpx.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		p, _ := auth.PrincipalFrom(r.Context())
		t, _ := p.Claims[tenantClaim].(string)
		if p == auth.DevPrincipal {
			t = tenant.DevTenant
		}
		httpx.JSON(w, r, http.StatusOK, map[string]any{
			"sub":        p.Subject,
			"client_id":  p.ClientID,
			"tenant":     t,
			"scopes":     p.Scopes,
			"expires_at": p.ExpiresAt,
		})
		return nil
	}
}

// NewAdminRouter builds the internal admin handler: metrics, probes, build
// info and pprof. Never expose it publicly.
func NewAdminRouter(cfg config.Config, reg *prometheus.Registry, hc *health.Checker) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		Registry:          reg,
	}))
	r.Get("/livez", hc.Live)
	r.Get("/readyz", hc.Ready)
	r.Get("/version", func(w http.ResponseWriter, r *http.Request) {
		httpx.JSON(w, r, http.StatusOK, buildinfo.Get())
	})

	if cfg.Admin.EnablePprof {
		r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)
		r.Handle("/debug/pprof/{name}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pprof.Handler(chi.URLParam(r, "name")).ServeHTTP(w, r)
		}))
	}
	return r
}
