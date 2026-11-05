package httpx

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Oxyrus/orderbook/internal/platform/requestid"
)

// HeaderRequestID is the header used to propagate request IDs.
const HeaderRequestID = "X-Request-ID"

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._:\-]{1,128}$`)

// RequestIDFromContext returns the request ID stored by RequestID, if any.
func RequestIDFromContext(ctx context.Context) string { return requestid.FromContext(ctx) }

// RequestID reuses a well-formed incoming X-Request-ID header or generates a
// new UUIDv7, stores it in the context and echoes it in the response.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderRequestID)
		if !validRequestID.MatchString(id) {
			id = newID()
		}
		w.Header().Set(HeaderRequestID, id)
		ctx := requestid.WithContext(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func newID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}

// accessFields collects fields that inner middleware and handlers add to the
// access-log line (see AddAccessLogField). The access log is written by
// Logger after the request completes, with the logger Logger created, so
// fields added to a derived context logger would otherwise be lost.
type accessFields struct {
	mu     sync.Mutex
	fields [][2]string
}

type accessFieldsKey struct{}

// AddAccessLogField adds key=value to the access-log line of the current
// request. Middleware that learns something about the caller (e.g. the
// authenticated subject) uses it so the one-line-per-request log carries it.
// It is a no-op outside a request handled by Logger.
func AddAccessLogField(ctx context.Context, key, value string) {
	af, ok := ctx.Value(accessFieldsKey{}).(*accessFields)
	if !ok {
		return
	}
	af.mu.Lock()
	defer af.mu.Unlock()
	af.fields = append(af.fields, [2]string{key, value})
}

// Logger attaches a request-scoped zerolog logger to the context (enriched
// with request_id and, when tracing is active, trace_id/span_id) and writes
// one access-log line per request after it completes, including any fields
// added with AddAccessLogField.
func Logger(base zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			lc := base.With().Str("request_id", RequestIDFromContext(r.Context()))
			if sc := trace.SpanContextFromContext(r.Context()); sc.IsValid() {
				lc = lc.Str("trace_id", sc.TraceID().String()).Str("span_id", sc.SpanID().String())
			}
			log := lc.Logger()

			af := &accessFields{}
			ctx := context.WithValue(log.WithContext(r.Context()), accessFieldsKey{}, af)
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r.WithContext(ctx))

			status := ww.Status()
			if status == 0 {
				status = http.StatusOK
			}
			var ev *zerolog.Event
			switch {
			case status >= 500:
				ev = log.Error()
			case status >= 400:
				ev = log.Warn()
			default:
				ev = log.Info()
			}
			af.mu.Lock()
			for _, f := range af.fields {
				ev.Str(f[0], f[1])
			}
			af.mu.Unlock()
			ev.Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("route", routePattern(r)).
				Int("status", status).
				Int("bytes", ww.BytesWritten()).
				Dur("duration_ms", time.Since(start)).
				Str("client_ip", clientIP(r)).
				Str("user_agent", r.UserAgent()).
				Msg("http request")
		})
	}
}

// Recoverer converts panics into 500 problem responses and logs the stack.
// http.ErrAbortHandler is re-panicked as the net/http contract requires.
func Recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if rec == http.ErrAbortHandler { //nolint:errorlint // sentinel comparison required by net/http
				panic(rec)
			}
			zerolog.Ctx(r.Context()).Error().
				Interface("panic", rec).
				Bytes("stack", debug.Stack()).
				Msg("panic recovered")
			WriteError(w, r, Internal(fmt.Errorf("panic: %v", rec)))
		}()
		next.ServeHTTP(w, r)
	})
}

// SecureHeaders sets conservative security headers suitable for JSON APIs.
func SecureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		h.Set("Cache-Control", "no-store")
		if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
			h.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		}
		next.ServeHTTP(w, r)
	})
}

// MaxBodyBytes limits the size of request bodies.
func MaxBodyBytes(n int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, n)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Timeout sets a deadline on the request context. Handlers and the database
// driver honour it, so slow requests are cancelled instead of piling up.
func Timeout(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// Metrics records RED metrics (rate, errors, duration) per route pattern.
// Using the route pattern (e.g. /v1/items/{id}) instead of the raw path keeps
// label cardinality bounded.
type Metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight prometheus.Gauge
	size     *prometheus.HistogramVec
}

// NewMetrics registers HTTP metrics with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests by method, route and status code.",
		}, []string{"method", "route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by method, route and status code.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"method", "route", "status"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Number of HTTP requests currently being served.",
		}),
		size: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_response_size_bytes",
			Help:    "HTTP response size by method and route.",
			Buckets: prometheus.ExponentialBuckets(100, 10, 7),
		}, []string{"method", "route"}),
	}
	reg.MustRegister(m.requests, m.duration, m.inFlight, m.size)
	return m
}

// Middleware returns the instrumentation middleware.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		m.inFlight.Inc()
		defer m.inFlight.Dec()

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		status := ww.Status()
		if status == 0 {
			status = http.StatusOK
		}
		route := routePattern(r)
		code := strconv.Itoa(status)
		m.requests.WithLabelValues(r.Method, route, code).Inc()
		m.duration.WithLabelValues(r.Method, route, code).Observe(time.Since(start).Seconds())
		m.size.WithLabelValues(r.Method, route).Observe(float64(ww.BytesWritten()))
	})
}

// routePattern returns the matched chi route pattern, or a constant for
// unmatched routes so 404 scans cannot explode metric cardinality.
func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if p := rctx.RoutePattern(); p != "" {
			return p
		}
	}
	return "unmatched"
}

// TraceRoute renames the active OpenTelemetry server span to
// "METHOD /route/{pattern}" once routing has resolved the pattern, and tags
// it with http.route. Mount it inside the router.
func TraceRoute(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		span := trace.SpanFromContext(r.Context())
		if !span.IsRecording() {
			return
		}
		route := routePattern(r)
		span.SetName(r.Method + " " + route)
		span.SetAttributes(attribute.String("http.route", route))
	})
}

// clientIP returns the IP resolved by chi's ClientIPFrom* middleware, falling
// back to the TCP peer address.
func clientIP(r *http.Request) string {
	if ip := middleware.GetClientIP(r.Context()); ip != "" {
		return ip
	}
	return r.RemoteAddr
}
