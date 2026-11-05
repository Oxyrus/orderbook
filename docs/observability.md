# Observability

The service covers the three signals (logs, metrics, traces) and correlates
them: every log line of a request carries its `request_id` and `trace_id`.

## Logging

[zerolog](https://github.com/rs/zerolog) writes structured JSON with zero
allocations on the hot path. It is set up in `internal/platform/logger`.

### Output

Production (`LOG_FORMAT=json`), one access-log line per request:

```json
{"level":"info","service":"orderbook","env":"production","version":"v1.4.0",
 "request_id":"01a1a21a-53f7-7b98-9f88-c5856a6c1e52",
 "trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7",
 "sub":"alice","client_id":"web-app",
 "method":"POST","path":"/v1/orders","route":"/v1/orders","status":201,"bytes":191,
 "duration_ms":3.81,"client_ip":"203.0.113.7","user_agent":"curl/8.7.1",
 "time":"2026-11-03T15:49:56.859Z","message":"http request"}
```

Development (`LOG_FORMAT=console`) shows the same fields, colorised and
human-readable.

Access logs are written at `info` for 2xx/3xx, `warn` for 4xx and `error`
for 5xx. A 5xx also logs the internal error that caused it. Panics are
logged with a stack trace.

### Logging in your code

Always use the **request-scoped logger from the context**. It already holds
`request_id`, `trace_id` and `sub`:

```go
import "github.com/rs/zerolog"

func (s *Service) Approve(ctx context.Context, id uuid.UUID) error {
    log := zerolog.Ctx(ctx)
    log.Info().Stringer("order_id", id).Msg("cancelling order")
    ...
    log.Warn().Err(err).Int("attempt", n).Msg("payment provider slow")
}
```

`zerolog.Ctx` never returns nil. Outside a request it falls back to the root
logger (set as `zerolog.DefaultContextLogger` in `main`).

Guidelines:

- Log **events with fields**, not formatted strings: `.Str("order_id", id)`
  rather than `Msgf("order %s", id)`. Fields are indexable and cheap.
- Use `debug` for detail you only want while investigating, and turn it on
  with `LOG_LEVEL=debug`.
- Don't log secrets, tokens or full request bodies with personal data.
- Don't log *and* return the same error. Return it and let the edge (the
  error renderer) log it once.
- Add long-lived fields with a child logger:
  `l := zerolog.Ctx(ctx).With().Str("job", name).Logger()`.

## Metrics

Prometheus metrics are served at `http://<admin>:9090/metrics` (OpenMetrics
format supported). They use a dedicated registry, not the global default.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `http_requests_total` | counter | `method`, `route`, `status` | Request count (**R**ate, **E**rrors) |
| `http_request_duration_seconds` | histogram | `method`, `route`, `status` | Latency (**D**uration) |
| `http_requests_in_flight` | gauge | | Concurrent requests |
| `http_response_size_bytes` | histogram | `method`, `route` | Response sizes |
| `db_pool_acquired_conns`, `db_pool_idle_conns`, `db_pool_total_conns`, `db_pool_max_conns` | gauge | | Pool state |
| `db_pool_acquire_total`, `db_pool_empty_acquire_total`, `db_pool_acquire_duration_seconds_total`, … | counter | | Pool pressure. A rising `empty_acquire` means requests waited for a connection. |
| `redis_pool_total_conns`, `redis_pool_idle_conns`, `redis_pool_max_conns`, `redis_pool_pending_requests` | gauge | | Redis pool state (only when `REDIS_ENABLED=true`) |
| `redis_pool_hits_total`, `redis_pool_misses_total`, `redis_pool_timeouts_total`, `redis_pool_wait_total`, `redis_pool_wait_duration_seconds_total`, `redis_pool_stale_conns_total` | counter | | Redis pool pressure. Rising `timeouts` means commands failed waiting for a connection. |
| `app_build_info` | gauge (=1) | `service`, `version`, `commit` | Which version is running |
| `go_*`, `process_*` | various | | Runtime: goroutines, GC, memory, CPU, file descriptors |

`route` is the **route pattern** (`/v1/orders/{id}`), never the raw path, and
unmatched requests are labelled `unmatched`. This keeps label cardinality
bounded even under 404 scans.

### Useful PromQL

```promql
# Requests per second by route
sum by (route) (rate(http_requests_total[5m]))

# Error ratio (5xx)
sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))

# p95 / p99 latency per route
histogram_quantile(0.95, sum by (le, route) (rate(http_request_duration_seconds_bucket[5m])))

# DB pool utilisation
db_pool_acquired_conns / db_pool_max_conns

# Average time to get a DB connection
rate(db_pool_acquire_duration_seconds_total[5m]) / rate(db_pool_acquire_total[5m])

# Redis pool utilisation / connection-wait timeouts
redis_pool_total_conns / redis_pool_max_conns
rate(redis_pool_timeouts_total[5m])
```

### Alerts

`deploy/prometheus/alerts.yml` has starter rules: high 5xx ratio, high p99
latency, DB pool saturation and instance down. Tune thresholds to your SLOs.
If you use the Prometheus Operator, port them to a `PrometheusRule`.

### Custom business metrics

Register metrics in your feature with the registry passed in from `app`:

```go
type Metrics struct{ created prometheus.Counter }

func NewMetrics(reg prometheus.Registerer) *Metrics {
    m := &Metrics{created: prometheus.NewCounter(prometheus.CounterOpts{
        Name: "orders_placed_total", Help: "Orders placed.",
    })}
    reg.MustRegister(m.created)
    return m
}
```

Never use unbounded values (user IDs, order IDs, tenant IDs, raw paths) as label values.

### The event pipeline (CDC)

`api worker` exposes the same admin endpoints (`/metrics`, `/livez`,
`/readyz` checking Postgres and Kafka) on its own `ADMIN_ADDR`
(`:9092` locally):

```promql
# Events processed / retried / dead-lettered per second
sum by (outcome) (rate(kafka_consumer_messages_total[5m]))
# p99 handler latency
histogram_quantile(0.99, sum by (le) (rate(kafka_consumer_handle_duration_seconds_bucket[5m])))
# Anything in the DLQ needs a human
increase(kafka_consumer_messages_total{outcome="dead_lettered"}[15m]) > 0
```

Outside the service, monitor:

- **replication slot lag on every Citus worker**, which is critical: a stopped
  connector makes the worker retain WAL until the disk fills
  (`pg_replication_slots`, via postgres_exporter; `make cdc-status`
  locally);
- Kafka Connect connector and task state (REST API / JMX);
- consumer group lag (`make kafka-topics` locally; kminion, Burrow or
  your Kafka platform in production).

See [cdc.md](cdc.md#operations).

## Tracing

OpenTelemetry tracing is set up in `internal/platform/telemetry`. Enable it
with `OTEL_TRACES_ENABLED=true` and point `OTEL_EXPORTER_OTLP_ENDPOINT` at a
collector or a backend that speaks OTLP/HTTP (OpenTelemetry Collector,
Jaeger, Grafana Tempo, Honeycomb, Datadog Agent, New Relic, …).

You get, with no code changes:

- **A server span per request**, named after the route (`POST /v1/orders`)
  and tagged `http.route`. Probes are not traced.
- **A child span for every SQL operation**: `pool.acquire`, `BEGIN`,
  `INSERT`, `SELECT`, `COMMIT`, … with the statement attached.
- **W3C `traceparent`/`baggage` propagation**: if a caller sends a trace
  context, the request joins that trace. Outbound calls made with
  `auth.NewClient` (or any `otelhttp.NewTransport`) carry it onward.
- **Trace IDs in logs** (`trace_id`, `span_id`). Propagation runs even when
  export is disabled, so logs still correlate with upstream traces.

Sampling is parent-based. If the caller decided to sample, so do we.
Otherwise `OTEL_TRACES_SAMPLE_RATIO` applies.

Traces continue across the CDC hop: the outbox stores the request's W3C
`traceparent`, Debezium forwards it as a Kafka header, and the worker starts
its consumer span (`orderbook.events.order process`) as a child of it. One
trace shows the HTTP request, its SQL, and the asynchronous projection
update that followed.

### Custom spans

```go
import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("github.com/Oxyrus/orderbook/internal/order")

func (s *Service) Recalculate(ctx context.Context) error {
    ctx, span := tracer.Start(ctx, "order.Recalculate")
    defer span.End()
    span.SetAttributes(attribute.Int("batch.size", n))
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "recalculate failed")
    }
    ...
}
```

### Locally

```bash
make up-app            # API + Jaeger, tracing on
# or: make up-all && OTEL_TRACES_ENABLED=true make run
open http://localhost:16686   # Jaeger → service "orderbook"
```

## Health checks

| Endpoint | Purpose | Checks | Failure means |
|---|---|---|---|
| `GET /livez` | Liveness | None (the process answers HTTP) | Restart the container |
| `GET /readyz` | Readiness | All registered checks, run concurrently with a 2s timeout; also fails while starting and while draining | Stop routing traffic, but **don't restart** |

Both are served on the admin port (9090), which probes should use, and on
the public port for load balancers that can only reach 8080. The admin port
returns each check's result and the build info:

```json
{"status":"ok","checks":{"postgres":"ok"},"version":"v1.4.0","commit":"9f2c…"}
```

The public port returns the same status codes but only the overall status
(`{"status":"ok"}`, `"starting"`, `"unavailable"` or `"draining"`), because
check errors can contain hostnames, database users and database names.

The admin server starts **before** the service connects to its dependencies.
While it connects to Postgres and Redis, runs migrations and fetches the OIDC
discovery document, `/livez` returns 200 and `/readyz` returns
`503 {"status":"starting"}`. The public port opens only once initialisation
has finished. A slow migration or identity provider therefore delays
readiness instead of failing liveness and triggering a restart loop.

With `REDIS_ENABLED=true` and `REDIS_READINESS_CHECK=true`, a `redis` check
is registered too. Register more readiness checks in
`internal/app/app.go`:

```go
hc.Register("search", func(ctx context.Context) error { return searchClient.Ping(ctx) })
```

Only check **hard** dependencies. If the service can still serve useful
responses without a dependency, keep that dependency out of readiness.
Otherwise one flaky dependency takes the whole fleet out of rotation.

## Profiling

When `ADMIN_ENABLE_PPROF=true` (off by default; on in `.env.example`,
`make run` and docker-compose), `net/http/pprof` is served on the admin port:

```bash
go tool pprof http://localhost:9090/debug/pprof/profile?seconds=30   # CPU
go tool pprof http://localhost:9090/debug/pprof/heap                 # memory
go tool pprof http://localhost:9090/debug/pprof/goroutine            # goroutine leaks
curl -o trace.out http://localhost:9090/debug/pprof/trace?seconds=5 && go tool trace trace.out
```

In Kubernetes: `kubectl port-forward deploy/orderbook 9090` and then the
commands above.

## Build info

`GET :9090/version` and the `api version` command return the version, commit
and build time injected at build time (`make build`, `make docker-build` and
CI set them through `-ldflags`).
