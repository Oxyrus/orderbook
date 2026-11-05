# Configuration

All configuration comes from **environment variables**
([12-factor](https://12factor.net/config)). There are no config files. Every
variable has a default that works with the local docker-compose stack, so
`go run ./cmd/api` works without any setup. The exception is `APP_ENV`, which
defaults to `production` so that a deployment that forgets it gets the strict
behaviour. Set `APP_ENV=development` locally (`.env.example` and `make run`
already do).

- Parsing: [`caarlos0/env`](https://github.com/caarlos0/env), defined in
  [`internal/config/config.go`](../internal/config/config.go).
- Validation: `Config.Validate()` runs at startup. Invalid configuration
  prints every problem at once and exits with code 1 before anything starts.
  Settings that are valid but likely wrong (see `Config.Warnings()`) are
  logged as warnings at startup.
- Durations use Go syntax: `500ms`, `10s`, `5m`, `1h`.
- Lists are comma-separated: `a,b,c`.
- Locally, `make` loads `.env` automatically (copy `.env.example`). `go run`
  does not, so export the variables or use `make run`.

## General

| Variable | Default | Description |
|---|---|---|
| `SERVICE_NAME` | `orderbook` | Name used in logs (`service`), metrics (`app_build_info`) and traces (`service.name`). |
| `APP_ENV` | `production` | `development`, `test` or `production`. In `production`, `AUTH_ENABLED=false` is rejected and risky settings are logged as warnings. The default fails closed: set `development` explicitly on your machine. |

## HTTP server (public API)

| Variable | Default | Description |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Listen address. |
| `HTTP_READ_TIMEOUT` | `10s` | Max time to read the entire request, including the body. |
| `HTTP_READ_HEADER_TIMEOUT` | `5s` | Max time to read request headers (Slowloris protection). |
| `HTTP_WRITE_TIMEOUT` | `30s` | Max time to write the response. Must be greater than `HTTP_REQUEST_TIMEOUT`. |
| `HTTP_IDLE_TIMEOUT` | `120s` | Keep-alive idle timeout. Keep it above your load balancer's idle timeout (e.g. AWS ALB: 60s). |
| `HTTP_REQUEST_TIMEOUT` | `20s` | Deadline put on each request's `context.Context`. DB queries and outbound calls are cancelled when it expires. |
| `HTTP_SHUTDOWN_DELAY` | `0s` | On SIGTERM, keep serving for this long while `/readyz` returns 503, so load balancers can deregister the instance. Use `5s` on Kubernetes. |
| `HTTP_SHUTDOWN_TIMEOUT` | `25s` | Time allowed for in-flight requests to finish after shutdown begins. `DELAY + TIMEOUT` must be under `HTTP_TERMINATION_GRACE_PERIOD`. |
| `HTTP_TERMINATION_GRACE_PERIOD` | `30s` | How long your platform waits after SIGTERM before killing the process: Kubernetes `terminationGracePeriodSeconds` (default 30s), ECS `stopTimeout`, Cloud Run 10s. Only used to validate that the shutdown settings fit; set it to match the platform. |
| `HTTP_MAX_BODY_BYTES` | `1048576` | Maximum request body size (1 MiB). Larger bodies get a 413. |

### Client IP

The client IP feeds the access log (`client_ip`) and the rate limiter.
Forwarding headers are **trivially spoofable**, so you have to state your
network topology explicitly. These settings use chi's spoof-safe
`ClientIPFrom*` middleware.

| Variable | Default | Description |
|---|---|---|
| `HTTP_CLIENT_IP_SOURCE` | `remote_addr` | `remote_addr`, `header`, `xff` or `xff_count`. See the table below. |
| `HTTP_CLIENT_IP_HEADER` | `X-Real-IP` | Header to read when `SOURCE=header`. |
| `HTTP_CLIENT_IP_TRUSTED_PROXIES` | (empty) | CIDRs of your proxies/load balancers. Required for `xff`; strongly recommended for `header`. |
| `HTTP_CLIENT_IP_PROXY_COUNT` | `1` | Number of trusted proxies when `SOURCE=xff_count`. |

| Topology | Setting |
|---|---|
| Exposed directly to the internet, no proxy | `remote_addr` |
| Behind nginx / Cloudflare that **overwrites** a single-IP header | `header` + `HTTP_CLIENT_IP_HEADER=CF-Connecting-IP` (or `X-Real-IP`) + the proxy CIDRs |
| Behind load balancers whose CIDRs you know (VPC range, CDN ranges) | `xff` + `HTTP_CLIENT_IP_TRUSTED_PROXIES=10.0.0.0/8` |
| Behind a fixed number of proxies with dynamic IPs (e.g. one AWS ALB) | `xff_count` + `HTTP_CLIENT_IP_PROXY_COUNT=1` |

Read the resolved IP in code with `middleware.GetClientIP(r.Context())`
(chi's `middleware` package).

> **Behind a proxy, the default is wrong.** With `remote_addr`, every request
> that arrives through a load balancer or ingress has the proxy's IP, so all
> clients share **one** rate-limit bucket (100 requests/min per replica by
> default) and the access log shows the proxy. The service logs a warning at
> startup when `APP_ENV=production`, rate limiting is on, and the source is
> `remote_addr`.

## Admin server

The admin server exposes `/metrics`, `/livez`, `/readyz`, `/version` and
`/debug/pprof/*`. **Never expose it publicly**: pprof can leak memory
contents and burn CPU.

| Variable | Default | Description |
|---|---|---|
| `ADMIN_ADDR` | `:9090` | Listen address. `api healthcheck` also reads this variable. |
| `ADMIN_ENABLE_PPROF` | `false` | Mount `net/http/pprof` handlers. Enabled by `.env.example`, `make run` and docker-compose for local use. In production, enable it only temporarily; a warning is logged when it is on. |

## Logging

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `info` | `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `panic`, `disabled`. 4xx responses are logged at `warn`; the reasons for client errors are logged at `debug`. |
| `LOG_FORMAT` | `json` | `json` (production, machine-parsable) or `console` (colored, human-friendly). |

## Database

| Variable | Default | Description |
|---|---|---|
| `DB_URL` | `postgres://orderbook:orderbook@localhost:5432/orderbook?sslmode=disable` | The **Citus coordinator** (the app never connects to workers). libpq URL or DSN. Use `sslmode=verify-full` in production. Any pgx parameter works here (e.g. `pool_max_conn_lifetime_jitter`, `application_name`). |
| `DB_MAX_CONNS` | `20` | Pool size. Keep `replicas × MAX_CONNS` below Postgres `max_connections` (minus headroom), or put PgBouncer in front. |
| `DB_MIN_CONNS` | `2` | Connections kept open while idle. |
| `DB_MAX_CONN_LIFETIME` | `1h` | Recycle connections periodically (helps with failovers and DNS changes). |
| `DB_MAX_CONN_IDLE_TIME` | `30m` | Close connections idle for longer than this. |
| `DB_HEALTH_CHECK_PERIOD` | `1m` | Background health check of idle connections. |
| `DB_CONNECT_TIMEOUT` | `5s` | Timeout for establishing a connection and for the startup ping. |
| `DB_AUTO_MIGRATE` | `false` | Apply pending migrations on startup. Convenient in dev. In production, prefer an explicit release step (see [deployment](deployment.md#running-migrations)). |

## Tenancy

| Variable | Default | Description |
|---|---|---|
| `TENANT_CLAIM` | `tenant_id` | Access-token claim holding the caller's tenant id. The tenant scopes every query and is the shard key. See [authentication.md](authentication.md#the-tenant-claim). |

## Kafka (event worker)

Used by `api worker` only; the API never connects to Kafka.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:29092` | Comma-separated seed brokers. |
| `KAFKA_CLIENT_ID` | `orderbook` | Client id in broker logs and quotas. |
| `KAFKA_GROUP_ID` | `orderbook-sales-projection` | Consumer group of the sales projection. Changing it starts the projection from the beginning of the topic. |
| `KAFKA_ORDER_EVENTS_TOPIC` | `orderbook.events.order` | Topic Debezium's outbox router publishes order events to. |
| `KAFKA_DLQ_TOPIC` | `orderbook.events.order.dlq` | Dead-letter topic for events that keep failing. |
| `KAFKA_MAX_ATTEMPTS` | `5` | Attempts per event (exponential backoff) before dead-lettering. Permanent errors skip retries. |
| `KAFKA_DIAL_TIMEOUT` | `10s` | Broker connection timeout. |
| `KAFKA_TLS` | `false` | Use TLS (managed Kafka: Confluent Cloud, MSK, Aiven, ...). |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`; used when `KAFKA_SASL_USER` is set. |
| `KAFKA_SASL_USER` / `KAFKA_SASL_PASSWORD` | | SASL credentials. Set both or neither. |

## Outbox housekeeping (event worker)

| Variable | Default | Description |
|---|---|---|
| `OUTBOX_RETENTION` | `24h` | Outbox rows older than this are deleted. Debezium reads the WAL, not the table, so this only bounds how far back you can inspect or manually replay. |
| `OUTBOX_PRUNE_INTERVAL` | `10m` | How often the worker prunes. |
| `OUTBOX_DEDUP_RETENTION` | `168h` | How long processed event ids are remembered for idempotency. Must exceed the longest redelivery delay (consumer outage, offset reset). |

## Redis

Optional. Disabled by default; see [redis.md](redis.md) for usage.

| Variable | Default | Description |
|---|---|---|
| `REDIS_ENABLED` | `false` | Connect to Redis at startup (fails fast if unreachable), register pool metrics, and add a readiness check. |
| `REDIS_URL` | `redis://localhost:6379/0` | `redis://` or `rediss://` (TLS) URL: `rediss://user:password@host:6380/0`. Credentials and the DB number come from the URL. |
| `REDIS_POOL_SIZE` | `0` | Maximum connections per instance. `0` means the go-redis default (10 × `GOMAXPROCS`). Keep `replicas × POOL_SIZE` under the server's `maxclients`. |
| `REDIS_MIN_IDLE_CONNS` | `2` | Connections kept open while idle. |
| `REDIS_POOL_TIMEOUT` | `2s` | How long a command waits for a free connection when the pool is exhausted. |
| `REDIS_CONN_MAX_IDLE_TIME` | `30m` | Close connections idle for longer than this. |
| `REDIS_CONN_MAX_LIFETIME` | `1h` | Recycle connections periodically (helps with failovers and DNS changes). |
| `REDIS_DIAL_TIMEOUT` | `5s` | Timeout for establishing a connection and for the startup ping. |
| `REDIS_READ_TIMEOUT` / `REDIS_WRITE_TIMEOUT` | `1s` | Socket timeouts per command. The request deadline also applies. |
| `REDIS_READINESS_CHECK` | `false` | Fail `/readyz` while Redis is unreachable. Off by default because a cache outage should cost latency, not availability. Turn it on when the service can't work without Redis (see [redis.md](redis.md#readiness-and-failure-modes)). |

## Authentication (OAuth 2.0)

| Variable | Default | Description |
|---|---|---|
| `AUTH_ENABLED` | `true` | If `false`, every request runs as `dev-user` with all scopes. Rejected when `APP_ENV=production`. |
| `AUTH_ISSUER` | `http://localhost:8081/default` | Expected `iss` claim. Its `/.well-known/openid-configuration` is fetched at startup to find the JWKS, unless `AUTH_JWKS_URL` is set. |
| `AUTH_JWKS_URL` | (empty) | Use this JWKS URL directly and skip discovery. Useful when the issuer URL in tokens is not reachable from the service (e.g. in Docker). |
| `AUTH_AUDIENCE` | `orderbook-api` | Expected `aud` claim, usually your API identifier at the identity provider. |
| `AUTH_SUPPORTED_ALGS` | `RS256,ES256` | Accepted JWS algorithms. `none` and HMAC are never accepted. |
| `AUTH_REQUIRE_AT_JWT` | `false` | Reject tokens whose JOSE `typ` header isn't `at+jwt` ([RFC 9068 §4](https://www.rfc-editor.org/rfc/rfc9068#section-4)), so ID tokens from the same issuer can't be used as access tokens. Enable it if your provider issues RFC 9068 tokens (e.g. Auth0, Zitadel, Ory Hydra with JWT access tokens). Keycloak, Okta and Entra ID send `typ: JWT` and would be rejected. |

See [authentication.md](authentication.md) for provider-specific values.

## Tracing (OpenTelemetry)

| Variable | Default | Description |
|---|---|---|
| `OTEL_TRACES_ENABLED` | `false` | Export traces via OTLP/HTTP. When disabled, incoming trace context is still propagated into logs. |
| `OTEL_TRACES_SAMPLE_RATIO` | `1.0` | Parent-based ratio sampler (0–1). Use something like `0.05` at high traffic. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` (SDK default) | Collector endpoint. Standard OTel SDK variable. |
| `OTEL_EXPORTER_OTLP_HEADERS` | (empty) | E.g. `x-honeycomb-team=KEY`. Standard OTel SDK variable. |
| `OTEL_RESOURCE_ATTRIBUTES` | (empty) | Extra resource attributes, e.g. `k8s.pod.name=...`. |

All other [standard `OTEL_*` exporter variables](https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/)
are honoured too.

## CORS

CORS is only mounted when `CORS_ALLOWED_ORIGINS` is set. Pure server-to-server
APIs don't need it.

| Variable | Default | Description |
|---|---|---|
| `CORS_ALLOWED_ORIGINS` | (empty) | Comma-separated origins, e.g. `https://app.example.com`. `*` allows any origin; combining it with `CORS_ALLOW_CREDENTIALS=true` is rejected at startup. |
| `CORS_ALLOW_CREDENTIALS` | `false` | Allow cookies/credentials. Bearer tokens don't need this. |

## Rate limiting

A sliding-window limiter applied per client IP to all `/v1` routes, before
authentication, so token-verification CPU is protected too.

| Variable | Default | Description |
|---|---|---|
| `RATE_LIMIT_ENABLED` | `true` | Toggle. |
| `RATE_LIMIT_REQUESTS` | `100` | Requests allowed per window per IP. |
| `RATE_LIMIT_WINDOW` | `1m` | Window length. |

Counters live in memory, so each replica limits separately. For a global
limit, pass `httprate.WithLimitCounter` a Redis-backed counter
([`go-chi/httprate-redis`](https://github.com/go-chi/httprate-redis)) in
`internal/app/router.go`, or enforce limits at your API gateway.

## Adding a setting

1. Add a field with `env:"NAME"` and `envDefault:"..."` to the right struct
   in `internal/config/config.go`, with a doc comment.
2. Add any cross-field rules to `Validate()`, plus a case in
   `config_test.go`.
3. Document it here and, if it's useful locally, in `.env.example`.
