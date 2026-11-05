# Development

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Go | 1.27+ | Version set in `go.mod` |
| Docker + Compose v2 | recent | Local stack and integration tests |
| make | any | Optional, but every workflow below uses it |

Nothing else needs installing. Dev tools run pinned through Go:
`govulncheck` is a `tool` directive in `go.mod` (`go tool govulncheck`), and
golangci-lint and goose run through `go run …@version` from the Makefile.

## Daily workflow

```bash
make up          # postgres + mock OAuth2 (once per session)
make run         # API on :8080, admin on :9090, auto-migrates, console logs
make test        # fast unit tests while coding
make check       # tidy + lint + vuln + all tests before pushing
```

`make run` uses `go run`. To get automatic restarts on file changes, use
[air](https://github.com/air-verse/air) or [watchexec](https://github.com/watchexec/watchexec):
`watchexec -r -e go -- make run`.

## Local services

| Service | Address | Credentials |
|---|---|---|
| Citus coordinator | `localhost:5432` | `orderbook` / `orderbook`, db `orderbook` (`make psql`). The app connects here only |
| Citus workers | `localhost:5433`, `localhost:5434` | same (`make psql-worker n=1`). Shards live here |
| Kafka | `localhost:29092` (host), `kafka:9092` (containers) | no auth (`make events`, `make kafka-topics`) |
| Kafka Connect + Debezium | `http://localhost:8083` | REST API (`make cdc-register`, `make cdc-status`) |
| Redis | `localhost:6379` | no auth, db `0` (`make redis-cli`). Used when `REDIS_ENABLED=true` |
| Mock OAuth2 | `http://localhost:8081/default` | any client id/secret; the client id becomes the `tenant_id` claim (`make token client=acme`) |
| Prometheus | `http://localhost:9091` | `make up-all` |
| Grafana | `http://localhost:3000` | anonymous admin; Prometheus + Jaeger datasources pre-provisioned |
| Jaeger | `http://localhost:16686` | `make up-all` |

Prometheus scrapes both `api:9090` (API in compose) and
`host.docker.internal:9090` (API running through `make run`), so metrics
work either way. One of the two targets will always show as down; that's
expected.

## Demo data

`make seed` migrates and then creates demo tenants (`acme`, `globex`,
`initech`, `umbrella`, `hooli`, `soylent`, `wonka`, `stark`), each with one
currency, 25 products and 40 orders, about 10% of them cancelled. It goes
through the real catalog and order services, so every order writes its
outbox event. With the connectors and the worker running, `/v1/sales/daily`
fills up a second later. Re-running it reuses the products (by SKU) and adds
40 more orders per tenant. Options: `make seed orders=500 products=50
cancel=0.2 tenants=acme,globex` (or `go run ./cmd/api seed -h`).

To start from scratch: `make reset`, then `make up-app` (or `make up` +
`make run` + `make cdc-register` + `make run-worker`), then `make seed`.

## Calling the API

```bash
TOKEN=$(make -s token)                       # tenant "acme", all tenant scopes
curl -s localhost:8080/v1/orders -H "Authorization: Bearer $TOKEN" | jq

# Other tenants / scopes
GLOBEX=$(make -s token client=globex)
READONLY=$(make -s token client=acme scope="catalog:read orders:read sales:read")
OPS=$(make -s token client=ops scope=platform:read)   # no tenant; /v1/platform/* only
```

The Docker VM needs about 4 GB of memory for the whole stack. With Colima:
`colima start --memory 6 --cpu 4` (add `--vm-type vz --vz-rosetta` on Apple
Silicon to run the amd64-only Citus image much faster).

The worker processes events only after the outbox connectors exist. With
`make run`, register them once after the first migration: `make
cdc-register` (they survive restarts; `make reset` removes them).

`docs/openapi.yaml` can be imported into Postman, Insomnia, Bruno, or
Swagger UI:

```bash
docker run --rm -p 8082:8080 -e SWAGGER_JSON=/spec/openapi.yaml \
  -v $PWD/docs:/spec swaggerapi/swagger-ui
```

## Code style

- `make fmt` runs gofmt + goimports, grouping imports as std / third-party /
  local.
- `make lint` runs golangci-lint v2 with the config in `.golangci.yml`
  (standard linters plus gosec, errorlint, sqlclosecheck, rowserrcheck,
  bodyclose, noctx, revive, …).
- Wrap errors with context: `fmt.Errorf("doing x: %w", err)`.
- Pass `context.Context` as the first parameter through every layer that
  performs I/O.
- No package-level mutable state. Dependencies are passed explicitly from
  `internal/app`.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `ping database: … connection refused` | Postgres isn't running: `make up`. |
| `oidc discovery … connection refused` on startup | The mock OAuth2 server isn't running (`make up`), or `AUTH_ISSUER` is wrong. Set `AUTH_ENABLED=false` to prototype without auth. |
| 401 `invalid_token` with a fresh token | `iss`/`aud` mismatch. Decode the token (`make -s token \| cut -d. -f2 \| base64 -d`) and compare with `AUTH_ISSUER`/`AUTH_AUDIENCE`. Run with `LOG_LEVEL=debug` to see the reason. |
| 415 `unsupported_media_type` | Send `Content-Type: application/json` (`curl -d` defaults to form encoding). |
| Integration tests: `Cannot connect to the Docker daemon` | Start Docker. With Colima/Podman/Rancher, set `DOCKER_HOST` and possibly `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE`; see [testcontainers docs](https://golang.testcontainers.org/system_requirements/using_colima/). |
| Integration tests are slow the first time | The Postgres image is being pulled. Later runs reuse it. |
| `bind: address already in use` | Something else holds 8080/9090/5432. Change `HTTP_ADDR`/`ADMIN_ADDR` or stop it. |
| Want a clean database | `make reset && make up`. |
| Migration failed halfway in dev | Fix the SQL. Goose wraps each migration in a transaction, so it simply wasn't applied. Re-run. |
