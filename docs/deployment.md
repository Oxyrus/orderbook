# Deployment

## The container image

`Dockerfile` is a multi-stage build:

1. **build**: `golang:1.27-alpine`. Dependencies are downloaded in their own
   cached layer. A static binary is built (`CGO_ENABLED=0`, `-trimpath`,
   `-s -w`) with version info passed through `-ldflags`. BuildKit cache
   mounts make rebuilds fast, and `--platform=$BUILDPLATFORM` with
   `TARGETOS/TARGETARCH` cross-compiles natively (no QEMU) for
   multi-arch images.
2. **runtime**: `gcr.io/distroless/static-debian13:nonroot`. It has no
   shell or package manager, includes CA certificates and tzdata, and runs as
   UID 65532. The image is about **37 MB**.

```bash
make docker-build VERSION=v1.2.3
# multi-arch, pushed:
docker buildx build --platform linux/amd64,linux/arm64 \
  --build-arg VERSION=v1.2.3 --build-arg COMMIT=$(git rev-parse HEAD) \
  -t ghcr.io/you/orderbook:v1.2.3 --push .
```

There is no shell, so the Docker `HEALTHCHECK` uses the binary itself
(`api healthcheck` calls `/livez` on the admin port). Orchestrators like
Kubernetes ignore `HEALTHCHECK` and use their own probes (below).

Migrations are embedded in the binary, so the image needs no extra files.

## Running migrations

Pick one strategy and use it everywhere:

| Strategy | How | When |
|---|---|---|
| **Release step (recommended)** | Run `api migrate up` as a one-off job **before** rolling out new pods: a Kubernetes `Job` / Helm pre-upgrade hook / Argo CD PreSync hook / ECS one-off task / a CI step | Default. Failures block the deploy and are visible. |
| Init container | `initContainers: [{ args: ["migrate","up"] }]` in the Deployment | Simple setups. Every pod runs it; the advisory lock serialises them. |
| Auto-migrate | `DB_AUTO_MIGRATE=true` | Dev/preview environments. Simple, but a slow migration delays readiness for every pod. |

Whatever you choose, migrations must stay **backward compatible with the
previous release** during a rolling update. See
[database.md](database.md#writing-zero-downtime-migrations).

Migrations target the **Citus coordinator**; Citus propagates DDL to the
workers. Register (or update) the Debezium connectors after the migration
that creates the publications, and before traffic that should be captured.

Use a DB role for migrations that can run DDL, and a less privileged role
for the app (DML only) if your security posture requires it. Set `DB_URL`
separately for the job.

## Processes and dependencies

The same image runs two processes:

| Process | Command | Scales with | Needs |
|---|---|---|---|
| API | `api serve` (default) | HTTP traffic; stateless | Citus coordinator, IdP (JWKS), optional Redis |
| Event worker | `api worker` | Kafka partitions (≤ 12 replicas for `orderbook.events.order`) | Citus coordinator, Kafka |

Deploy them as separate Deployments: a Kafka incident then stops
projections but never order taking. The worker serves the same admin
endpoints (`/livez`, `/readyz`, `/metrics`) on `ADMIN_ADDR`. Use the same
probes as the API, minus the public port.

Infrastructure the service relies on, and the usual production choices:

| Component | Local (compose) | Production |
|---|---|---|
| Citus | coordinator + 2 workers, trust auth | Azure Cosmos DB for PostgreSQL, or self-hosted with an HA replica per node (Patroni / operator), PgBouncer in front of the coordinator. See [sharding.md](sharding.md#operations) |
| Kafka | single KRaft broker | Managed (Confluent Cloud, MSK, Aiven, Redpanda) or Strimzi; RF 3, `min.insync.replicas=2`, topics provisioned as code |
| Kafka Connect + Debezium | one worker, `register.sh` | Distributed Connect cluster (≥ 2 workers), e.g. Strimzi `KafkaConnect` + `KafkaConnector` resources (one connector per Citus worker), secrets via config providers. See [cdc.md](cdc.md#production-checklist) |

## Kubernetes

A minimal, production-shaped set of manifests:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orderbook
  labels: { app: orderbook }
spec:
  replicas: 3
  revisionHistoryLimit: 5
  strategy:
    rollingUpdate: { maxUnavailable: 0, maxSurge: 1 }
  selector:
    matchLabels: { app: orderbook }
  template:
    metadata:
      labels: { app: orderbook }
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      terminationGracePeriodSeconds: 30   # = HTTP_TERMINATION_GRACE_PERIOD; > HTTP_SHUTDOWN_DELAY + HTTP_SHUTDOWN_TIMEOUT
      securityContext:
        runAsNonRoot: true
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: api
          image: ghcr.io/you/orderbook:v1.2.3
          args: ["serve"]
          ports:
            - { name: http, containerPort: 8080 }
            - { name: admin, containerPort: 9090 }
          env:
            - { name: APP_ENV, value: production }
            - { name: LOG_LEVEL, value: info }
            - { name: HTTP_SHUTDOWN_DELAY, value: 5s }
            - { name: HTTP_SHUTDOWN_TIMEOUT, value: 20s }
            - { name: HTTP_TERMINATION_GRACE_PERIOD, value: 30s }
            - { name: HTTP_CLIENT_IP_SOURCE, value: xff }
            - { name: HTTP_CLIENT_IP_TRUSTED_PROXIES, value: "10.0.0.0/8" }
            - { name: AUTH_ISSUER, value: "https://idp.example.com/realms/prod" }
            - { name: AUTH_AUDIENCE, value: "orderbook-api" }
            - { name: OTEL_TRACES_ENABLED, value: "true" }
            - { name: OTEL_TRACES_SAMPLE_RATIO, value: "0.1" }
            - { name: OTEL_EXPORTER_OTLP_ENDPOINT, value: "http://otel-collector.observability:4318" }
            - name: DB_URL
              valueFrom: { secretKeyRef: { name: orderbook-db, key: url } }
            - name: GOMEMLIMIT          # let the GC respect the container limit
              valueFrom: { resourceFieldRef: { resource: limits.memory } }
          resources:
            requests: { cpu: 250m, memory: 128Mi }
            limits: { memory: 512Mi }
          # /livez answers as soon as the process starts (before migrations
          # and OIDC discovery), so slow initialisation never fails the
          # startup or liveness probe. /readyz stays 503 "starting" until the
          # service can take traffic.
          startupProbe:
            httpGet: { path: /livez, port: admin }
            periodSeconds: 2
            failureThreshold: 15
          livenessProbe:
            httpGet: { path: /livez, port: admin }
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet: { path: /readyz, port: admin }
            periodSeconds: 5
            failureThreshold: 2
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: ["ALL"] }
---
apiVersion: v1
kind: Service
metadata:
  name: orderbook
spec:
  selector: { app: orderbook }
  ports:
    - { name: http, port: 80, targetPort: http }
    - { name: admin, port: 9090, targetPort: admin }   # cluster-internal
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: orderbook
spec:
  minAvailable: 2
  selector:
    matchLabels: { app: orderbook }
---
apiVersion: batch/v1
kind: Job
metadata:
  name: orderbook-migrate-v1-2-3
  annotations:
    argocd.argoproj.io/hook: PreSync       # or helm.sh/hook: pre-upgrade,pre-install
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: migrate
          image: ghcr.io/you/orderbook:v1.2.3
          args: ["migrate", "up"]
          env:
            - name: DB_URL
              valueFrom: { secretKeyRef: { name: orderbook-db-migrator, key: url } }
            - { name: AUTH_ENABLED, value: "true" }
```

Notes:

- **Only route the `http` port through your Ingress.** Keep `admin`
  internal. Add a NetworkPolicy if your cluster supports it.
- **CPU limits** are deliberately unset (throttling hurts tail latency). Set
  one if your platform requires it. Go 1.25+ reads the cgroup CPU quota to
  size `GOMAXPROCS` automatically.
- **`GOMEMLIMIT`** from the memory limit stops the GC from letting the
  heap hit the OOM-killer.
- If you use the Prometheus Operator, replace the annotations with a
  `ServiceMonitor` that targets port `admin`.
- Add a `HorizontalPodAutoscaler` on CPU, or on
  `http_requests_in_flight` through a metrics adapter.

### Graceful shutdown on Kubernetes

When a pod is terminated, Kubernetes sends SIGTERM **and** removes the pod
from Service endpoints at the same time, but endpoint propagation takes a
few seconds. With `HTTP_SHUTDOWN_DELAY=5s`, the service keeps serving (with
`/readyz` returning 503) while load balancers catch up. Then it stops
accepting new connections and gives in-flight requests
`HTTP_SHUTDOWN_TIMEOUT` to finish. Set `HTTP_TERMINATION_GRACE_PERIOD` to
the pod's `terminationGracePeriodSeconds`; config validation rejects
`DELAY + TIMEOUT` that doesn't fit inside it.

## Other platforms

| Platform | Notes |
|---|---|
| **AWS ECS/Fargate** | Target group health check: `/readyz` on 8080 (or put admin on a second target group). `HTTP_CLIENT_IP_SOURCE=xff_count`, `PROXY_COUNT=1` behind an ALB. Set the ALB idle timeout below `HTTP_IDLE_TIMEOUT`. `stopTimeout` 30s. |
| **Google Cloud Run** | Set `HTTP_ADDR=:$PORT` (Cloud Run sets `PORT`). Only one port is exposed, so use the public `/livez` and `/readyz`. Cloud Run sends SIGTERM with 10s grace, so set `HTTP_TERMINATION_GRACE_PERIOD=10s` and `HTTP_SHUTDOWN_TIMEOUT=8s`. `xff_count`, `PROXY_COUNT=1`. |
| **Fly.io / Render / Railway** | Health check `/readyz` on the public port. Set `HTTP_CLIENT_IP_SOURCE=header` with the platform's client-IP header (`Fly-Client-IP`, …). |
| **Plain VM + systemd** | `make build`, copy `bin/api`, `Restart=always`, `KillSignal=SIGTERM`, `TimeoutStopSec=30`, put nginx in front with `HTTP_CLIENT_IP_SOURCE=header` and `TRUSTED_PROXIES=127.0.0.1/32`. |

## Production checklist

**Configuration**
- [ ] `APP_ENV=production`, `LOG_FORMAT=json`, `LOG_LEVEL=info`.
- [ ] `DB_URL` comes from a secret manager and uses `sslmode=verify-full`.
- [ ] `AUTH_ISSUER` / `AUTH_AUDIENCE` point at the production IdP. Tokens are short-lived.
- [ ] `HTTP_CLIENT_IP_SOURCE` matches the real network topology. Behind any proxy, the default `remote_addr` puts every client in one rate-limit bucket.
- [ ] No `config:` warnings in the startup logs.
- [ ] `CORS_ALLOWED_ORIGINS` is an explicit list (or unset for server-to-server APIs).
- [ ] Rate limits are sized, and enforced globally (gateway or Redis counter) if needed.

**Network**
- [ ] TLS terminates at the load balancer (or in front of the service).
- [ ] The admin port (9090) is not reachable from the internet. `ADMIN_ENABLE_PPROF` stays off unless you are actively profiling.
- [ ] The load balancer idle timeout is lower than `HTTP_IDLE_TIMEOUT`.

**Data**
- [ ] Migrations run as a release step against the coordinator, are backward compatible, and follow the [Citus rules](sharding.md#writing-migrations-for-citus).
- [ ] Every Citus node has an HA replica; backups are consistent across nodes.
- [ ] CDC: one Debezium connector per shard-holding node, slot lag alerting on every node, `max_slot_wal_keep_size` set, failover slots or a reconciliation runbook ([cdc.md](cdc.md#production-checklist)).
- [ ] The event worker runs as its own Deployment; DLQ alerting and a replay runbook exist.
- [ ] Backups and point-in-time recovery are configured and **restore-tested**.
- [ ] `replicas × DB_MAX_CONNS` fits within `max_connections` (or PgBouncer is in front).
- [ ] If Redis is enabled: managed with failover, `rediss://` + auth from a secret manager, an eviction policy set for caches, `replicas × REDIS_POOL_SIZE` within `maxclients`, and `REDIS_READINESS_CHECK=true` only if Redis is a hard dependency ([redis.md](redis.md#production)).

**Observability**
- [ ] Prometheus scrapes `:9090/metrics` of the API and the worker, and alerts are routed to on-call.
- [ ] Traces are exported with a sampling ratio that fits your budget.
- [ ] Logs are shipped to a central store and searchable by `request_id` and `trace_id`.

**Supply chain**
- [ ] CI runs lint, `govulncheck` and tests on every PR.
- [ ] Images are tagged immutably (version or commit), scanned (Trivy/Grype), and optionally signed (cosign).
- [ ] Dependabot or Renovate keeps Go modules, base images and Actions current.
