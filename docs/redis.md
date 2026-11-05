# Redis

The service ships with an optional Redis client
([go-redis v9](https://github.com/redis/go-redis)), set up the same way as
the database: pooled connections, timeouts, OpenTelemetry spans, Prometheus
pool metrics, a readiness check, graceful close and real-Redis integration
tests. Nothing in the example feature uses it yet. It's there so the first
feature that needs Redis only has to write the feature code.

- Client setup: [`internal/platform/redisx/redisx.go`](../internal/platform/redisx/redisx.go)
- Typed cache-aside helper: [`internal/platform/redisx/cache.go`](../internal/platform/redisx/cache.go)
- Pool metrics: [`internal/platform/redisx/metrics.go`](../internal/platform/redisx/metrics.go)
- Test helper: [`internal/testutil/redistest`](../internal/testutil/redistest/redistest.go)

## When to use Redis (and when not to)

Good fits:

| Use | Why Redis |
|---|---|
| Caching expensive reads (aggregations, external API responses, permission lookups) | Shared across replicas, survives deploys, TTL built in |
| Counters shared across replicas (quotas, global rate limits) | Atomic `INCR` / `EXPIRE` |
| Short-lived state: idempotency keys, one-time tokens, dedup windows | `SET NX` with TTL |
| Coordination: leases / simple locks, leader election for cron-like jobs | `SET NX PX` |
| Lightweight queues / fan-out | Streams, pub/sub |

Not a fit:

- **Data you can't lose.** Postgres is the source of truth. Assume anything
  in Redis can disappear at any moment (eviction, failover, flush).
- **Caching cheap queries.** A primary-key lookup in Postgres takes about a
  millisecond. Caching it adds a network hop, invalidation bugs and stale
  reads for little gain. Measure first (`http_request_duration_seconds`,
  traces).
- **Per-process data.** If staleness per replica is fine and the data is
  small, an in-memory map with a TTL is simpler.

## Enabling it

Redis is **off by default** (`REDIS_ENABLED=false`), so services that don't
use it don't have to run it. Locally, `make up` starts Redis and
`.env.example` turns it on:

```bash
make up                  # postgres + redis + oauth2
REDIS_ENABLED=true make run
make redis-cli           # poke at it: KEYS *, TTL <key>, MONITOR
```

When it's enabled, startup fails fast if Redis is unreachable (same as
Postgres). Every setting is listed in
[configuration.md](configuration.md#redis).

## Using the client in a feature

`app.Run` creates one `*redis.Client` (`rdb`) and shares it. The client is
safe for concurrent use and manages its own pool. Never create a client per
request.

Depend on the narrowest interface you need, not the concrete client. That
keeps unit tests on fakes:

```go
// internal/project/project.go
type Counter interface {
    Incr(ctx context.Context, key string) (int64, error)
}
```

```go
// internal/project/counter.go
type RedisCounter struct{ rdb redis.Cmdable }

func NewRedisCounter(rdb redis.Cmdable) *RedisCounter { return &RedisCounter{rdb: rdb} }

func (c *RedisCounter) Incr(ctx context.Context, key string) (int64, error) {
    return c.rdb.Incr(ctx, "projects:views:"+key).Result()
}
```

Wire it in `internal/app/app.go`, next to the other features:

```go
projectSvc := project.NewService(projectRepo, db, project.NewRedisCounter(rdb))
```

`rdb` is `nil` when Redis is disabled. Once a feature needs Redis, make the
dependency explicit in `config.Validate`, so a misconfigured deploy fails at
startup instead of with a nil pointer at runtime:

```go
if !c.Redis.Enabled {
    errs = append(errs, errors.New("REDIS_ENABLED=true is required (projects use Redis)"))
}
```

`redis.Cmdable` is implemented by `*redis.Client`, `*redis.ClusterClient`,
pipelines and transactions (`TxPipeline`), so code written against it
doesn't change if you move to Redis Cluster later.

Use `redisx.IsNil(err)` to tell "key not found" apart from real errors:

```go
v, err := rdb.Get(ctx, key).Result()
switch {
case redisx.IsNil(err):
    // miss
case err != nil:
    return fmt.Errorf("get %s: %w", key, err)
}
```

## Caching with `redisx.Cache`

`redisx.Cache[T]` is a small typed helper for the cache-aside pattern. Values
are stored as JSON under `<namespace>:<key>`, and every entry has a TTL.

```go
type Service struct {
    repo  Repository
    tx    database.Transactor
    cache *redisx.Cache[Project]
}

func NewService(repo Repository, tx database.Transactor, rdb redis.Cmdable) *Service {
    return &Service{
        repo:  repo,
        tx:    tx,
        cache: redisx.NewCache[Project](rdb, "projects:v1", 5*time.Minute),
    }
}

func (s *Service) Get(ctx context.Context, owner string, id uuid.UUID) (*Project, error) {
    p, err := s.cache.GetOrLoad(ctx, owner+":"+id.String(), func(ctx context.Context) (Project, error) {
        p, err := s.repo.Get(ctx, owner, id)
        if err != nil {
            return Project{}, err
        }
        return *p, nil
    })
    if err != nil {
        return nil, err
    }
    return &p, nil
}
```

What `GetOrLoad` does for you:

- **Stampede protection.** When many requests miss the same key at the same
  time, they share one `load` call within a process (`singleflight`).
- **Graceful degradation.** Redis errors are logged as warnings and treated
  as misses, so a Redis outage makes requests slower instead of failing
  them. Call `Get`/`Set` directly if you need to see Redis errors.
- **Errors aren't cached.** If `load` fails (including `ErrNotFound`), the
  error is returned and nothing is stored.
- **Cancellation.** A caller that gives up stops waiting, but the shared load
  keeps running with the first caller's deadline, so the other waiters still
  get the result.

### Invalidation

Delete entries after the source of truth changes, and **after the
transaction commits**. Otherwise a concurrent reader can re-cache the old
row between your delete and the commit:

```go
func (s *Service) Update(ctx context.Context, owner string, id uuid.UUID, in UpdateInput) (*Project, error) {
    var out *Project
    err := s.tx.WithTx(ctx, func(ctx context.Context) error {
        var err error
        out, err = s.repo.Update(ctx, owner, id, in)
        return err
    })
    if err != nil {
        return nil, err
    }
    // After commit. A failed delete leaves a stale entry until the TTL ends,
    // so keep TTLs short enough that this is acceptable.
    if err := s.cache.Delete(ctx, owner+":"+id.String()); err != nil {
        logger.FromContext(ctx).Warn().Err(err).Msg("cache invalidation failed")
    }
    return out, nil
}
```

The TTL is your upper bound on staleness. Pick it per use case, and make sure
it's an acceptable worst case if an invalidation is missed.

Deleting after commit still leaves one race, inherent to cache-aside: a
reader that missed the cache **before** your update loads the old row, and
writes it to Redis **after** your delete. The stale entry then lives until
the TTL expires. It needs a read and a write on the same key interleaved
within a few milliseconds, so it is rare, but the TTL bounds it, not the
invalidation. If a stale read is unacceptable for some data, don't cache it,
or put a version in the key (e.g. `project:<id>:v<version>`) so updates
naturally miss old entries.

### Key naming

- `<feature>:<version>:<id...>`, e.g. `projects:v1:<owner>:<id>`.
- **Bump the version when the cached shape changes** (`v1` → `v2`). Old
  entries are then ignored and expire on their own, so a deploy never reads
  JSON it can't decode.
- Include the tenant/owner in the key whenever the data is owner-scoped.
  Cache keys need the same tenancy as your SQL `WHERE` clauses.
- Never put secrets or raw PII in key names. Keys show up in `MONITOR`,
  slow logs and metrics tools.

## Other patterns with the raw client

**Fixed-window counter / quota:**

```go
pipe := rdb.TxPipeline()
n := pipe.Incr(ctx, key)
pipe.ExpireNX(ctx, key, time.Minute) // NX: start the window on the first hit only
if _, err := pipe.Exec(ctx); err != nil {
    return err
}
if n.Val() > limit { /* reject */ }
```

**Idempotency key / dedup** (only the first request wins):

```go
ok, err := rdb.SetNX(ctx, "payments:idem:"+key, requestID, 24*time.Hour).Result()
if err != nil { return err }
if !ok { /* duplicate: return the stored result */ }
```

**Lease / lock** for "run this once across replicas" jobs: acquire with
`SET key token NX PX ttl`, and release only if you still hold it (compare
the token in a Lua script). Redis locks are leases, not mutexes: a GC pause
or network partition can let two holders overlap. If correctness depends on
mutual exclusion, use a Postgres advisory lock or row lock
(`SELECT ... FOR UPDATE`) instead.

## Readiness and failure modes

| Situation | Behaviour |
|---|---|
| Redis down at startup | The process exits with `ping redis: ...` (same as Postgres) |
| Redis down while running, `REDIS_READINESS_CHECK=false` (default) | Instances stay in rotation. `Cache.GetOrLoad` falls back to the source; raw calls return errors |
| Redis down while running, `REDIS_READINESS_CHECK=true` | `/readyz` returns 503 (the admin port shows `"redis":"fail: ..."`), and the load balancer stops sending traffic |
| Slow Redis | Commands fail after `REDIS_READ_TIMEOUT`/`REDIS_WRITE_TIMEOUT` (1s) or the request deadline, whichever comes first |
| Pool exhausted | Callers wait up to `REDIS_POOL_TIMEOUT` (2s), then get an error. `redis_pool_timeouts_total` goes up |

**The readiness check is off by default**, because Redis is usually a cache:
if a cache outage failed readiness, it would take the whole fleet out of the
load balancer, which is worse than serving slower uncached responses. Set
`REDIS_READINESS_CHECK=true` when the service can't do its job without Redis
(quotas, idempotency, sessions, locks).

## Observability

- **Traces.** With `OTEL_TRACES_ENABLED=true`, every command and pipeline
  becomes a child span of the request (`redisotel`). Command arguments are
  **not** recorded, so values never leak into traces.
- **Metrics.** `redis_pool_*` on `/metrics`; see
  [observability.md](observability.md#metrics).
- **Logs.** `Cache` logs Redis failures with the request-scoped logger, so
  the lines carry `request_id` and `trace_id`.

## Testing

**Unit tests:** depend on a small interface (see above) and use a fake,
like the repository fakes in `internal/order/service_test.go`.

**Integration tests:** `redistest` starts one real Redis container per test
package. Each test gets a unique key namespace that's cleaned up when the
test ends, so tests can run in parallel:

```go
//go:build integration

var rs *redistest.Server

func TestMain(m *testing.M) { redistest.Main(m, &rs) }

func TestProjectCache(t *testing.T) {
    t.Parallel()
    rdb := rs.NewClient(t)
    ns := redistest.Namespace(t, rdb)  // "test:3fa1c2"
    cache := redisx.NewCache[Project](rdb, ns, time.Minute)
    ...
}
```

If a package needs both Postgres and Redis, start both in `TestMain`:

```go
func TestMain(m *testing.M) {
    ctx := context.Background()
    var err error
    if pg, err = pgtest.Start(ctx); err != nil { log.Fatal(err) }
    if rs, err = redistest.Start(ctx); err != nil { log.Fatal(err) }
    code := m.Run()
    _ = rs.Stop(ctx)
    _ = pg.Stop(ctx)
    os.Exit(code)
}
```

Code that receives the namespace prefix (like `Cache`) isolates itself
automatically. Code that builds its own keys should take the prefix as a
constructor argument, so tests can pass `ns`.

Set `TEST_REDIS_URL=redis://localhost:6379/15` to reuse the compose Redis
instead of starting a container.

## Production

- **Use a managed service** (ElastiCache/MemoryDB, Memorystore, Azure Cache,
  Upstash, Redis Cloud) with replication and automatic failover.
- **TLS and auth:** `REDIS_URL=rediss://user:password@host:6380/0`. Keep it
  in a secret manager, like `DB_URL`.
- **Eviction policy:** for caches, `maxmemory-policy volatile-lru` or
  `allkeys-lru`. With the default `noeviction`, writes fail once memory is
  full. The local compose setup uses `volatile-lru` with 256 MB.
- **Connections:** `replicas × pool size` must fit the server's
  `maxclients` and your plan's connection limit. The default pool size is
  10 × `GOMAXPROCS` per instance, so set `REDIS_POOL_SIZE` explicitly on
  large machines.
- **Redis Cluster:** `redisx.Connect` returns a single-node client (which
  also works with a primary endpoint that fails over). For Cluster, build a
  `redis.NewClusterClient` in `redisx` instead. Feature code written
  against `redis.Cmdable` doesn't change, but multi-key commands need keys
  in the same hash slot (`{tenant}:...`).
- **Don't run `KEYS` or `FLUSHALL` in production code.** Use `SCAN`, and
  delete by known key.
