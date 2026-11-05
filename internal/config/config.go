// Package config loads and validates the service configuration from the
// environment. Defaults work with the bundled docker-compose stack, except
// that APP_ENV defaults to production so that safety checks (e.g. refusing
// AUTH_ENABLED=false) fail closed when APP_ENV is forgotten in a deployment.
// Set APP_ENV=development locally (.env.example and `make run` do).
package config

import (
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Environment names recognised by the service.
const (
	EnvDevelopment = "development"
	EnvTest        = "test"
	EnvProduction  = "production"
)

// Config is the root configuration object.
type Config struct {
	ServiceName string `env:"SERVICE_NAME" envDefault:"orderbook"`
	// Environment defaults to production so a deployment that forgets to
	// set it gets the strict behaviour, not the permissive one.
	Environment string `env:"APP_ENV" envDefault:"production"`

	HTTP      HTTP      `envPrefix:"HTTP_"`
	Admin     Admin     `envPrefix:"ADMIN_"`
	Log       Log       `envPrefix:"LOG_"`
	DB        DB        `envPrefix:"DB_"`
	Redis     Redis     `envPrefix:"REDIS_"`
	Auth      Auth      `envPrefix:"AUTH_"`
	Telemetry Telemetry `envPrefix:"OTEL_"`
	CORS      CORS      `envPrefix:"CORS_"`
	RateLimit RateLimit `envPrefix:"RATE_LIMIT_"`
	Tenancy   Tenancy   `envPrefix:"TENANT_"`
	Kafka     Kafka     `envPrefix:"KAFKA_"`
	Outbox    Outbox    `envPrefix:"OUTBOX_"`
}

// HTTP configures the public API server.
type HTTP struct {
	Addr              string        `env:"ADDR" envDefault:":8080"`
	ReadTimeout       time.Duration `env:"READ_TIMEOUT" envDefault:"10s"`
	ReadHeaderTimeout time.Duration `env:"READ_HEADER_TIMEOUT" envDefault:"5s"`
	WriteTimeout      time.Duration `env:"WRITE_TIMEOUT" envDefault:"30s"`
	IdleTimeout       time.Duration `env:"IDLE_TIMEOUT" envDefault:"120s"`
	// RequestTimeout bounds the time spent in a handler (context deadline).
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"20s"`
	// ShutdownTimeout is how long in-flight requests get to finish on SIGTERM.
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"25s"`
	// TerminationGracePeriod is how long the platform waits after SIGTERM
	// before it kills the process (Kubernetes terminationGracePeriodSeconds,
	// ECS stopTimeout, ...). ShutdownDelay + ShutdownTimeout must fit in it.
	TerminationGracePeriod time.Duration `env:"TERMINATION_GRACE_PERIOD" envDefault:"30s"`
	// ShutdownDelay keeps serving (while /readyz reports "draining") before
	// shutdown begins, giving load balancers time to deregister the
	// instance. Use ~5s on Kubernetes.
	ShutdownDelay time.Duration `env:"SHUTDOWN_DELAY" envDefault:"0s"`
	// MaxBodyBytes caps request bodies.
	MaxBodyBytes int64 `env:"MAX_BODY_BYTES" envDefault:"1048576"`
	// ClientIP selects how the real client IP (used for logs and rate
	// limiting) is determined. See docs/configuration.md#client-ip.
	ClientIP ClientIP `envPrefix:"CLIENT_IP_"`
}

// Client IP sources.
const (
	ClientIPRemoteAddr = "remote_addr" // directly exposed, no proxy
	ClientIPHeader     = "header"      // proxy sets a single-IP header (X-Real-IP, CF-Connecting-IP)
	ClientIPXFF        = "xff"         // proxies with known CIDRs append to X-Forwarded-For
	ClientIPXFFCount   = "xff_count"   // fixed number of proxies with dynamic IPs
)

// ClientIP configures client IP resolution behind proxies. Never trust
// forwarding headers blindly: they are trivially spoofable.
type ClientIP struct {
	Source string `env:"SOURCE" envDefault:"remote_addr"`
	// Header is used when Source=header.
	Header string `env:"HEADER" envDefault:"X-Real-IP"`
	// TrustedProxies are CIDRs of your proxies/load balancers. Required for
	// Source=xff; optional (but recommended) for Source=header.
	TrustedProxies []string `env:"TRUSTED_PROXIES" envSeparator:","`
	// ProxyCount is the number of trusted proxies when Source=xff_count.
	ProxyCount int `env:"PROXY_COUNT" envDefault:"1"`
}

// Admin configures the internal server that exposes metrics, health and pprof.
// It must never be exposed publicly.
type Admin struct {
	Addr string `env:"ADDR" envDefault:":9090"`
	// EnablePprof mounts net/http/pprof. Off by default; enable it locally
	// or temporarily while debugging.
	EnablePprof bool `env:"ENABLE_PPROF" envDefault:"false"`
}

// Log configures structured logging.
type Log struct {
	Level string `env:"LEVEL" envDefault:"info"`
	// Format is "json" (production) or "console" (human friendly).
	Format string `env:"FORMAT" envDefault:"json"`
}

// DB configures the PostgreSQL connection pool.
type DB struct {
	URL               string        `env:"URL" envDefault:"postgres://orderbook:orderbook@localhost:5432/orderbook?sslmode=disable"`
	MaxConns          int32         `env:"MAX_CONNS" envDefault:"20"`
	MinConns          int32         `env:"MIN_CONNS" envDefault:"2"`
	MaxConnLifetime   time.Duration `env:"MAX_CONN_LIFETIME" envDefault:"1h"`
	MaxConnIdleTime   time.Duration `env:"MAX_CONN_IDLE_TIME" envDefault:"30m"`
	HealthCheckPeriod time.Duration `env:"HEALTH_CHECK_PERIOD" envDefault:"1m"`
	ConnectTimeout    time.Duration `env:"CONNECT_TIMEOUT" envDefault:"5s"`
	// AutoMigrate runs pending migrations on startup. Convenient for
	// development; prefer running `api migrate up` as a release step in
	// production.
	AutoMigrate bool `env:"AUTO_MIGRATE" envDefault:"false"`
}

// Redis configures the optional Redis client. It is disabled by default so
// services that don't need Redis don't have to run it. See docs/redis.md.
type Redis struct {
	Enabled bool `env:"ENABLED" envDefault:"false"`
	// URL uses the redis:// (plain) or rediss:// (TLS) scheme, e.g.
	// redis://user:password@host:6379/0.
	URL string `env:"URL" envDefault:"redis://localhost:6379/0"`
	// PoolSize is the maximum number of connections. 0 means go-redis'
	// default (10 per GOMAXPROCS).
	PoolSize        int           `env:"POOL_SIZE" envDefault:"0"`
	MinIdleConns    int           `env:"MIN_IDLE_CONNS" envDefault:"2"`
	PoolTimeout     time.Duration `env:"POOL_TIMEOUT" envDefault:"2s"`
	ConnMaxIdleTime time.Duration `env:"CONN_MAX_IDLE_TIME" envDefault:"30m"`
	ConnMaxLifetime time.Duration `env:"CONN_MAX_LIFETIME" envDefault:"1h"`
	DialTimeout     time.Duration `env:"DIAL_TIMEOUT" envDefault:"5s"`
	ReadTimeout     time.Duration `env:"READ_TIMEOUT" envDefault:"1s"`
	WriteTimeout    time.Duration `env:"WRITE_TIMEOUT" envDefault:"1s"`
	// ReadinessCheck makes /readyz fail while Redis is unreachable. Off by
	// default because the bundled use of Redis (redisx.Cache) is best-effort:
	// an outage should degrade latency, not take every instance out of the
	// load balancer. Turn it on when Redis is a hard dependency (sessions,
	// locks, queues).
	ReadinessCheck bool `env:"READINESS_CHECK" envDefault:"false"`
}

// Auth configures OAuth 2.0 bearer-token validation (resource server role).
type Auth struct {
	// Enabled toggles authentication. It can only be disabled outside of
	// production.
	Enabled bool `env:"ENABLED" envDefault:"true"`
	// Issuer is the OAuth 2.0 / OIDC issuer URL. Its discovery document
	// (/.well-known/openid-configuration) is used to locate the JWKS.
	Issuer string `env:"ISSUER" envDefault:"http://localhost:8081/default"`
	// JWKSURL skips discovery and uses this key set directly when set.
	JWKSURL string `env:"JWKS_URL"`
	// Audience is the expected "aud" claim of access tokens.
	Audience string `env:"AUDIENCE" envDefault:"orderbook-api"`
	// SupportedAlgs restricts accepted signing algorithms.
	SupportedAlgs []string `env:"SUPPORTED_ALGS" envDefault:"RS256,ES256" envSeparator:","`
	// RequireATJWT rejects tokens whose JOSE "typ" header is not "at+jwt"
	// (RFC 9068 §4), so ID tokens and other JWTs from the same issuer can't
	// be replayed as access tokens. Off by default because several providers
	// (Keycloak, Okta, Entra ID) still issue access tokens with typ "JWT".
	RequireATJWT bool `env:"REQUIRE_AT_JWT" envDefault:"false"`
}

// Telemetry configures OpenTelemetry tracing.
type Telemetry struct {
	// Enabled turns on trace export via OTLP/HTTP. Standard OTEL_EXPORTER_OTLP_*
	// variables are honoured by the exporter (endpoint, headers, ...).
	Enabled bool `env:"TRACES_ENABLED" envDefault:"false"`
	// SampleRatio is the parent-based trace sampling ratio (0..1).
	SampleRatio float64 `env:"TRACES_SAMPLE_RATIO" envDefault:"1.0"`
}

// CORS configures cross-origin resource sharing for browser clients.
type CORS struct {
	AllowedOrigins   []string `env:"ALLOWED_ORIGINS" envSeparator:","`
	AllowCredentials bool     `env:"ALLOW_CREDENTIALS" envDefault:"false"`
}

// RateLimit configures a per-client-IP token bucket.
type RateLimit struct {
	Enabled  bool          `env:"ENABLED" envDefault:"true"`
	Requests int           `env:"REQUESTS" envDefault:"100"`
	Window   time.Duration `env:"WINDOW" envDefault:"1m"`
}

// Tenancy configures how the tenant of a request is determined. Every
// tenant-owned table is sharded by tenant id (see docs/sharding.md), so the
// tenant must come from a trusted source: the verified access token.
type Tenancy struct {
	// Claim is the access-token claim holding the tenant id. Most identity
	// providers can add a custom claim (Auth0 actions, Keycloak mappers,
	// Entra ID optional claims, Cognito pre-token-generation triggers).
	Claim string `env:"CLAIM" envDefault:"tenant_id"`
}

// Kafka configures the client used by `api worker` to consume the domain
// events Debezium publishes.
type Kafka struct {
	Brokers []string `env:"BROKERS" envDefault:"localhost:29092" envSeparator:","`
	// ClientID identifies this process in broker logs and quotas.
	ClientID string `env:"CLIENT_ID" envDefault:"orderbook"`
	// GroupID is the consumer group of the sales projection.
	GroupID string `env:"GROUP_ID" envDefault:"orderbook-sales-projection"`
	// OrderEventsTopic is where Debezium's outbox event router publishes
	// events whose aggregatetype is "order".
	OrderEventsTopic string `env:"ORDER_EVENTS_TOPIC" envDefault:"orderbook.events.order"`
	// DLQTopic receives events that still fail after MaxAttempts, so one
	// poison message cannot block a partition forever.
	DLQTopic    string        `env:"DLQ_TOPIC" envDefault:"orderbook.events.order.dlq"`
	MaxAttempts int           `env:"MAX_ATTEMPTS" envDefault:"5"`
	DialTimeout time.Duration `env:"DIAL_TIMEOUT" envDefault:"10s"`
	// SASL/TLS for managed Kafka (Confluent Cloud, MSK, Aiven, Redpanda).
	TLS bool `env:"TLS" envDefault:"false"`
	// SASLMechanism is PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512. SASL is used
	// when SASLUser is set.
	SASLMechanism string `env:"SASL_MECHANISM" envDefault:"SCRAM-SHA-512"`
	SASLUser      string `env:"SASL_USER"`
	SASLPassword  string `env:"SASL_PASSWORD"`
}

// Outbox configures housekeeping of the transactional outbox, done by
// `api worker`. Debezium reads events from the WAL, not from the table, so
// rows can be deleted soon after commit; they are kept for a while only to
// help debugging and manual replays.
type Outbox struct {
	Retention     time.Duration `env:"RETENTION" envDefault:"24h"`
	PruneInterval time.Duration `env:"PRUNE_INTERVAL" envDefault:"10m"`
	// DedupRetention is how long processed event ids are remembered for
	// idempotent consumption. It must exceed the longest possible
	// redelivery delay (consumer outage + Kafka retention of the topic).
	DedupRetention time.Duration `env:"DEDUP_RETENTION" envDefault:"168h"`
}

// Load parses the environment into a Config and validates it.
func Load() (Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse env: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Defaults returns the configuration built from the defaults alone, ignoring
// the process environment, without validation. Tests use it so their results
// don't depend on the developer's shell or .env file (which the Makefile
// exports).
func Defaults() Config {
	cfg, err := env.ParseAsWithOptions[Config](env.Options{Environment: map[string]string{}})
	if err != nil {
		panic(fmt.Sprintf("config: invalid defaults: %v", err))
	}
	return cfg
}

// IsProduction reports whether the service runs in production mode.
func (c Config) IsProduction() bool { return c.Environment == EnvProduction }

// Validate checks semantic constraints that struct tags cannot express.
func (c Config) Validate() error {
	var errs []error
	switch c.Environment {
	case EnvDevelopment, EnvTest, EnvProduction:
	default:
		errs = append(errs, fmt.Errorf("APP_ENV must be one of development|test|production, got %q", c.Environment))
	}
	switch strings.ToLower(c.Log.Format) {
	case "json", "console":
	default:
		errs = append(errs, fmt.Errorf("LOG_FORMAT must be json or console, got %q", c.Log.Format))
	}
	if c.DB.URL == "" {
		errs = append(errs, errors.New("DB_URL is required"))
	}
	if c.DB.MinConns > c.DB.MaxConns {
		errs = append(errs, errors.New("DB_MIN_CONNS must be <= DB_MAX_CONNS"))
	}
	if c.Redis.Enabled {
		if c.Redis.URL == "" {
			errs = append(errs, errors.New("REDIS_URL is required when REDIS_ENABLED=true"))
		} else if !strings.HasPrefix(c.Redis.URL, "redis://") && !strings.HasPrefix(c.Redis.URL, "rediss://") {
			errs = append(errs, errors.New("REDIS_URL must start with redis:// or rediss://"))
		}
		if c.Redis.PoolSize < 0 || c.Redis.MinIdleConns < 0 {
			errs = append(errs, errors.New("REDIS_POOL_SIZE and REDIS_MIN_IDLE_CONNS must be >= 0"))
		}
		if c.Redis.PoolSize > 0 && c.Redis.MinIdleConns > c.Redis.PoolSize {
			errs = append(errs, errors.New("REDIS_MIN_IDLE_CONNS must be <= REDIS_POOL_SIZE"))
		}
	}
	if c.Auth.Enabled {
		if c.Auth.Issuer == "" {
			errs = append(errs, errors.New("AUTH_ISSUER is required when auth is enabled"))
		}
		if c.Auth.Audience == "" {
			errs = append(errs, errors.New("AUTH_AUDIENCE is required when auth is enabled"))
		}
	} else if c.IsProduction() {
		errs = append(errs, errors.New("AUTH_ENABLED=false is not allowed in production"))
	}
	switch c.HTTP.ClientIP.Source {
	case ClientIPRemoteAddr, ClientIPHeader:
	case ClientIPXFF:
		if len(c.HTTP.ClientIP.TrustedProxies) == 0 {
			errs = append(errs, errors.New("HTTP_CLIENT_IP_TRUSTED_PROXIES is required when HTTP_CLIENT_IP_SOURCE=xff"))
		}
	case ClientIPXFFCount:
		if c.HTTP.ClientIP.ProxyCount < 1 {
			errs = append(errs, errors.New("HTTP_CLIENT_IP_PROXY_COUNT must be >= 1"))
		}
	default:
		errs = append(errs, fmt.Errorf("HTTP_CLIENT_IP_SOURCE must be one of remote_addr|header|xff|xff_count, got %q", c.HTTP.ClientIP.Source))
	}
	for _, p := range c.HTTP.ClientIP.TrustedProxies {
		if _, err := netip.ParsePrefix(p); err != nil {
			errs = append(errs, fmt.Errorf("HTTP_CLIENT_IP_TRUSTED_PROXIES: invalid CIDR %q", p))
		}
	}
	if c.Telemetry.SampleRatio < 0 || c.Telemetry.SampleRatio > 1 {
		errs = append(errs, errors.New("OTEL_TRACES_SAMPLE_RATIO must be between 0 and 1"))
	}
	if c.RateLimit.Enabled && (c.RateLimit.Requests <= 0 || c.RateLimit.Window <= 0) {
		errs = append(errs, errors.New("RATE_LIMIT_REQUESTS and RATE_LIMIT_WINDOW must be positive"))
	}
	if c.CORS.AllowCredentials && slices.Contains(c.CORS.AllowedOrigins, "*") {
		errs = append(errs, errors.New("CORS_ALLOW_CREDENTIALS=true cannot be combined with CORS_ALLOWED_ORIGINS=*: list the trusted origins explicitly"))
	}
	if c.Tenancy.Claim == "" {
		errs = append(errs, errors.New("TENANT_CLAIM is required"))
	}
	if len(c.Kafka.Brokers) == 0 || c.Kafka.Brokers[0] == "" {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	if c.Kafka.MaxAttempts < 1 {
		errs = append(errs, errors.New("KAFKA_MAX_ATTEMPTS must be >= 1"))
	}
	if (c.Kafka.SASLUser == "") != (c.Kafka.SASLPassword == "") {
		errs = append(errs, errors.New("KAFKA_SASL_USER and KAFKA_SASL_PASSWORD must be set together"))
	}
	switch c.Kafka.SASLMechanism {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		errs = append(errs, fmt.Errorf("KAFKA_SASL_MECHANISM must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512, got %q", c.Kafka.SASLMechanism))
	}
	if c.Outbox.Retention <= 0 || c.Outbox.PruneInterval <= 0 || c.Outbox.DedupRetention <= 0 {
		errs = append(errs, errors.New("OUTBOX_RETENTION, OUTBOX_PRUNE_INTERVAL and OUTBOX_DEDUP_RETENTION must be positive"))
	}
	if c.HTTP.MaxBodyBytes <= 0 {
		errs = append(errs, errors.New("HTTP_MAX_BODY_BYTES must be positive"))
	}
	if g := c.HTTP.TerminationGracePeriod; g <= 0 {
		errs = append(errs, errors.New("HTTP_TERMINATION_GRACE_PERIOD must be positive"))
	} else if c.HTTP.ShutdownDelay+c.HTTP.ShutdownTimeout >= g {
		errs = append(errs, fmt.Errorf("HTTP_SHUTDOWN_DELAY + HTTP_SHUTDOWN_TIMEOUT (%s) must be less than HTTP_TERMINATION_GRACE_PERIOD (%s), or the process is killed before it finishes draining",
			c.HTTP.ShutdownDelay+c.HTTP.ShutdownTimeout, g))
	}
	return errors.Join(errs...)
}

// Warnings returns settings that are valid but probably wrong. They are
// logged at startup.
func (c Config) Warnings() []string {
	var w []string
	if c.IsProduction() && c.RateLimit.Enabled && c.HTTP.ClientIP.Source == ClientIPRemoteAddr {
		w = append(w, "HTTP_CLIENT_IP_SOURCE=remote_addr with rate limiting enabled: "+
			"behind a load balancer or ingress every client shares the proxy's IP and therefore one rate-limit bucket. "+
			"Set HTTP_CLIENT_IP_SOURCE to match your topology (see docs/configuration.md#client-ip)")
	}
	if c.IsProduction() && c.Admin.EnablePprof {
		w = append(w, "ADMIN_ENABLE_PPROF=true in production: make sure the admin port is not reachable from outside the cluster")
	}
	return w
}
