package config

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaults(t *testing.T) {
	t.Setenv("APP_ENV", "development") // must be ignored
	t.Setenv("HTTP_ADDR", ":1")
	cfg := Defaults()
	require.NoError(t, cfg.Validate(), "defaults must be valid")
	assert.Equal(t, ":8080", cfg.HTTP.Addr)
	assert.Equal(t, EnvProduction, cfg.Environment, "fail closed when APP_ENV is unset")
	assert.False(t, cfg.Admin.EnablePprof)
	assert.False(t, cfg.Auth.RequireATJWT)
	assert.Equal(t, int32(20), cfg.DB.MaxConns)
	assert.Equal(t, []string{"RS256", "ES256"}, cfg.Auth.SupportedAlgs)
	assert.True(t, cfg.Auth.Enabled)
	assert.False(t, cfg.Redis.Enabled)
	assert.False(t, cfg.Redis.ReadinessCheck)
	assert.Equal(t, 30*time.Second, cfg.HTTP.TerminationGracePeriod)
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	t.Setenv("HTTP_ADDR", ":1234")
	t.Setenv("DB_MAX_CONNS", "50")
	t.Setenv("HTTP_REQUEST_TIMEOUT", "3s")
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://a.example,https://b.example")

	cfg, err := Load()
	require.NoError(t, err)
	assert.False(t, cfg.IsProduction())
	assert.Equal(t, ":1234", cfg.HTTP.Addr)
	assert.Equal(t, int32(50), cfg.DB.MaxConns)
	assert.Equal(t, 3*time.Second, cfg.HTTP.RequestTimeout)
	assert.Equal(t, []string{"https://a.example", "https://b.example"}, cfg.CORS.AllowedOrigins)
}

func TestValidate(t *testing.T) {
	valid := Defaults

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"valid defaults", func(*Config) {}, ""},
		{"bad env", func(c *Config) { c.Environment = "staging" }, "APP_ENV"},
		{"bad log format", func(c *Config) { c.Log.Format = "xml" }, "LOG_FORMAT"},
		{"min > max conns", func(c *Config) { c.DB.MinConns = 30 }, "DB_MIN_CONNS"},
		{"auth disabled in prod", func(c *Config) { c.Environment = EnvProduction; c.Auth.Enabled = false }, "not allowed in production"},
		{"auth disabled by default env", func(c *Config) { c.Auth.Enabled = false }, "not allowed in production"},
		{"auth disabled in dev ok", func(c *Config) { c.Environment = EnvDevelopment; c.Auth.Enabled = false }, ""},
		{"missing audience", func(c *Config) { c.Auth.Audience = "" }, "AUTH_AUDIENCE"},
		{"bad sample ratio", func(c *Config) { c.Telemetry.SampleRatio = 2 }, "SAMPLE_RATIO"},
		{"shutdown too long", func(c *Config) { c.HTTP.ShutdownDelay = 10 * time.Second }, "HTTP_TERMINATION_GRACE_PERIOD"},
		{"longer grace period allows longer shutdown", func(c *Config) {
			c.HTTP.ShutdownDelay = 10 * time.Second
			c.HTTP.TerminationGracePeriod = time.Minute
		}, ""},
		{"shorter grace period (Cloud Run)", func(c *Config) { c.HTTP.TerminationGracePeriod = 10 * time.Second }, "HTTP_TERMINATION_GRACE_PERIOD"},
		{"zero grace period", func(c *Config) { c.HTTP.TerminationGracePeriod = 0 }, "must be positive"},
		{"xff without proxies", func(c *Config) { c.HTTP.ClientIP.Source = ClientIPXFF }, "TRUSTED_PROXIES is required"},
		{"bad cidr", func(c *Config) { c.HTTP.ClientIP.TrustedProxies = []string{"nope"} }, "invalid CIDR"},
		{"redis enabled defaults ok", func(c *Config) { c.Redis.Enabled = true }, ""},
		{"redis disabled ignores url", func(c *Config) { c.Redis.URL = "" }, ""},
		{"redis missing url", func(c *Config) { c.Redis.Enabled = true; c.Redis.URL = "" }, "REDIS_URL is required"},
		{"redis bad scheme", func(c *Config) { c.Redis.Enabled = true; c.Redis.URL = "localhost:6379" }, "redis:// or rediss://"},
		{"redis min idle > pool", func(c *Config) { c.Redis.Enabled = true; c.Redis.PoolSize = 2; c.Redis.MinIdleConns = 5 }, "REDIS_MIN_IDLE_CONNS"},
		{"cors wildcard with credentials", func(c *Config) {
			c.CORS.AllowedOrigins = []string{"https://a.example", "*"}
			c.CORS.AllowCredentials = true
		}, "CORS_ALLOW_CREDENTIALS"},
		{"cors wildcard without credentials ok", func(c *Config) { c.CORS.AllowedOrigins = []string{"*"} }, ""},
		{"unknown ip source", func(c *Config) { c.HTTP.ClientIP.Source = "magic" }, "HTTP_CLIENT_IP_SOURCE"},
		{"missing tenant claim", func(c *Config) { c.Tenancy.Claim = "" }, "TENANT_CLAIM"},
		{"missing kafka brokers", func(c *Config) { c.Kafka.Brokers = nil }, "KAFKA_BROKERS"},
		{"bad max attempts", func(c *Config) { c.Kafka.MaxAttempts = 0 }, "KAFKA_MAX_ATTEMPTS"},
		{"sasl user without password", func(c *Config) { c.Kafka.SASLUser = "u" }, "KAFKA_SASL_PASSWORD"},
		{"bad sasl mechanism", func(c *Config) { c.Kafka.SASLMechanism = "GSSAPI" }, "KAFKA_SASL_MECHANISM"},
		{"zero outbox retention", func(c *Config) { c.Outbox.Retention = 0 }, "OUTBOX_RETENTION"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), tt.wantErr), "error %q should contain %q", err, tt.wantErr)
		})
	}
}

func TestWarnings(t *testing.T) {
	base := Defaults

	cfg := base()
	require.Len(t, cfg.Warnings(), 1, "production + rate limit + remote_addr")
	assert.Contains(t, cfg.Warnings()[0], "HTTP_CLIENT_IP_SOURCE")

	cfg = base()
	cfg.HTTP.ClientIP.Source = ClientIPXFFCount
	assert.Empty(t, cfg.Warnings())

	cfg = base()
	cfg.RateLimit.Enabled = false
	assert.Empty(t, cfg.Warnings())

	cfg = base()
	cfg.Environment = EnvDevelopment
	cfg.Admin.EnablePprof = true
	assert.Empty(t, cfg.Warnings(), "no warnings outside production")

	cfg = base()
	cfg.HTTP.ClientIP.Source = ClientIPXFFCount
	cfg.Admin.EnablePprof = true
	require.Len(t, cfg.Warnings(), 1)
	assert.Contains(t, cfg.Warnings()[0], "ADMIN_ENABLE_PPROF")
}
