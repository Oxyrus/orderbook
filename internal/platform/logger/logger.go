// Package logger configures zerolog, a zero-allocation structured logger.
//
// Conventions:
//   - The root logger is created once in main and passed explicitly.
//   - Per-request loggers (enriched with request_id, trace_id, ...) are stored
//     in the request context; retrieve them with FromContext / zerolog.Ctx.
package logger

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Options configures New.
type Options struct {
	Level       string
	Format      string // json | console
	ServiceName string
	Environment string
	Version     string
	Output      io.Writer // defaults to os.Stdout
}

func init() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.DurationFieldUnit = time.Millisecond
	zerolog.DurationFieldInteger = false
}

// New builds the root logger.
func New(opts Options) zerolog.Logger {
	level, err := zerolog.ParseLevel(strings.ToLower(opts.Level))
	if err != nil || level == zerolog.NoLevel {
		level = zerolog.InfoLevel
	}

	var out io.Writer = os.Stdout
	if opts.Output != nil {
		out = opts.Output
	}
	if strings.EqualFold(opts.Format, "console") {
		out = zerolog.ConsoleWriter{Out: out, TimeFormat: "15:04:05.000"}
	}

	l := zerolog.New(out).Level(level).With().Timestamp()
	if opts.ServiceName != "" {
		l = l.Str("service", opts.ServiceName)
	}
	if opts.Environment != "" {
		l = l.Str("env", opts.Environment)
	}
	if opts.Version != "" {
		l = l.Str("version", opts.Version)
	}
	return l.Logger()
}

// Nop returns a disabled logger, handy in tests.
func Nop() zerolog.Logger { return zerolog.Nop() }

// FromContext returns the request-scoped logger. When none is attached it
// returns zerolog.DefaultContextLogger, which main sets to the root logger
// (so logs from background work aren't lost), or a disabled logger if that
// is unset, as in tests. It never returns nil.
func FromContext(ctx context.Context) *zerolog.Logger {
	return zerolog.Ctx(ctx)
}

// WithContext attaches l to ctx.
func WithContext(ctx context.Context, l zerolog.Logger) context.Context {
	return l.WithContext(ctx)
}
