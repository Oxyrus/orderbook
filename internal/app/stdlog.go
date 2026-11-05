package app

import (
	"log"
	"strings"

	"github.com/rs/zerolog"
)

// newStdLogger adapts zerolog to the *log.Logger expected by http.Server so
// internal server errors (TLS handshake failures, etc.) are structured too.
func newStdLogger(l zerolog.Logger) *log.Logger {
	return log.New(stdWriter{l: l.With().Str("component", "net/http").Logger()}, "", 0)
}

type stdWriter struct{ l zerolog.Logger }

func (w stdWriter) Write(p []byte) (int, error) {
	w.l.Warn().Msg(strings.TrimSpace(string(p)))
	return len(p), nil
}
