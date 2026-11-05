package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want int
	}{
		{"version", []string{"version"}, 0},
		{"help", []string{"--help"}, 0},
		{"unknown command", []string{"frobnicate"}, 2},
		{"migrate without subcommand", []string{"migrate"}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", "disabled")
			assert.Equal(t, tt.want, run(tt.args))
		})
	}
}

func TestRunInvalidConfig(t *testing.T) {
	t.Setenv("APP_ENV", "staging")
	assert.Equal(t, 1, run([]string{"serve"}))
}

func TestHealthcheck(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/livez" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	t.Setenv("ADMIN_ADDR", addr)

	assert.Equal(t, 0, run([]string{"healthcheck"}))

	status = http.StatusServiceUnavailable
	assert.Equal(t, 1, run([]string{"healthcheck"}))

	t.Setenv("ADMIN_ADDR", ":"+strings.Split(addr, ":")[1]) // ":port" form
	status = http.StatusOK
	assert.Equal(t, 0, run([]string{"healthcheck"}))

	srv.Close()
	assert.Equal(t, 1, run([]string{"healthcheck"}), "unreachable admin port")
}
