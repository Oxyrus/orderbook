package httpx_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/platform/httpx"
)

type payload struct {
	Name  string `json:"name" validate:"required,max=5"`
	Email string `json:"email" validate:"omitempty,email"`
}

func TestDecode(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		ct         string
		wantStatus int
		wantCode   string
	}{
		{"ok", `{"name":"bob"}`, "application/json", 0, ""},
		{"no content type is accepted", `{"name":"bob"}`, "", 0, ""},
		{"wrong content type", `{"name":"bob"}`, "text/plain", 415, "unsupported_media_type"},
		{"malformed", `{"name":`, "application/json", 400, "invalid_json"},
		{"syntax", `{"name" "x"}`, "application/json", 400, "invalid_json"},
		{"wrong type", `{"name":1}`, "application/json", 400, "invalid_json"},
		{"unknown field", `{"name":"a","x":1}`, "application/json", 400, "unknown_field"},
		{"empty", ``, "application/json", 400, "empty_body"},
		{"trailing data", `{"name":"a"}{}`, "application/json", 400, "invalid_json"},
		{"validation", `{"name":"toolong","email":"nope"}`, "application/json", 422, "validation_failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			if tt.ct != "" {
				r.Header.Set("Content-Type", tt.ct)
			}
			var p payload
			err := httpx.Decode(r, &p)
			if tt.wantStatus == 0 {
				require.NoError(t, err)
				return
			}
			e := httpx.AsError(err)
			assert.Equal(t, tt.wantStatus, e.Status)
			assert.Equal(t, tt.wantCode, e.Code)
		})
	}
}

func TestDecodeValidationFields(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"","email":"nope"}`))
	var p payload
	e := httpx.AsError(httpx.Decode(r, &p))
	assert.Equal(t, map[string]string{"name": "is required", "email": "must be a valid email address"}, e.Fields)
}

func TestValidationMessages(t *testing.T) {
	type sized struct {
		Name  string   `json:"name" validate:"required,notblank,min=2,max=4"`
		Tags  []string `json:"tags" validate:"max=1"`
		Count int      `json:"count" validate:"min=1"`
	}
	tests := []struct {
		body string
		want map[string]string
	}{
		{`{"name":"   ","count":1}`, map[string]string{"name": "must not be blank"}},
		{`{"name":"a","count":1}`, map[string]string{"name": "must be at least 2 characters"}},
		{`{"name":"abcde","tags":["a","b"],"count":0}`, map[string]string{
			"name":  "must be at most 4 characters",
			"tags":  "must be at most 1 items",
			"count": "must be at least 1",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.body, func(t *testing.T) {
			var v sized
			e := httpx.AsError(httpx.Decode(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body)), &v))
			assert.Equal(t, http.StatusUnprocessableEntity, e.Status)
			assert.Equal(t, tt.want, e.Fields)
		})
	}
}

func TestDecodeBodyTooLarge(t *testing.T) {
	h := httpx.MaxBodyBytes(10)(httpx.Handle(func(w http.ResponseWriter, r *http.Request) error {
		var p payload
		return httpx.Decode(r, &p)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"aaaaaaaaaaaaaaaaaaaa"}`)))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func TestWriteErrorHidesInternalDetails(t *testing.T) {
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	httpx.WriteError(rec, r, errors.New("pq: password authentication failed for user secret"))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	assert.NotContains(t, rec.Body.String(), "password")

	var p httpx.Problem
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	assert.Equal(t, "internal_error", p.Code)
	assert.Equal(t, "/x", p.Instance)
}

func TestRequestID(t *testing.T) {
	var seen string
	h := httpx.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = httpx.RequestIDFromContext(r.Context())
	}))

	t.Run("generated", func(t *testing.T) {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		assert.NotEmpty(t, seen)
		assert.Equal(t, seen, rec.Header().Get(httpx.HeaderRequestID))
	})
	t.Run("propagated", func(t *testing.T) {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set(httpx.HeaderRequestID, "abc-123")
		h.ServeHTTP(rec, r)
		assert.Equal(t, "abc-123", seen)
	})
	t.Run("malicious value replaced", func(t *testing.T) {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set(httpx.HeaderRequestID, "bad\nvalue with spaces")
		h.ServeHTTP(rec, r)
		assert.NotEqual(t, "bad\nvalue with spaces", seen)
	})
}

func TestRecoverer(t *testing.T) {
	h := httpx.Recoverer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "internal_error")
}

func TestSecureHeaders(t *testing.T) {
	h := httpx.SecureHeaders(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "DENY", rec.Header().Get("X-Frame-Options"))
}
