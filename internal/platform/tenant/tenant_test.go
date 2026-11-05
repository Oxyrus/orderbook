package tenant_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Oxyrus/orderbook/internal/platform/auth"
	"github.com/Oxyrus/orderbook/internal/platform/tenant"
)

func TestValid(t *testing.T) {
	for _, id := range []string{"acme", "a", "acme-corp_2", "0day"} {
		assert.True(t, tenant.Valid(id), id)
	}
	for _, id := range []string{"", "Acme", "-acme", "acme corp", "../x", "a'b", string(make([]byte, 64))} {
		assert.False(t, tenant.Valid(id), id)
	}
}

func TestMiddleware(t *testing.T) {
	var got string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = tenant.MustFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})
	serve := func(p *auth.Principal) int {
		got = ""
		h := tenant.Middleware("org")(next)
		if p != nil {
			h = auth.Static(p)(h)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		return rec.Code
	}

	assert.Equal(t, http.StatusNoContent, serve(&auth.Principal{Subject: "u", Claims: map[string]any{"org": "acme"}}))
	assert.Equal(t, "acme", got)

	assert.Equal(t, http.StatusForbidden, serve(&auth.Principal{Subject: "u", Claims: map[string]any{"tenant_id": "acme"}}), "wrong claim name")
	assert.Equal(t, http.StatusForbidden, serve(&auth.Principal{Subject: "u", Claims: map[string]any{"org": 42}}), "non-string claim")
	assert.Equal(t, http.StatusForbidden, serve(&auth.Principal{Subject: "u", Claims: map[string]any{"org": "ACME"}}), "malformed")
	assert.Equal(t, http.StatusUnauthorized, serve(nil))

	assert.Equal(t, http.StatusNoContent, serve(auth.DevPrincipal))
	assert.Equal(t, tenant.DevTenant, got)
}

func TestMustFromContextPanics(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	assert.Panics(t, func() { tenant.MustFromContext(r.Context()) })
	id, ok := tenant.FromContext(tenant.WithID(r.Context(), "acme"))
	assert.True(t, ok)
	assert.Equal(t, "acme", id)
}
