package httpx_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Oxyrus/orderbook/internal/platform/httpx"
)

func TestETags(t *testing.T) {
	assert.Equal(t, `"3"`, httpx.ETag(3))
	v, ok := httpx.ParseETag(`"12"`)
	assert.True(t, ok)
	assert.Equal(t, 12, v)
	for _, bad := range []string{`12`, `W/"1"`, `"0"`, `"x"`, `""`} {
		_, ok := httpx.ParseETag(bad)
		assert.False(t, ok, bad)
	}

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	_, present, err := httpx.IfMatchVersion(r)
	assert.False(t, present)
	require.NoError(t, err)
	r.Header.Set("If-Match", `W/"2"`)
	_, _, err = httpx.IfMatchVersion(r)
	assert.Error(t, err, "weak ETags never satisfy If-Match")

	r.Header.Set("If-None-Match", `"1", W/"4"`)
	assert.True(t, httpx.NotModified(r, 4))
	assert.False(t, httpx.NotModified(r, 2))
	r.Header.Set("If-None-Match", "*")
	assert.True(t, httpx.NotModified(r, 9))
}

func TestCursorRoundTrip(t *testing.T) {
	c := httpx.Cursor{At: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC), ID: uuid.New()}
	got, err := httpx.DecodeCursor(httpx.EncodeCursor(c))
	require.NoError(t, err)
	assert.True(t, c.At.Equal(got.At))
	assert.Equal(t, c.ID, got.ID)

	for _, bad := range []string{"%%%", "e30", httpx.EncodeCursor(httpx.Cursor{})} {
		_, err := httpx.DecodeCursor(bad)
		assert.Error(t, err, bad)
	}
}

func TestPageParams(t *testing.T) {
	limit, after, err := httpx.PageParams(httptest.NewRequest(http.MethodGet, "/", nil), 20, 100)
	require.NoError(t, err)
	assert.Equal(t, 20, limit)
	assert.Nil(t, after)

	for _, q := range []string{"?limit=0", "?limit=101", "?limit=x", "?cursor=nope"} {
		_, _, err := httpx.PageParams(httptest.NewRequest(http.MethodGet, "/"+q, nil), 20, 100)
		assert.Equal(t, http.StatusUnprocessableEntity, httpx.AsError(err).Status, q)
	}
}
