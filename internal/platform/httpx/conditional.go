package httpx

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Conditional requests (RFC 9110 §13) for resources with an integer version
// used for optimistic locking. The ETag is the quoted version, e.g. "3".

// ETag returns the strong ETag for version.
func ETag(version int) string { return `"` + strconv.Itoa(version) + `"` }

// IfMatchVersion returns the version in the If-Match header. present is
// false when the header is absent; a malformed value is a 400.
func IfMatchVersion(r *http.Request) (version int, present bool, err error) {
	im := strings.TrimSpace(r.Header.Get("If-Match"))
	if im == "" {
		return 0, false, nil
	}
	v, ok := ParseETag(im)
	if !ok {
		return 0, false, BadRequest("invalid_if_match", `If-Match must be a single strong ETag as returned by this API, e.g. "3".`)
	}
	return v, true, nil
}

// ParseETag parses one strong ETag of the form "N" (N >= 1). Weak ETags
// never match for If-Match (RFC 9110 §13.1.1).
func ParseETag(s string) (int, bool) {
	if len(s) < 3 || s[0] != '"' || s[len(s)-1] != '"' {
		return 0, false
	}
	v, err := strconv.Atoi(s[1 : len(s)-1])
	if err != nil || v < 1 {
		return 0, false
	}
	return v, true
}

// NotModified reports whether the If-None-Match header matches version
// using the weak comparison of RFC 9110 §13.1.2: "*" or any listed ETag,
// weak or strong, with the same opaque value.
func NotModified(r *http.Request, version int) bool {
	header := r.Header.Get("If-None-Match")
	if header == "" {
		return false
	}
	for _, tag := range strings.Split(header, ",") {
		tag = strings.TrimSpace(tag)
		if tag == "*" {
			return true
		}
		if v, ok := ParseETag(strings.TrimPrefix(tag, "W/")); ok && v == version {
			return true
		}
	}
	return false
}

// Cursor marks a position in a keyset-paginated listing ordered by
// (timestamp DESC, id DESC).
type Cursor struct {
	At time.Time
	ID uuid.UUID
}

type cursorWire struct {
	T time.Time `json:"t"`
	I uuid.UUID `json:"i"`
}

// EncodeCursor returns the opaque, URL-safe form of c.
func EncodeCursor(c Cursor) string {
	b, _ := json.Marshal(cursorWire{T: c.At, I: c.ID})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a cursor produced by EncodeCursor.
func DecodeCursor(s string) (*Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var w cursorWire
	if err := json.Unmarshal(b, &w); err != nil {
		return nil, err
	}
	if w.I == uuid.Nil || w.T.IsZero() {
		return nil, errors.New("incomplete cursor")
	}
	return &Cursor{At: w.T, ID: w.I}, nil
}

// PageParams parses the limit and cursor query parameters shared by every
// list endpoint.
func PageParams(r *http.Request, defaultLimit, maxLimit int) (limit int, after *Cursor, err error) {
	q := r.URL.Query()
	limit = defaultLimit
	if v := q.Get("limit"); v != "" {
		n, convErr := strconv.Atoi(v)
		if convErr != nil || n < 1 || n > maxLimit {
			return 0, nil, Validation(map[string]string{"limit": "must be an integer between 1 and " + strconv.Itoa(maxLimit)})
		}
		limit = n
	}
	if v := q.Get("cursor"); v != "" {
		c, decErr := DecodeCursor(v)
		if decErr != nil {
			return 0, nil, Validation(map[string]string{"cursor": "is invalid"})
		}
		after = c
	}
	return limit, after, nil
}
