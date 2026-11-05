package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
	"golang.org/x/sync/singleflight"
)

// Key set refresh policy.
const (
	// jwksMinRefresh is the minimum time between two JWKS fetches. It bounds
	// how often tokens with unknown key IDs (forged or not) can make the
	// service call the issuer.
	jwksMinRefresh = 30 * time.Second
	// jwksMaxAge is how long fetched keys are trusted before they are
	// refreshed, so keys the issuer removes stop being accepted.
	jwksMaxAge = time.Hour
	// jwksFetchTimeout bounds a single JWKS request.
	jwksFetchTimeout = 10 * time.Second
	// jwksMaxBytes caps the JWKS response size.
	jwksMaxBytes = 1 << 20
)

var errRefreshThrottled = errors.New("jwks refresh throttled")

// remoteKeySet is an oidc.KeySet backed by a JWKS URL.
//
// It replaces oidc.RemoteKeySet, which refetches the JWKS after every failed
// signature check. That lets anyone turn each garbage token into a request to
// the issuer. This implementation refetches only when a token names a key ID
// that isn't cached (key rotation), or when the cache is older than
// jwksMaxAge, and never more than once per jwksMinRefresh. A token whose key
// ID is known but whose signature is wrong is rejected without any network
// call. Concurrent refreshes share one request.
type remoteKeySet struct {
	url    string
	client *http.Client
	algs   []jose.SignatureAlgorithm

	minRefresh time.Duration
	maxAge     time.Duration
	now        func() time.Time

	mu          sync.RWMutex
	keys        []jose.JSONWebKey
	attemptedAt time.Time // last fetch attempt, successful or not
	loadedAt    time.Time // last successful fetch

	group singleflight.Group
}

var _ oidc.KeySet = (*remoteKeySet)(nil)

func newRemoteKeySet(url string, algs []string) *remoteKeySet {
	ja := make([]jose.SignatureAlgorithm, len(algs))
	for i, a := range algs {
		ja[i] = jose.SignatureAlgorithm(a)
	}
	return &remoteKeySet{
		url:        url,
		client:     &http.Client{Timeout: jwksFetchTimeout},
		algs:       ja,
		minRefresh: jwksMinRefresh,
		maxAge:     jwksMaxAge,
		now:        time.Now,
	}
}

// VerifySignature implements oidc.KeySet.
func (s *remoteKeySet) VerifySignature(ctx context.Context, raw string) ([]byte, error) {
	jws, err := jose.ParseSignedCompact(raw, s.algs)
	if err != nil {
		return nil, fmt.Errorf("parse jws: %w", err)
	}
	if len(jws.Signatures) != 1 {
		return nil, errors.New("token must have exactly one signature")
	}
	kid := jws.Signatures[0].Header.KeyID

	keys, stale := s.cached()
	if stale || !hasKey(keys, kid) {
		fresh, err := s.refresh(ctx)
		switch {
		case err == nil:
			keys = fresh
		case len(keys) == 0:
			return nil, fmt.Errorf("no signing keys available: %w", err)
		}
		// Otherwise keep using the cached keys: a throttled or failed refresh
		// must not reject tokens signed with keys we already trust.
	}

	for i := range keys {
		if kid != "" && keys[i].KeyID != kid {
			continue
		}
		if payload, err := jws.Verify(&keys[i]); err == nil {
			return payload, nil
		}
	}
	return nil, errors.New("signature does not match any key in the issuer's JWKS")
}

func (s *remoteKeySet) cached() (keys []jose.JSONWebKey, stale bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.keys, s.loadedAt.IsZero() || s.now().Sub(s.loadedAt) >= s.maxAge
}

// hasKey reports whether keys can verify a token with key ID kid. Tokens
// without a kid are tried against every key, so any cached key will do.
func hasKey(keys []jose.JSONWebKey, kid string) bool {
	if kid == "" {
		return len(keys) > 0
	}
	for _, k := range keys {
		if k.KeyID == kid {
			return true
		}
	}
	return false
}

// refresh fetches the JWKS unless another fetch happened within minRefresh.
// The fetch is detached from ctx (it is shared by concurrent callers), but
// the caller stops waiting when ctx is done.
func (s *remoteKeySet) refresh(ctx context.Context) ([]jose.JSONWebKey, error) {
	ch := s.group.DoChan("jwks", func() (any, error) {
		s.mu.Lock()
		if !s.attemptedAt.IsZero() && s.now().Sub(s.attemptedAt) < s.minRefresh {
			s.mu.Unlock()
			return nil, errRefreshThrottled
		}
		s.attemptedAt = s.now()
		s.mu.Unlock()

		keys, err := s.fetch()
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.keys, s.loadedAt = keys, s.now()
		s.mu.Unlock()
		return keys, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		keys, _ := r.Val.([]jose.JSONWebKey)
		return keys, nil
	}
}

func (s *remoteKeySet) fetch() ([]jose.JSONWebKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), jwksFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch jwks: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch jwks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch jwks: unexpected status %s", resp.Status)
	}

	// Decode keys one by one so a single unsupported key type (e.g. a new
	// algorithm the issuer publishes) doesn't invalidate the whole set.
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, jwksMaxBytes)).Decode(&set); err != nil {
		return nil, fmt.Errorf("decode jwks: %w", err)
	}
	keys := make([]jose.JSONWebKey, 0, len(set.Keys))
	for _, rawKey := range set.Keys {
		var k jose.JSONWebKey
		if err := json.Unmarshal(rawKey, &k); err != nil {
			continue
		}
		if (k.Use != "" && k.Use != "sig") || !k.IsPublic() {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil, errors.New("jwks contains no usable signing keys")
	}
	return keys, nil
}
