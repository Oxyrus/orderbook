// Package requestid carries the request correlation ID through
// context.Context. It is transport-agnostic so services and repositories can
// read the ID without depending on the HTTP layer.
package requestid

import "context"

type key struct{}

// WithContext returns a copy of ctx carrying id.
func WithContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, key{}, id)
}

// FromContext returns the request ID, or "" when absent.
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(key{}).(string)
	return id
}
