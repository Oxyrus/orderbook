// Package httpx contains HTTP helpers shared by every handler: JSON
// encoding/decoding, request validation, RFC 9457 problem responses and
// middleware.
package httpx

import (
	"errors"
	"fmt"
	"net/http"
)

// Error is an application error that maps to an HTTP status and is rendered
// as an RFC 9457 "application/problem+json" document.
//
// Domain packages should define sentinel errors (e.g. item.ErrNotFound)
// and translate them into *Error values in their HTTP handlers, or wrap them
// with the helpers below.
type Error struct {
	Status int    // HTTP status code
	Code   string // stable, machine-readable code, e.g. "item_not_found"
	Detail string // human-readable explanation, safe to show to clients
	// Fields holds per-field validation messages.
	Fields map[string]string
	// Err is the underlying cause. It is logged but never sent to clients.
	Err error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Detail, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

func (e *Error) Unwrap() error { return e.Err }

// Constructors for the most common cases.

func BadRequest(code, detail string) *Error {
	return &Error{Status: http.StatusBadRequest, Code: code, Detail: detail}
}

func Unauthorized(detail string) *Error {
	return &Error{Status: http.StatusUnauthorized, Code: "unauthorized", Detail: detail}
}

func Forbidden(detail string) *Error {
	return &Error{Status: http.StatusForbidden, Code: "forbidden", Detail: detail}
}

func NotFound(code, detail string) *Error {
	return &Error{Status: http.StatusNotFound, Code: code, Detail: detail}
}

func Conflict(code, detail string) *Error {
	return &Error{Status: http.StatusConflict, Code: code, Detail: detail}
}

func PreconditionFailed(code, detail string) *Error {
	return &Error{Status: http.StatusPreconditionFailed, Code: code, Detail: detail}
}

func Validation(fields map[string]string) *Error {
	return &Error{
		Status: http.StatusUnprocessableEntity,
		Code:   "validation_failed",
		Detail: "The request contains invalid fields.",
		Fields: fields,
	}
}

func Internal(err error) *Error {
	return &Error{
		Status: http.StatusInternalServerError,
		Code:   "internal_error",
		Detail: "An unexpected error occurred.",
		Err:    err,
	}
}

// AsError converts any error into an *Error. Unknown errors become 500s so
// internal details never leak to clients.
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return Internal(err)
}

// Problem is the RFC 9457 wire format.
type Problem struct {
	Type      string            `json:"type"`
	Title     string            `json:"title"`
	Status    int               `json:"status"`
	Detail    string            `json:"detail,omitempty"`
	Instance  string            `json:"instance,omitempty"`
	Code      string            `json:"code"`
	RequestID string            `json:"request_id,omitempty"`
	Errors    map[string]string `json:"errors,omitempty"`
}
