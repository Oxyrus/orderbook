package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"

	"github.com/go-playground/validator/v10"
	"github.com/go-playground/validator/v10/non-standard/validators"
	"github.com/rs/zerolog"
)

// JSON writes v as a JSON response with the given status.
func JSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("encode response")
	}
}

// NoContent writes a 204 response.
func NoContent(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }

// WriteError renders err as an RFC 9457 problem document and logs it. 5xx
// errors are logged at error level including the internal cause; 4xx errors
// at debug level.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	e := AsError(err)
	log := zerolog.Ctx(r.Context())
	if e.Status >= http.StatusInternalServerError {
		log.Error().Err(err).Str("code", e.Code).Msg("request failed")
	} else {
		log.Debug().Err(err).Str("code", e.Code).Int("status", e.Status).Msg("request rejected")
	}

	p := Problem{
		Type:      "about:blank",
		Title:     http.StatusText(e.Status),
		Status:    e.Status,
		Detail:    e.Detail,
		Instance:  r.URL.Path,
		Code:      e.Code,
		RequestID: RequestIDFromContext(r.Context()),
		Errors:    e.Fields,
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(e.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// HandlerFunc is an http.HandlerFunc that returns an error. Wrap it with
// Handle to get centralised error rendering, which keeps handlers short:
//
//	r.Get("/items/{id}", httpx.Handle(h.get))
type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

// Handle adapts a HandlerFunc to http.HandlerFunc.
func Handle(h HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			WriteError(w, r, err)
		}
	}
}

var (
	validate     *validator.Validate
	validateOnce sync.Once
)

// Validator returns the shared validator instance. Field names in error
// messages use the struct's json tag.
//
// Besides the built-in tags, "notblank" rejects strings that are empty or
// only whitespace ("required" accepts "   ").
func Validator() *validator.Validate {
	validateOnce.Do(func() {
		validate = validator.New(validator.WithRequiredStructEnabled())
		if err := validate.RegisterValidation("notblank", validators.NotBlank); err != nil {
			panic(err)
		}
		validate.RegisterTagNameFunc(func(f reflect.StructField) string {
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "-" {
				return ""
			}
			if name == "" {
				return f.Name
			}
			return name
		})
	})
	return validate
}

// Decode reads a JSON body into dst, rejecting unknown fields, trailing data
// and oversized bodies, then validates dst using `validate` struct tags.
func Decode(r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		return &Error{Status: http.StatusUnsupportedMediaType, Code: "unsupported_media_type", Detail: "Content-Type must be application/json."}
	}

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return decodeError(err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return BadRequest("invalid_json", "Request body must contain a single JSON object.")
	}
	return ValidateStruct(dst)
}

// ValidateStruct validates v and converts failures into a 422 *Error.
func ValidateStruct(v any) error {
	err := Validator().Struct(v)
	if err == nil {
		return nil
	}
	var verrs validator.ValidationErrors
	if !errors.As(err, &verrs) {
		return Internal(err)
	}
	fields := make(map[string]string, len(verrs))
	for _, fe := range verrs {
		fields[fieldPath(fe)] = validationMessage(fe)
	}
	return Validation(fields)
}

func fieldPath(fe validator.FieldError) string {
	// Namespace is "Struct.field.sub"; drop the root struct name.
	ns := fe.Namespace()
	if _, rest, ok := strings.Cut(ns, "."); ok {
		return rest
	}
	return fe.Field()
}

func validationMessage(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "is required"
	case "notblank":
		return "must not be blank"
	case "min", "gte":
		return "must be at least " + fe.Param() + sizeUnit(fe)
	case "max", "lte":
		return "must be at most " + fe.Param() + sizeUnit(fe)
	case "email":
		return "must be a valid email address"
	case "uuid", "uuid4":
		return "must be a valid UUID"
	case "oneof":
		return "must be one of: " + fe.Param()
	case "url":
		return "must be a valid URL"
	default:
		return fmt.Sprintf("failed %q validation", fe.Tag())
	}
}

// sizeUnit returns what min/max count for the field's kind: characters for
// strings, items for collections and nothing for numbers.
func sizeUnit(fe validator.FieldError) string {
	switch fe.Kind() { //nolint:exhaustive // every other kind is compared by value
	case reflect.String:
		return " characters"
	case reflect.Slice, reflect.Array, reflect.Map:
		return " items"
	default:
		return ""
	}
}

func decodeError(err error) error {
	var (
		syntaxErr *json.SyntaxError
		typeErr   *json.UnmarshalTypeError
		maxErr    *http.MaxBytesError
	)
	switch {
	case errors.As(err, &syntaxErr):
		return BadRequest("invalid_json", fmt.Sprintf("Malformed JSON at position %d.", syntaxErr.Offset))
	case errors.Is(err, io.ErrUnexpectedEOF):
		return BadRequest("invalid_json", "Malformed JSON.")
	case errors.As(err, &typeErr):
		return BadRequest("invalid_json", fmt.Sprintf("Field %q has the wrong type.", typeErr.Field))
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		return BadRequest("unknown_field", fmt.Sprintf("Unknown field %s.", strings.TrimPrefix(err.Error(), "json: unknown field ")))
	case errors.Is(err, io.EOF):
		return BadRequest("empty_body", "Request body must not be empty.")
	case errors.As(err, &maxErr):
		return &Error{Status: http.StatusRequestEntityTooLarge, Code: "body_too_large", Detail: fmt.Sprintf("Request body must not exceed %d bytes.", maxErr.Limit)}
	default:
		return BadRequest("invalid_json", "Request body could not be decoded.")
	}
}
