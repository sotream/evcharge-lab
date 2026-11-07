package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

// DefaultMaxBody is the body limit DecodeJSON applies when limit <= 0.
const DefaultMaxBody = 1 << 20

// DecodeJSON strictly decodes the request body into dst: at most limit bytes
// (DefaultMaxBody if <= 0), unknown fields rejected, exactly one JSON value.
// A Content-Type other than application/json or +json yields 415 (a missing
// Content-Type is accepted). Errors are
// *ProblemError (400/413/415) ready for WriteError.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any, limit int64) error {
	if limit <= 0 {
		limit = DefaultMaxBody
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil || (mt != "application/json" && !strings.HasSuffix(mt, "+json")) {
			return &ProblemError{NewProblem(http.StatusUnsupportedMediaType, "unsupported_media_type", "expected application/json")}
		}
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return decodeProblem(err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err != nil {
			var mbe *http.MaxBytesError
			if errors.As(err, &mbe) {
				return decodeProblem(err)
			}
		}
		return &ProblemError{NewProblem(http.StatusBadRequest, "invalid_json", "body must contain a single JSON value")}
	}
	return nil
}

func decodeProblem(err error) *ProblemError {
	var (
		mbe *http.MaxBytesError
		se  *json.SyntaxError
		ute *json.UnmarshalTypeError
	)
	switch {
	case errors.As(err, &mbe):
		return &ProblemError{NewProblem(http.StatusRequestEntityTooLarge, "body_too_large", fmt.Sprintf("body exceeds %d bytes", mbe.Limit))}
	case errors.Is(err, io.EOF):
		return &ProblemError{NewProblem(http.StatusBadRequest, "invalid_json", "body is empty")}
	case errors.As(err, &se), errors.Is(err, io.ErrUnexpectedEOF):
		return &ProblemError{NewProblem(http.StatusBadRequest, "invalid_json", "malformed JSON")}
	case errors.As(err, &ute):
		return &ProblemError{NewProblem(http.StatusBadRequest, "invalid_json", fmt.Sprintf("field %q has the wrong type", ute.Field))}
	case strings.HasPrefix(err.Error(), "json: unknown field"):
		return &ProblemError{NewProblem(http.StatusBadRequest, "unknown_field", strings.TrimPrefix(err.Error(), "json: "))}
	default:
		return &ProblemError{NewProblem(http.StatusBadRequest, "invalid_json", err.Error())}
	}
}
