// Package httpx holds HTTP helpers shared by the services: problem+json
// errors (contracts.Problem), strict JSON decoding, the x-internal-token
// middleware, and an outbound client with per-attempt timeouts and retries
// for idempotent requests.
package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	contracts "github.com/sotream/evcharge-lab/packages/contracts-go"
)

// ProblemContentType is the RFC 9457 media type.
const ProblemContentType = "application/problem+json"

// NewProblem builds a Problem with Type "about:blank" and the standard status
// text as Title.
func NewProblem(status int, code, detail string) contracts.Problem {
	return contracts.Problem{Type: "about:blank", Title: http.StatusText(status), Status: status, Code: code, Detail: detail}
}

// WriteProblem writes p as application/problem+json with p.Status.
func WriteProblem(w http.ResponseWriter, p contracts.Problem) {
	if p.Status == 0 {
		p.Status = http.StatusInternalServerError
	}
	if p.Type == "" {
		p.Type = "about:blank"
	}
	if p.Title == "" {
		p.Title = http.StatusText(p.Status)
	}
	w.Header().Set("Content-Type", ProblemContentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// Error writes a problem for status with a machine-readable code and detail.
func Error(w http.ResponseWriter, status int, code, detail string) {
	WriteProblem(w, NewProblem(status, code, detail))
}

// WriteError maps err to a problem: a *ProblemError (e.g. from DecodeJSON or
// an upstream call) keeps its problem, anything else becomes a 500 without
// leaking err's message.
func WriteError(w http.ResponseWriter, err error) {
	var pe *ProblemError
	if errors.As(err, &pe) {
		WriteProblem(w, pe.Problem)
		return
	}
	Error(w, http.StatusInternalServerError, "internal", "")
}

// WriteJSON writes v as application/json with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ProblemError is an error carrying a Problem: returned by DecodeJSON for
// bad requests and by ReadProblem/Client.DoJSON for non-2xx upstream answers.
type ProblemError struct {
	contracts.Problem
}

func (e *ProblemError) Error() string {
	s := fmt.Sprintf("http %d %s", e.Status, e.Title)
	if e.Code != "" {
		s += " [" + e.Code + "]"
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// maxProblemBody bounds how much of an error body ReadProblem consumes.
const maxProblemBody = 64 << 10

// ReadProblem converts a non-2xx response into a *ProblemError. Bodies that
// are not problem+json produce a synthetic Problem with the response status.
// It reads (bounded) but does not close resp.Body.
func ReadProblem(resp *http.Response) *ProblemError {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProblemBody))
	pe := &ProblemError{Problem: NewProblem(resp.StatusCode, "", "")}
	mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mt == ProblemContentType || mt == "application/json" {
		var p contracts.Problem
		if json.Unmarshal(body, &p) == nil && p.Status != 0 {
			pe.Problem = p
			return pe
		}
	}
	if len(body) > 0 && len(body) <= 512 {
		pe.Detail = string(body)
	}
	return pe
}
