package httpx

import (
	"crypto/subtle"
	"net/http"
)

// InternalTokenHeader carries the shared INTERNAL_TOKEN on service-to-service calls.
const InternalTokenHeader = "X-Internal-Token"

// RequireInternalToken rejects requests whose x-internal-token header does
// not equal token (constant-time compare) with 401 problem+json.
func RequireInternalToken(token string) func(http.Handler) http.Handler {
	want := []byte(token)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := []byte(r.Header.Get(InternalTokenHeader))
			if len(want) == 0 || subtle.ConstantTimeCompare(got, want) != 1 {
				Error(w, http.StatusUnauthorized, "unauthorized", "missing or invalid internal token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
