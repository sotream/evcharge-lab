// Package metrics provides a Prometheus registry pre-loaded with Go runtime
// and process collectors, HTTP server middleware recording
// http_server_requests_seconds{method,route,status}, and the /metrics handler.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// UnmatchedRoute is the route label for requests that matched no
// http.ServeMux pattern (keeps label cardinality bounded).
const UnmatchedRoute = "unmatched"

// Registry wraps a dedicated *prometheus.Registry (not the global default).
// Register service metrics on it with MustRegister / Register.
type Registry struct {
	*prometheus.Registry
	requests *prometheus.HistogramVec
}

// New returns a registry with go and process collectors and the HTTP
// request-duration histogram registered.
func New() *Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	requests := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_server_requests_seconds",
		Help:    "HTTP server request duration in seconds.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"method", "route", "status"})
	reg.MustRegister(requests)
	return &Registry{Registry: reg, requests: requests}
}

// Handler serves the registry in the Prometheus exposition format.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.Registry, promhttp.HandlerOpts{Registry: r.Registry})
}

// Middleware records request duration. The route label is the matched
// http.ServeMux pattern (path part only, e.g. "/v1/tariffs/{id}"), read from
// Request.Pattern after the inner handler ran; the request must therefore
// reach the mux without being copied (r.WithContext) by intermediate wrappers,
// otherwise the route is reported as "unmatched".
func (r *Registry) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			r.requests.WithLabelValues(req.Method, Route(req), strconv.Itoa(status)).Observe(time.Since(start).Seconds())
		}()
		next.ServeHTTP(sw, req)
	})
}

// Route returns the path part of req.Pattern ("GET /a/{id}" -> "/a/{id}"),
// or UnmatchedRoute.
func Route(req *http.Request) string {
	p := req.Pattern
	if p == "" {
		return UnmatchedRoute
	}
	for i := 0; i < len(p); i++ {
		if p[i] == ' ' || p[i] == '\t' {
			return p[i+1:]
		}
	}
	return p
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach Flush/Hijack/deadlines.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Flush supports streaming handlers that type-assert http.Flusher.
func (w *statusWriter) Flush() {
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
