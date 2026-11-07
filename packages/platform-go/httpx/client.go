package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/sotream/evcharge-lab/packages/platform-go/telemetry"
)

// IdempotencyKeyHeader marks a mutating request as safe to retry.
const IdempotencyKeyHeader = "Idempotency-Key"

// ClientOptions configures Client. Zero values pick the defaults noted.
type ClientOptions struct {
	// Transport is the base round tripper (default http.DefaultTransport);
	// it is always wrapped with otelhttp so traceparent is propagated.
	Transport http.RoundTripper
	// AttemptTimeout bounds each attempt including reading headers (default 2s).
	// The body of the returned response stays readable until closed.
	AttemptTimeout time.Duration
	// MaxAttempts is the total number of attempts for retryable requests (default 3).
	MaxAttempts int
	// BaseBackoff and MaxBackoff shape the exponential backoff with full
	// jitter: sleep = rand[0, min(MaxBackoff, BaseBackoff*2^(attempt-1))).
	// Defaults 50ms and 1s. A Retry-After header (seconds) raises the sleep,
	// still capped by MaxBackoff.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// InternalToken, when set, is sent as x-internal-token on every request.
	InternalToken string
}

// Client is an outbound HTTP client for service-to-service calls. Retries
// happen only for idempotent requests (GET/HEAD, or any request carrying an
// Idempotency-Key header) whose body can be replayed, and only on network
// errors (including attempt timeouts), 5xx (except 501) and 429.
type Client struct {
	hc   *http.Client
	opts ClientOptions
}

// NewClient builds a Client.
func NewClient(opts ClientOptions) *Client {
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = 2 * time.Second
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 3
	}
	if opts.BaseBackoff <= 0 {
		opts.BaseBackoff = 50 * time.Millisecond
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = time.Second
	}
	return &Client{hc: &http.Client{Transport: telemetry.HTTPTransport(opts.Transport)}, opts: opts}
}

// Do sends req, retrying as described on Client. The caller must close the
// returned response body.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	parent := req.Context()
	retryable := isIdempotent(req) && (req.Body == nil || req.Body == http.NoBody || req.GetBody != nil)
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(parent, c.opts.AttemptTimeout)
		r := req.Clone(actx)
		if attempt > 1 && req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				cancel()
				return nil, fmt.Errorf("httpx: replay body: %w", err)
			}
			r.Body = body
		}
		if c.opts.InternalToken != "" {
			r.Header.Set(InternalTokenHeader, c.opts.InternalToken)
		}

		resp, err := c.hc.Do(r)
		last := !retryable || attempt >= c.opts.MaxAttempts || parent.Err() != nil
		if last || !shouldRetry(resp, err) {
			if err != nil {
				cancel()
				return nil, err
			}
			resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
			return resp, nil
		}

		wait := c.backoff(attempt)
		if resp != nil {
			if ra := retryAfter(resp); ra > wait {
				wait = min(ra, c.opts.MaxBackoff)
			}
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
			_ = resp.Body.Close()
		}
		cancel()

		t := time.NewTimer(wait)
		select {
		case <-parent.Done():
			t.Stop()
			return nil, parent.Err()
		case <-t.C:
		}
	}
}

// DoJSON sends in (if non-nil) as JSON and decodes a 2xx response into out
// (if non-nil). Non-2xx responses return a *ProblemError. header is merged
// into the request (e.g. Idempotency-Key).
func (c *Client) DoJSON(ctx context.Context, method, url string, in, out any, header http.Header) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("httpx: marshal request: %w", err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json, "+ProblemContentType)

	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return ReadProblem(resp)
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("httpx: decode response: %w", err)
	}
	return nil
}

func isIdempotent(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return true
	}
	return r.Header.Get(IdempotencyKeyHeader) != ""
}

func shouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		return !errors.Is(err, context.Canceled)
	}
	return resp.StatusCode == http.StatusTooManyRequests ||
		(resp.StatusCode >= 500 && resp.StatusCode != http.StatusNotImplemented)
}

func (c *Client) backoff(attempt int) time.Duration {
	ceil := c.opts.BaseBackoff << min(attempt-1, 30)
	if ceil <= 0 || ceil > c.opts.MaxBackoff {
		ceil = c.opts.MaxBackoff
	}
	return rand.N(ceil) //nolint:gosec // jitter, not security sensitive
}

func retryAfter(resp *http.Response) time.Duration {
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return 0
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnClose) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}
