/* oxlint-disable no-await-in-loop */ // sequential awaits are intentional here (ordering, retries, backoff)
import { INTERNAL_TOKEN_HEADER } from '../auth/constants';
import { counter, histogram, LATENCY_BUCKETS } from '../metrics/metrics';
import { fullJitterBackoff, sleep } from '../util';
import { CircuitBreaker, type CircuitBreakerOptions } from './circuit-breaker';
import { CircuitOpenError, HttpClientError, HttpError, HttpNetworkError, HttpTimeoutError, parseProblem, type RequestInfo } from './errors';

export interface HttpClientOptions {
  /** Target name used in metrics and errors, e.g. "tariff". */
  name: string;
  baseUrl: string;
  /** Per-attempt timeout. Default 2000ms. */
  timeoutMs?: number;
  /** Extra attempts after the first, for retryable requests only. Default 2. */
  retries?: number;
  /** Sent as x-internal-token on every request. */
  internalToken?: string;
  backoffBaseMs?: number;
  backoffMaxMs?: number;
  /** false disables the breaker. */
  circuitBreaker?: CircuitBreakerOptions | false;
  headers?: Record<string, string>;
  /** Injected for tests; defaults to global fetch (undici). */
  fetch?: typeof fetch;
}

export type HttpMethod = 'GET' | 'HEAD' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';

export interface RequestOptions {
  method?: HttpMethod;
  path: string;
  query?: Record<string, string | number | boolean | undefined>;
  headers?: Record<string, string>;
  /** JSON-serialised when not a string. */
  body?: unknown;
  /** Sets the idempotency-key header and makes non-GET requests retryable. */
  idempotencyKey?: string;
  timeoutMs?: number;
  retries?: number;
  signal?: AbortSignal;
}

export interface HttpResponse<T> {
  status: number;
  headers: Headers;
  body: T;
}

export interface HttpClient {
  readonly name: string;
  /** Exposed so callers can short-circuit to a fallback (`if (client.circuit.isOpen) ...`). */
  readonly circuit: CircuitBreaker;
  request<T = unknown>(opts: RequestOptions): Promise<HttpResponse<T>>;
  get<T = unknown>(path: string, opts?: Omit<RequestOptions, 'path' | 'method' | 'body'>): Promise<HttpResponse<T>>;
  post<T = unknown>(path: string, body?: unknown, opts?: Omit<RequestOptions, 'path' | 'method' | 'body'>): Promise<HttpResponse<T>>;
  put<T = unknown>(path: string, body?: unknown, opts?: Omit<RequestOptions, 'path' | 'method' | 'body'>): Promise<HttpResponse<T>>;
  patch<T = unknown>(path: string, body?: unknown, opts?: Omit<RequestOptions, 'path' | 'method' | 'body'>): Promise<HttpResponse<T>>;
  delete<T = unknown>(path: string, opts?: Omit<RequestOptions, 'path' | 'method'>): Promise<HttpResponse<T>>;
}

type Outcome = 'success' | 'client_error' | 'server_error' | 'throttled' | 'timeout' | 'network_error' | 'circuit_open' | 'aborted';

/** Network errors, timeouts, 5xx and 429 are retryable (when the request itself is). */
const retryableStatus = (s: number) => s >= 500 || s === 429;

/**
 * JSON HTTP client over global fetch: per-attempt timeout, full-jitter exponential backoff,
 * retries only for GET/HEAD or requests with an idempotency-key, problem+json -> HttpError,
 * `http_client_requests_seconds{target,method,outcome}` and a consecutive-failure breaker.
 */
export function createHttpClient(opts: HttpClientOptions): HttpClient {
  const doFetch = opts.fetch ?? globalThis.fetch;
  const baseUrl = opts.baseUrl.replace(/\/$/, '');
  const breaker = new CircuitBreaker(opts.circuitBreaker === false ? { failureThreshold: Number.POSITIVE_INFINITY } : opts.circuitBreaker);
  const latency = histogram({
    name: 'http_client_requests_seconds',
    help: 'Outbound HTTP request duration (whole call incl. retries)',
    labelNames: ['target', 'method', 'outcome'] as const,
    buckets: LATENCY_BUCKETS,
  });
  const retriesTotal = counter({ name: 'http_client_retries_total', help: 'Outbound HTTP retry attempts', labelNames: ['target'] as const });

  async function request<T>(r: RequestOptions): Promise<HttpResponse<T>> {
    const method = r.method ?? 'GET';
    const url = buildUrl(baseUrl, r.path, r.query);
    const info: RequestInfo = { target: opts.name, method, url };
    const headers: Record<string, string> = { accept: 'application/json, application/problem+json', ...opts.headers, ...r.headers };
    if (opts.internalToken) headers[INTERNAL_TOKEN_HEADER] = opts.internalToken;
    if (r.idempotencyKey) headers['idempotency-key'] = r.idempotencyKey;
    let body: string | undefined;
    if (r.body !== undefined) {
      body = typeof r.body === 'string' ? r.body : JSON.stringify(r.body);
      headers['content-type'] ??= 'application/json';
    }
    const hasKey = Object.keys(headers).some((k) => k.toLowerCase() === 'idempotency-key');
    const retryable = method === 'GET' || method === 'HEAD' || hasKey;
    const maxAttempts = 1 + (retryable ? Math.max(0, r.retries ?? opts.retries ?? 2) : 0);
    const timeoutMs = r.timeoutMs ?? opts.timeoutMs ?? 2000;
    const t0 = performance.now();
    const observe = (outcome: Outcome) => latency.observe({ target: opts.name, method, outcome }, (performance.now() - t0) / 1000);

    let lastErr: HttpClientError | undefined;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      if (!breaker.tryAcquire()) {
        observe('circuit_open');
        throw new CircuitOpenError(`circuit open for ${opts.name}`, info, { cause: lastErr });
      }
      const signal = r.signal ? AbortSignal.any([r.signal, AbortSignal.timeout(timeoutMs)]) : AbortSignal.timeout(timeoutMs);
      let res: Response;
      try {
        res = await doFetch(url, { method, headers, ...(body !== undefined ? { body } : {}), signal });
      } catch (err) {
        if (r.signal?.aborted) {
          breaker.onNeutral();
          observe('aborted');
          throw err;
        }
        breaker.onFailure();
        const timedOut = (err as Error)?.name === 'TimeoutError' || signal.aborted;
        lastErr = timedOut
          ? new HttpTimeoutError(`${method} ${url} timed out after ${timeoutMs}ms`, info, { cause: err })
          : new HttpNetworkError(`${method} ${url} failed: ${(err as Error)?.message}`, info, { cause: err });
        if (attempt < maxAttempts) {
          retriesTotal.inc({ target: opts.name });
          await sleep(fullJitterBackoff(attempt - 1, opts.backoffBaseMs ?? 50, opts.backoffMaxMs ?? 1000), r.signal);
          continue;
        }
        observe(timedOut ? 'timeout' : 'network_error');
        throw lastErr;
      }

      if (res.ok) {
        breaker.onSuccess();
        const parsed = await readBody<T>(res);
        observe('success');
        return { status: res.status, headers: res.headers, body: parsed };
      }
      const problem = await parseProblem(res);
      const err = new HttpError(res.status, problem, info, attempt);
      if (res.status >= 500) breaker.onFailure();
      else breaker.onNeutral();
      if (retryableStatus(res.status) && attempt < maxAttempts) {
        lastErr = err;
        retriesTotal.inc({ target: opts.name });
        const base = fullJitterBackoff(attempt - 1, opts.backoffBaseMs ?? 50, opts.backoffMaxMs ?? 1000);
        const retryAfter = retryAfterMs(res.headers.get('retry-after'));
        await sleep(Math.min(Math.max(base, retryAfter), opts.backoffMaxMs ?? 1000), r.signal);
        continue;
      }
      observe(res.status === 429 ? 'throttled' : res.status >= 500 ? 'server_error' : 'client_error');
      throw err;
    }
    /* unreachable: the loop always returns or throws */
    throw lastErr ?? new HttpClientError('request failed', info);
  }

  return {
    name: opts.name,
    circuit: breaker,
    request,
    get: (path, o) => request({ ...o, path, method: 'GET' }),
    post: (path, body, o) => request({ ...o, path, body, method: 'POST' }),
    put: (path, body, o) => request({ ...o, path, body, method: 'PUT' }),
    patch: (path, body, o) => request({ ...o, path, body, method: 'PATCH' }),
    delete: (path, o) => request({ ...o, path, method: 'DELETE' }),
  };
}

function buildUrl(base: string, path: string, query?: RequestOptions['query']): string {
  const u = new URL(base + (path.startsWith('/') ? path : `/${path}`));
  for (const [k, v] of Object.entries(query ?? {})) if (v !== undefined) u.searchParams.set(k, String(v));
  return u.toString();
}

async function readBody<T>(res: Response): Promise<T> {
  if (res.status === 204 || res.status === 205) return undefined as T;
  const text = await res.text();
  if (!text) return undefined as T;
  const ct = res.headers.get('content-type') ?? '';
  return (ct.includes('json') ? JSON.parse(text) : text) as T;
}

function retryAfterMs(h: string | null): number {
  if (!h) return 0;
  const secs = Number(h);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const at = Date.parse(h);
  return Number.isFinite(at) ? Math.max(0, at - Date.now()) : 0;
}
