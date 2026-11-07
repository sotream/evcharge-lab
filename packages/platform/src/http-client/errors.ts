/** RFC 9457 problem details. Extension members are kept as-is. */
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail?: string;
  instance?: string;
  [extension: string]: unknown;
}

export interface RequestInfo {
  target: string;
  method: string;
  url: string;
}

/** Base class for every error thrown by the platform HTTP client. */
export class HttpClientError extends Error {
  constructor(message: string, readonly request: RequestInfo, options?: { cause?: unknown }) {
    super(message, options);
  }
}

/** The server answered with a non-2xx status. `problem` is parsed from problem+json when present. */
export class HttpError extends HttpClientError {
  override readonly name = 'HttpError';
  constructor(readonly status: number, readonly problem: ProblemDetails, request: RequestInfo, readonly attempts: number) {
    super(`${request.method} ${request.url} -> ${status} ${problem.title}${problem.detail ? `: ${problem.detail}` : ''}`, request);
  }
}

/** A single attempt exceeded timeoutMs (and retries, if any, were exhausted). */
export class HttpTimeoutError extends HttpClientError {
  override readonly name = 'HttpTimeoutError';
}

/** Connection-level failure (DNS, refused, reset, ...); `cause` holds the undici error. */
export class HttpNetworkError extends HttpClientError {
  override readonly name = 'HttpNetworkError';
}

/** Rejected without calling the target because its circuit breaker is open. */
export class CircuitOpenError extends HttpClientError {
  override readonly name = 'CircuitOpenError';
}

export async function parseProblem(res: Response): Promise<ProblemDetails> {
  const text = await res.text().catch(() => '');
  const ct = res.headers.get('content-type') ?? '';
  if (ct.includes('json') && text) {
    try {
      const body = JSON.parse(text) as Record<string, unknown>;
      if (body && typeof body === 'object' && !Array.isArray(body)) {
        const title = typeof body['title'] === 'string' ? body['title'] : typeof body['message'] === 'string' ? body['message'] : res.statusText || `HTTP ${res.status}`;
        return {
          ...body,
          type: typeof body['type'] === 'string' ? body['type'] : 'about:blank',
          title,
          status: typeof body['status'] === 'number' ? body['status'] : res.status,
        };
      }
    } catch {
      /* fall through to text */
    }
  }
  return { type: 'about:blank', title: res.statusText || `HTTP ${res.status}`, status: res.status, ...(text ? { detail: text.slice(0, 500) } : {}) };
}
