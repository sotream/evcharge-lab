import { counter } from '@evcharge/platform';

/** HTTP metrics come from the platform (`http_server_requests_seconds`); these add auth and cache outcomes. */
export const metrics = {
  /** op = register|login|refresh|logout; outcome = ok|invalid_credentials|email_taken|rate_limited|reuse_detected|expired|unknown_token|downstream_failed|... */
  auth: counter({ name: 'driver_api_auth_total', help: 'Authentication operations by outcome', labelNames: ['op', 'outcome'] as const }),
  /** Access token verification on every authenticated request. */
  tokenVerify: counter({ name: 'driver_api_token_verify_total', help: 'Access token verification by outcome', labelNames: ['outcome'] as const }),
  sitesCache: counter({ name: 'driver_api_sites_cache_total', help: 'GET /v1/sites cache lookups', labelNames: ['result'] as const }),
  rateLimiterErrors: counter({ name: 'driver_api_rate_limiter_errors_total', help: 'Redis failures in the login rate limiter (it fails open)' }),
};
