import { type CanActivate, type ExecutionContext, Inject, Injectable } from '@nestjs/common';
import { JwtService, TokenError } from './jwt';
import { metrics } from './metrics';
import { unauthorized } from './problem';
import { JWT } from './tokens';

type Req = { headers: Record<string, string | string[] | undefined>; driverId?: string };

/**
 * ADR 0010: the driver identity comes only from a verified access token. Client-sent `x-driver-id` and
 * `x-internal-token` are removed here, and downstream calls build their own headers from `req.driverId`.
 */
@Injectable()
export class AuthGuard implements CanActivate {
  constructor(@Inject(JWT) private readonly jwt: JwtService) {}

  canActivate(ctx: ExecutionContext): boolean {
    const req = ctx.switchToHttp().getRequest<Req>();
    delete req.headers['x-driver-id'];
    delete req.headers['x-internal-token'];
    const auth = req.headers['authorization'];
    const m = typeof auth === 'string' ? /^Bearer (\S+)$/i.exec(auth) : null;
    if (!m?.[1]) {
      metrics.tokenVerify.inc({ outcome: 'missing' });
      throw unauthorized('missing_token', 'a Bearer access token is required');
    }
    try {
      req.driverId = this.jwt.verify(m[1]).sub;
    } catch (err) {
      const reason = err instanceof TokenError ? err.reason : 'error';
      metrics.tokenVerify.inc({ outcome: reason });
      throw unauthorized(reason === 'expired' ? 'token_expired' : 'invalid_token', 'access token is not valid');
    }
    metrics.tokenVerify.inc({ outcome: 'ok' });
    return true;
  }
}

export function driverIdOf(req: { driverId?: string }): string {
  if (!req.driverId) throw unauthorized('missing_token', 'authentication required');
  return req.driverId;
}
