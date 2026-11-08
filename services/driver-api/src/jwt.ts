import { createHash, createHmac, timingSafeEqual } from 'node:crypto';

export const ISSUER = 'evcharge';
export const AUDIENCE = 'driver-api';

export type VerifyFailure = 'malformed' | 'bad_header' | 'unknown_kid' | 'bad_signature' | 'expired' | 'bad_claims';

export class TokenError extends Error {
  constructor(readonly reason: VerifyFailure) {
    super(`invalid access token: ${reason}`);
  }
}

export interface AccessClaims {
  sub: string;
  iss: string;
  aud: string;
  iat: number;
  exp: number;
}

const b64u = (b: Buffer | string) => Buffer.from(b).toString('base64url');
/** The key id is a short fingerprint of the secret, so rotating `JWT_SECRET` needs no extra setting. */
export const kidOf = (secret: string) => createHash('sha256').update(secret).digest('hex').slice(0, 8);
const sign = (secret: string, data: string) => createHmac('sha256', secret).update(data).digest();

/** HS256 access tokens. Signs with `secret`; verifies against `secret` and any `previous` secrets, selected by `kid`. */
export class JwtService {
  private readonly keys = new Map<string, string>();
  private readonly kid: string;

  constructor(
    private readonly secret: string,
    previous: string[] = [],
    private readonly ttlS = 900,
    private readonly now: () => number = () => Date.now(),
  ) {
    if (Buffer.byteLength(secret) < 32) throw new Error('JWT secret must be at least 32 bytes');
    for (const s of [...previous, secret]) this.keys.set(kidOf(s), s);
    this.kid = kidOf(secret);
  }

  get ttlSeconds(): number {
    return this.ttlS;
  }

  issue(driverId: string): string {
    const iat = Math.floor(this.now() / 1000);
    const claims: AccessClaims = { sub: driverId, iss: ISSUER, aud: AUDIENCE, iat, exp: iat + this.ttlS };
    const data = `${b64u(JSON.stringify({ alg: 'HS256', typ: 'JWT', kid: this.kid }))}.${b64u(JSON.stringify(claims))}`;
    return `${data}.${b64u(sign(this.secret, data))}`;
  }

  /** Returns the claims or throws TokenError. `alg` is pinned to HS256 (no "none", no algorithm switching). */
  verify(token: string): AccessClaims {
    const parts = token.split('.');
    if (parts.length !== 3) throw new TokenError('malformed');
    const [h, p, s] = parts as [string, string, string];
    let header: { alg?: unknown; kid?: unknown };
    let claims: Partial<AccessClaims>;
    try {
      header = JSON.parse(Buffer.from(h, 'base64url').toString());
      claims = JSON.parse(Buffer.from(p, 'base64url').toString());
    } catch {
      throw new TokenError('malformed');
    }
    if (header.alg !== 'HS256' || typeof header.kid !== 'string') throw new TokenError('bad_header');
    const secret = this.keys.get(header.kid);
    if (!secret) throw new TokenError('unknown_kid');
    const expected = sign(secret, `${h}.${p}`);
    const given = Buffer.from(s, 'base64url');
    if (given.length !== expected.length || !timingSafeEqual(given, expected)) throw new TokenError('bad_signature');
    if (typeof claims.exp !== 'number' || claims.exp * 1000 <= this.now()) throw new TokenError('expired');
    if (claims.iss !== ISSUER || claims.aud !== AUDIENCE || typeof claims.sub !== 'string' || !claims.sub) throw new TokenError('bad_claims');
    return claims as AccessClaims;
  }
}
