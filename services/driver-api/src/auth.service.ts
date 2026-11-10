import { createHash, randomBytes, randomUUID } from 'node:crypto';
import type { Logger, Pool, PoolClient } from '@evcharge/platform';
import { withTx } from '@evcharge/platform';
import { Inject, Injectable } from '@nestjs/common';
import type { Config } from './config';
import { JwtService } from './jwt';
import { metrics } from './metrics';
import { DUMMY_HASH, hashPassword, verifyPassword } from './password';
import { ProblemError, unauthorized } from './problem';
import type { RateLimiter } from './rate-limit';
import type { Downstream } from './downstream';
import { APP_CONFIG, DOWNSTREAM, JWT, LOGGER, PG_POOL, RATE_LIMITER } from './tokens';

export interface TokenPair {
  accessToken: string;
  tokenType: 'Bearer';
  expiresIn: number;
  refreshToken: string;
  driverId: string;
}

const sha256 = (s: string) => createHash('sha256').update(s).digest();
const newRefreshToken = () => randomBytes(32).toString('base64url'); // 256 bits
/** 15 random bytes = exactly 20 base64url characters (OCPP idTag is a 20-char string). */
export const newIdTag = () => randomBytes(15).toString('base64url');

type Rotation = { ok: true; pair: TokenPair } | { ok: false; reason: 'unknown_token' | 'expired' | 'reuse_detected' };

const taken = () => {
  metrics.auth.inc({ op: 'register', outcome: 'email_taken' });
  return new ProblemError(409, 'email_taken', 'Conflict', 'an account with this email already exists');
};

@Injectable()
export class AuthService {
  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(JWT) private readonly jwt: JwtService,
    @Inject(RATE_LIMITER) private readonly limiter: RateLimiter,
    @Inject(DOWNSTREAM) private readonly downstream: Downstream,
    @Inject(APP_CONFIG) private readonly config: Config,
    @Inject(LOGGER) private readonly logger: Logger,
  ) {}

  /**
   * Registers the idTag in sessions FIRST, outside any transaction (one attempt plus one retry, 1 s each, so a slow
   * sessions cannot pin database connections), then inserts the account. sessions is idempotent on driverId, and
   * an idTag left behind in sessions after a failed local insert (duplicate email race, crash) is harmless: its
   * random driverId has no account and is never used. A failed sessions call leaves nothing locally, so the
   * client may simply retry.
   */
  async register(email: string, password: string, ip: string): Promise<{ driverId: string; idTag: string; email: string }> {
    const limit = await this.limiter.hit('register-ip', ip, { limit: this.config.REGISTER_RATE_LIMIT, windowS: this.config.REGISTER_RATE_WINDOW_S });
    if (!limit.allowed) {
      metrics.auth.inc({ op: 'register', outcome: 'rate_limited' });
      throw new ProblemError(429, 'rate_limited', 'Too many requests', 'too many registrations, try again later', { 'retry-after': String(limit.retryAfterS) });
    }
    // Cheap early exit so a known email does not cost a sessions call or a scrypt.
    if ((await this.pool.query('SELECT 1 FROM drivers WHERE email = $1', [email])).rowCount) throw taken();
    const driverId = randomUUID();
    const idTag = newIdTag();
    try {
      await this.downstream.sessions.post('/internal/drivers', { driverId, idTag }, { idempotencyKey: driverId, timeoutMs: this.config.REGISTER_SESSIONS_TIMEOUT_MS, retries: 1 });
    } catch (err) {
      metrics.auth.inc({ op: 'register', outcome: 'downstream_failed' });
      throw err;
    }
    const passwordHash = await hashPassword(password);
    try {
      await this.pool.query('INSERT INTO drivers (driver_id, email, password_hash, id_tag) VALUES ($1, $2, $3, $4)', [driverId, email, passwordHash, idTag]);
    } catch (err) {
      if ((err as { code?: string; constraint?: string }).code === '23505' && (err as { constraint?: string }).constraint === 'drivers_email_uq') throw taken();
      throw err;
    }
    metrics.auth.inc({ op: 'register', outcome: 'ok' });
    return { driverId, idTag, email };
  }

  async login(email: string, password: string, ip: string): Promise<TokenPair> {
    const [byIp, byEmail] = await Promise.all([this.limiter.hit('login-ip', ip), this.limiter.hit('login-email', email)]);
    if (!byIp.allowed || !byEmail.allowed) {
      metrics.auth.inc({ op: 'login', outcome: 'rate_limited' });
      throw new ProblemError(429, 'rate_limited', 'Too many requests', 'too many login attempts, try again later', { 'retry-after': String(Math.max(byIp.retryAfterS, byEmail.retryAfterS)) });
    }
    const { rows } = await this.pool.query<{ driver_id: string; password_hash: string }>('SELECT driver_id, password_hash FROM drivers WHERE email = $1', [email]);
    const row = rows[0];
    // Always run one scrypt so an unknown email costs the same as a wrong password.
    const ok = await verifyPassword(password, row?.password_hash ?? (await DUMMY_HASH));
    if (!row || !ok) {
      metrics.auth.inc({ op: 'login', outcome: 'invalid_credentials' });
      throw unauthorized('invalid_credentials', 'email or password is wrong');
    }
    const pair = await withTx(this.pool, (db) => this.issuePair(db, row.driver_id, randomUUID()));
    metrics.auth.inc({ op: 'login', outcome: 'ok' });
    return pair;
  }

  /** Exchanges a refresh token for a new pair. Presenting an already-rotated token revokes its whole family. */
  async refresh(token: string): Promise<TokenPair> {
    const result = await withTx(this.pool, async (db): Promise<Rotation> => {
      const { rows } = await db.query<{ family_id: string; driver_id: string; expires_at: Date; rotated_at: Date | null; revoked_at: Date | null; in_grace: boolean }>(
        'SELECT family_id, driver_id, expires_at, rotated_at, revoked_at, COALESCE(rotated_at > now() - make_interval(secs => $2), false) AS in_grace FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE',
        [sha256(token), this.config.REFRESH_GRACE_S],
      );
      const row = rows[0];
      if (!row) return { ok: false, reason: 'unknown_token' };
      if (row.rotated_at && row.in_grace && !row.revoked_at) {
        // Grace window: the client most likely lost our response and retried (or two tabs raced). Hand out another
        // successor in the same family instead of treating it as theft. The token is only stored hashed, so the
        // first successor cannot be returned again; both stay valid and share the family.
        if (row.expires_at.getTime() <= Date.now()) return { ok: false, reason: 'expired' };
        metrics.auth.inc({ op: 'refresh', outcome: 'grace_retry' });
        return { ok: true, pair: await this.issuePair(db, row.driver_id, row.family_id) };
      }
      if (row.rotated_at || row.revoked_at) {
        // Reuse of a rotated (or revoked) token: assume theft, kill every token of the family. Committed, not thrown.
        await db.query('UPDATE refresh_tokens SET revoked_at = COALESCE(revoked_at, now()) WHERE family_id = $1', [row.family_id]);
        return { ok: false, reason: 'reuse_detected' };
      }
      if (row.expires_at.getTime() <= Date.now()) return { ok: false, reason: 'expired' };
      await db.query('UPDATE refresh_tokens SET rotated_at = now() WHERE token_hash = $1', [sha256(token)]);
      const pair = await this.issuePair(db, row.driver_id, row.family_id);
      return { ok: true, pair };
    });
    if (!result.ok) {
      metrics.auth.inc({ op: 'refresh', outcome: result.reason });
      if (result.reason === 'reuse_detected') this.logger.warn('refresh token reuse detected; token family revoked');
      throw unauthorized(result.reason === 'expired' ? 'refresh_token_expired' : 'invalid_refresh_token', 'refresh token is not valid');
    }
    metrics.auth.inc({ op: 'refresh', outcome: 'ok' });
    return result.pair;
  }

  /** Revokes the token's family. Unknown tokens are ignored, so logout is idempotent and leaks nothing. */
  async logout(token: string): Promise<void> {
    await this.pool.query('UPDATE refresh_tokens SET revoked_at = COALESCE(revoked_at, now()) WHERE family_id = (SELECT family_id FROM refresh_tokens WHERE token_hash = $1)', [sha256(token)]);
    metrics.auth.inc({ op: 'logout', outcome: 'ok' });
  }

  /**
   * Account deletion. sessions first (409 active_session while charging; blocks the idTag), then billing (payment
   * data), then the local credentials and refresh tokens. Invoices and sessions stay for the legal retention period.
   * Every step is idempotent, so a failed call is simply repeated. The 15 min access token expires on its own.
   */
  async deleteAccount(driverId: string): Promise<void> {
    await this.downstream.sessions.delete(`/internal/drivers/${driverId}`, { idempotencyKey: driverId });
    await this.downstream.billing.delete(`/internal/drivers/${driverId}/payment-data`, { idempotencyKey: driverId });
    await this.pool.query('DELETE FROM drivers WHERE driver_id = $1', [driverId]); // refresh_tokens cascade
    metrics.auth.inc({ op: 'delete', outcome: 'ok' });
  }

  /** The access token outlives the account by up to its TTL (15 min): writes that would recreate data must check the account still exists. */
  async assertAccountExists(driverId: string): Promise<void> {
    if (!(await this.pool.query('SELECT 1 FROM drivers WHERE driver_id = $1', [driverId])).rowCount) throw new ProblemError(403, 'account_deleted', 'Account deleted', 'this account was deleted');
  }

  async me(driverId: string): Promise<{ driverId: string; email: string; idTag: string; createdAt: string } | undefined> {
    const { rows } = await this.pool.query<{ email: string; id_tag: string; created_at: Date }>('SELECT email, id_tag, created_at FROM drivers WHERE driver_id = $1', [driverId]);
    const r = rows[0];
    return r && { driverId, email: r.email, idTag: r.id_tag, createdAt: r.created_at.toISOString() };
  }

  private async issuePair(db: PoolClient, driverId: string, familyId: string): Promise<TokenPair> {
    const refreshToken = newRefreshToken();
    await db.query('INSERT INTO refresh_tokens (token_hash, family_id, driver_id, expires_at) VALUES ($1, $2, $3, now() + make_interval(secs => $4))', [
      sha256(refreshToken),
      familyId,
      driverId,
      this.config.REFRESH_TOKEN_TTL_S,
    ]);
    return { accessToken: this.jwt.issue(driverId), tokenType: 'Bearer', expiresIn: this.jwt.ttlSeconds, refreshToken, driverId };
  }
}
