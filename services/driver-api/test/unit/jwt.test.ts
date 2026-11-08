import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { AUDIENCE, ISSUER, JwtService, kidOf, TokenError } from '../../src/jwt';

const S1 = 'a'.repeat(40);
const S2 = 'b'.repeat(40);
const DRIVER = '11111111-1111-4111-8111-111111111111';
const b64u = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url');
const reason = (fn: () => unknown) => {
  try {
    fn();
  } catch (e) {
    return e instanceof TokenError ? e.reason : 'other';
  }
  return 'no error';
};

describe('JwtService', () => {
  it('issues HS256 tokens with sub, iss, aud, kid and a 15 min lifetime', () => {
    let now = 1_700_000_000_000;
    const jwt = new JwtService(S1, [], 900, () => now);
    const token = jwt.issue(DRIVER);
    const [h, p] = token.split('.') as [string, string];
    expect(JSON.parse(Buffer.from(h, 'base64url').toString())).toEqual({ alg: 'HS256', typ: 'JWT', kid: kidOf(S1) });
    expect(JSON.parse(Buffer.from(p, 'base64url').toString())).toMatchObject({ sub: DRIVER, iss: ISSUER, aud: AUDIENCE, exp: 1_700_000_900 });
    expect(jwt.verify(token).sub).toBe(DRIVER);
    now += 899_000;
    expect(jwt.verify(token).sub).toBe(DRIVER);
    now += 1000;
    expect(reason(() => jwt.verify(token))).toBe('expired');
  });

  it('rejects bad signatures, tampered payloads, alg none and other algorithms', () => {
    const jwt = new JwtService(S1);
    const token = jwt.issue(DRIVER);
    const [h, p, s] = token.split('.') as [string, string, string];
    expect(reason(() => jwt.verify(`${h}.${p}.${s.slice(0, -2)}AA`))).toBe('bad_signature');
    const evil = b64u({ sub: 'someone-else', iss: ISSUER, aud: AUDIENCE, exp: 9_999_999_999 });
    expect(reason(() => jwt.verify(`${h}.${evil}.${s}`))).toBe('bad_signature');
    expect(reason(() => jwt.verify(`${b64u({ alg: 'none', kid: kidOf(S1) })}.${p}.`))).toBe('bad_header');
    expect(reason(() => jwt.verify(`${b64u({ alg: 'HS512', kid: kidOf(S1) })}.${p}.${s}`))).toBe('bad_header');
    expect(reason(() => jwt.verify('garbage'))).toBe('malformed');
    expect(reason(() => jwt.verify('a.b.c'))).toBe('malformed');
  });

  it('checks issuer, audience and subject even when the signature is valid', () => {
    const jwt = new JwtService(S1);
    const forge = (claims: object) => {
      const data = `${b64u({ alg: 'HS256', typ: 'JWT', kid: kidOf(S1) })}.${b64u(claims)}`;
      return `${data}.${createHmac('sha256', S1).update(data).digest('base64url')}`;
    };
    const exp = Math.floor(Date.now() / 1000) + 60;
    expect(jwt.verify(forge({ sub: DRIVER, iss: ISSUER, aud: AUDIENCE, exp })).sub).toBe(DRIVER);
    expect(reason(() => jwt.verify(forge({ sub: DRIVER, iss: 'other', aud: AUDIENCE, exp })))).toBe('bad_claims');
    expect(reason(() => jwt.verify(forge({ sub: DRIVER, iss: ISSUER, aud: 'other', exp })))).toBe('bad_claims');
    expect(reason(() => jwt.verify(forge({ iss: ISSUER, aud: AUDIENCE, exp })))).toBe('bad_claims');
  });

  it('rotates keys: tokens signed with a previous secret still verify, unknown keys do not', () => {
    const old = new JwtService(S1).issue(DRIVER);
    const rotated = new JwtService(S2, [S1]);
    expect(rotated.verify(old).sub).toBe(DRIVER);
    expect(rotated.verify(rotated.issue(DRIVER)).sub).toBe(DRIVER);
    expect(reason(() => new JwtService(S2).verify(old))).toBe('unknown_kid');
  });

  it('refuses secrets shorter than 32 bytes', () => {
    expect(() => new JwtService('short')).toThrow(/32 bytes/);
  });
});
