import { randomBytes } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { decryptToken, encryptToken, tokenKey } from '../../src/token-crypto';

const key = randomBytes(32);
describe('token-crypto', () => {
  it('round-trips', () => expect(decryptToken(encryptToken('tok_visa_4242', key), key)).toBe('tok_visa_4242'));
  it('encrypts the same token differently each time, with the v1 prefix', () => {
    const a = encryptToken('tok', key);
    expect(a.startsWith('v1:')).toBe(true);
    expect(a).not.toBe(encryptToken('tok', key));
  });
  it('throws on a tampered ciphertext', () => {
    const s = encryptToken('tok_visa_4242', key);
    const i = s.lastIndexOf(':') + 2;
    expect(() => decryptToken(s.slice(0, i) + (s[i] === 'A' ? 'B' : 'A') + s.slice(i + 1), key)).toThrow(/authenticate|malformed/);
  });
  it('passes a legacy plaintext value through', () => expect(decryptToken('tok_legacy', key)).toBe('tok_legacy'));
  it('accepts only a 32-byte key', () => {
    expect(tokenKey(key.toString('base64'))).toHaveLength(32);
    expect(() => tokenKey(randomBytes(16).toString('base64'))).toThrow('32 bytes');
  });
  it('rejects a truncated tag and a malformed value', () => {
    const s = encryptToken('tok_visa_4242', key);
    const [iv, body] = s.slice(3).split(':');
    const raw = Buffer.from(body as string, 'base64');
    expect(() => decryptToken(`v1:${iv}:${raw.subarray(0, raw.length - 4).toString('base64')}`, key)).toThrow(/authenticate|malformed/); // tag cut to 12 bytes
    expect(() => decryptToken(`v1:${iv}:${raw.subarray(0, 8).toString('base64')}`, key)).toThrow(/authenticate|malformed/); // shorter than a tag
    expect(() => decryptToken('v1:AAAA', key)).toThrow(/authenticate|malformed/);
  });
});
