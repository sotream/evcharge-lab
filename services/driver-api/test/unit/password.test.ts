import { describe, expect, it } from 'vitest';
import { hashPassword, verifyPassword } from '../../src/password';

describe('scrypt passwords', () => {
  it('stores N=16384 r=8 p=1 with a 16 byte salt and a 64 byte key', async () => {
    const h = await hashPassword('correct horse battery');
    const [alg, n, r, p, salt, key] = h.split('$') as [string, string, string, string, string, string];
    expect([alg, n, r, p]).toEqual(['scrypt', '16384', '8', '1']);
    expect(Buffer.from(salt, 'base64')).toHaveLength(16);
    expect(Buffer.from(key, 'base64')).toHaveLength(64);
  });

  it('verifies the right password only and salts every hash', async () => {
    const [a, b] = await Promise.all([hashPassword('pw-12345678'), hashPassword('pw-12345678')]);
    expect(a).not.toBe(b);
    expect(await verifyPassword('pw-12345678', a)).toBe(true);
    expect(await verifyPassword('pw-12345679', a)).toBe(false);
    expect(await verifyPassword('pw-12345678', 'garbage')).toBe(false);
  });
});
