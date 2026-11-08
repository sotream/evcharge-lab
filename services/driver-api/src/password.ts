import { randomBytes, scrypt, timingSafeEqual } from 'node:crypto';

// ADR 0010: scrypt N=16384, r=8, p=1, 16-byte salt, 64-byte key.
const N = 16384;
const R = 8;
const P = 1;
const SALT_LEN = 16;
const KEY_LEN = 64;

const derive = (password: string, salt: Buffer, n: number, r: number, p: number): Promise<Buffer> =>
  new Promise((resolve, reject) => scrypt(password.normalize('NFKC'), salt, KEY_LEN, { N: n, r, p }, (err, key) => (err ? reject(err) : resolve(key))));

/** Returns `scrypt$N$r$p$saltB64$keyB64`; parameters are stored so they can be raised later. */
export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(SALT_LEN);
  const key = await derive(password, salt, N, R, P);
  return `scrypt$${N}$${R}$${P}$${salt.toString('base64')}$${key.toString('base64')}`;
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const [alg, n, r, p, salt, key] = stored.split('$');
  if (alg !== 'scrypt' || !n || !r || !p || !salt || !key) return false;
  const expected = Buffer.from(key, 'base64');
  const actual = await derive(password, Buffer.from(salt, 'base64'), Number(n), Number(r), Number(p));
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}

/** A real hash to compare against when the email is unknown, so timing does not reveal which emails exist. */
export const DUMMY_HASH = hashPassword('dummy-password-for-timing');
