import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';

const PREFIX = 'v1:';
const keys = new Map<string, Buffer>();

/** The AES-256 key from the base64 config value (cached). */
export function tokenKey(b64: string): Buffer {
  let k = keys.get(b64);
  if (!k) {
    k = Buffer.from(b64, 'base64');
    if (k.length !== 32) throw new Error('TOKEN_ENC_KEY must be 32 bytes, base64 encoded');
    keys.set(b64, k);
  }
  return k;
}

/** AES-256-GCM, random 12-byte IV. Stored as `v1:<base64 iv>:<base64 ciphertext+tag>`. */
export function encryptToken(plain: string, key: Buffer): string {
  const iv = randomBytes(12);
  const c = createCipheriv('aes-256-gcm', key, iv);
  const body = Buffer.concat([c.update(plain, 'utf8'), c.final(), c.getAuthTag()]);
  return `${PREFIX}${iv.toString('base64')}:${body.toString('base64')}`;
}

/** Inverse of encryptToken; a value without the `v1:` prefix is a row written before encryption and passes through. */
export function decryptToken(stored: string, key: Buffer): string {
  if (!stored.startsWith(PREFIX)) return stored;
  const [ivB64, bodyB64] = stored.slice(PREFIX.length).split(':');
  const body = Buffer.from(bodyB64 ?? '', 'base64');
  const iv = Buffer.from(ivB64 ?? '', 'base64');
  if (iv.length !== 12 || body.length < 16) throw new Error('malformed encrypted token');
  const d = createDecipheriv('aes-256-gcm', key, iv, { authTagLength: 16 });
  d.setAuthTag(body.subarray(body.length - 16));
  return Buffer.concat([d.update(body.subarray(0, body.length - 16)), d.final()]).toString('utf8');
}
