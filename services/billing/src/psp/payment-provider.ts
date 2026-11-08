/**
 * Port to the payment service provider. A real PSP (Stripe, Adyen) plugs in here: both support
 * idempotency keys, which ADR 0007 depends on.
 *
 * Contract:
 *  - every call carries an idempotency key; repeating a call with the same key returns the stored
 *    outcome of the first one and never repeats the effect;
 *  - `PspTimeoutError` means the outcome is UNKNOWN (the effect may or may not have happened). The
 *    caller retries with the SAME key;
 *  - `PspDeclinedError` is a definite, final refusal for that key;
 *  - `PspIdempotencyMismatchError` means the key was already used for a different request. It is a
 *    bug on our side and is never retried.
 */
export interface AuthorizeRequest {
  idempotencyKey: string;
  token: string;
  amountMinor: number;
  currency: string;
}
export interface CaptureRequest {
  idempotencyKey: string;
  /** null: charge the payment method directly (the session had no hold). */
  authorizationId: string | null;
  token: string;
  amountMinor: number;
  currency: string;
}
export interface ReleaseRequest {
  idempotencyKey: string;
  /** The authorization to release, when its id is known. */
  authorizationId?: string;
  /**
   * Void by the idempotency key of the original authorize call, when its answer was lost and the
   * id is unknown. The PSP releases the hold if the authorization exists and refuses a late one if not.
   */
  authorizationKey?: string;
}

export interface PaymentProvider {
  authorize(req: AuthorizeRequest): Promise<{ authorizationId: string }>;
  capture(req: CaptureRequest): Promise<{ pspReference: string; capturedMinor: number }>;
  release(req: ReleaseRequest): Promise<void>;
}

export class PspTimeoutError extends Error {
  override readonly name = 'PspTimeoutError';
}

export class PspIdempotencyMismatchError extends Error {
  override readonly name = 'PspIdempotencyMismatchError';
}

export class PspDeclinedError extends Error {
  override readonly name = 'PspDeclinedError';
  constructor(
    readonly reason: string,
    message = `payment declined: ${reason}`,
  ) {
    super(message);
  }
}
