export type CircuitState = 'closed' | 'open' | 'half-open';

export interface CircuitBreakerOptions {
  /** Consecutive failures that open the circuit. Default 5. */
  failureThreshold?: number;
  /** How long the circuit stays open before a single trial request is let through. Default 10s. */
  openMs?: number;
  now?: () => number;
}

/**
 * Consecutive-failure circuit breaker. closed -> (N failures) -> open -> (openMs) -> half-open:
 * one trial call; success closes, failure re-opens. Callers check `isOpen` to serve fallbacks.
 */
export class CircuitBreaker {
  private failures = 0;
  private openedAt = 0;
  private stateValue: CircuitState = 'closed';
  private trialInFlight = false;
  readonly failureThreshold: number;
  readonly openMs: number;
  private readonly now: () => number;

  constructor(opts: CircuitBreakerOptions = {}) {
    this.failureThreshold = opts.failureThreshold ?? 5;
    this.openMs = opts.openMs ?? 10_000;
    this.now = opts.now ?? Date.now;
  }

  get state(): CircuitState {
    if (this.stateValue === 'open' && this.now() - this.openedAt >= this.openMs) this.stateValue = 'half-open';
    return this.stateValue;
  }

  get isOpen(): boolean {
    return this.state === 'open';
  }

  get consecutiveFailures(): number {
    return this.failures;
  }

  /** True if a call may proceed now. In half-open only one trial call is admitted. */
  tryAcquire(): boolean {
    const s = this.state;
    if (s === 'closed') return true;
    if (s === 'open') return false;
    if (this.trialInFlight) return false;
    this.trialInFlight = true;
    return true;
  }

  onSuccess(): void {
    this.failures = 0;
    this.trialInFlight = false;
    this.stateValue = 'closed';
  }

  onFailure(): void {
    this.trialInFlight = false;
    this.failures++;
    if (this.stateValue === 'half-open' || this.failures >= this.failureThreshold) {
      this.stateValue = 'open';
      this.openedAt = this.now();
    }
  }

  /** A call that neither succeeded nor failed from the breaker's point of view (e.g. 4xx). */
  onNeutral(): void {
    if (this.stateValue === 'half-open') this.onSuccess();
  }

  reset(): void {
    this.onSuccess();
  }
}
