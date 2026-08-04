/**
 * Retry backoff, in its own module because two callers need it.
 *
 * The worker computes it when a handler's failure ends an attempt; TaskContext
 * computes it when a handler fails one task of a batch itself. Keeping it in
 * worker.ts would make context.ts import the module that imports it.
 */

export const DEFAULT_RETRY_BACKOFF_MS = 1_000;
export const DEFAULT_RETRY_BACKOFF_MAX_MS = 30_000;

/**
 * Exponential backoff with equal jitter: the window doubles per attempt up to
 * `maxMs`, and the delay lands uniformly in its upper half, `[w/2, w)`.
 *
 * The jitter is what keeps a fleet from retrying in lockstep. Failures align
 * when the downstream fails fast enough that a whole concurrency batch raises
 * at once (connection refused, DNS gone), and capped exponential backoff then
 * *preserves* that alignment — once every task sits at `maxMs`, they all retry
 * on the same beat forever. Spreading over half the window breaks it; keeping
 * the lower half as a floor means jitter never shortens the wait to less than
 * half of what plain exponential backoff would have asked for.
 *
 * `rand` is injected so tests can pin an exact delay.
 */
export function retryDelayMs(
  attempt: number,
  baseMs: number,
  maxMs: number,
  rand: () => number = Math.random,
): number {
  if (baseMs <= 0) return 0;
  const exponent = Math.max(0, attempt - 1);
  const window = Math.min(maxMs, baseMs * 2 ** exponent);
  const floor = Math.floor(window / 2);
  return floor + Math.floor(rand() * (window - floor));
}

/**
 * The delay a `fail` write should carry. Not just the backoff: a permanent
 * failure is never re-run, so it always delays 0. Both settlement paths — the
 * worker's and a handler's `ctx.fail` — go through this, so they cannot end up
 * backing off differently.
 */
export function failDelayMs(
  attempt: number,
  retryable: boolean,
  baseMs: number,
  maxMs: number,
  rand: () => number = Math.random,
): number {
  return retryable ? retryDelayMs(attempt, baseMs, maxMs, rand) : 0;
}
