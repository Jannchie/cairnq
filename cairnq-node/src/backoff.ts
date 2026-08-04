/**
 * Retry backoff, in its own module because two callers need it.
 *
 * The worker computes it when a handler's failure ends an attempt; TaskContext
 * computes it when a handler fails one task of a batch itself. Keeping it in
 * worker.ts would make context.ts import the module that imports it.
 */

export const DEFAULT_RETRY_BACKOFF_MS = 1_000;
export const DEFAULT_RETRY_BACKOFF_MAX_MS = 30_000;

/** Exponential backoff for the next attempt of a task that just failed. */
export function retryDelayMs(attempt: number, baseMs: number, maxMs: number): number {
  if (baseMs <= 0) return 0;
  const exponent = Math.max(0, attempt - 1);
  return Math.min(maxMs, baseMs * 2 ** exponent);
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
): number {
  return retryable ? retryDelayMs(attempt, baseMs, maxMs) : 0;
}
