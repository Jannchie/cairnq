import type { PurgeInput, TaskStore } from "./store/base.js";

/** Sweep every hour unless asked otherwise — often enough that a queue with a
 * day of retention never carries more than an hour of extra rows, rare enough
 * that the sweep is invisible next to the task traffic. */
const DEFAULT_INTERVAL_MS = 3_600_000;
/** Rows per purge statement. The same bound `purge` defaults to: big enough that
 * a backlog drains in few statements, small enough that each is a short write. */
const DEFAULT_LIMIT = 1_000;

export interface RetentionOptions {
  /**
   * How long a terminal task is kept after it finished. Required: there is no
   * safe default for how long someone else's results stay readable.
   */
  olderThanMs: number;
  /** Time between sweeps. Default 3_600_000 (one hour). */
  intervalMs?: number;
  /** Rows deleted per statement while draining. Default 1_000. */
  limit?: number;
  /**
   * Called for a sweep that threw. The next sweep runs on schedule regardless —
   * a purge that failed because the database was busy is not a reason to stop
   * retaining — so without this a store quietly stops being swept. Must not throw.
   */
  onError?: (err: unknown) => void;
}

/**
 * Deletes terminal tasks on a schedule, for as long as the handle is open.
 *
 * `purge` exists because nothing else in CairnQ removes rows, and a queue whose
 * payloads carry real data — an image, a document, a batch of embeddings — turns
 * that into a disk leak measured in gigabytes per backfill. Every deployment
 * that runs longer than a demo needs the sweep; leaving it to an external
 * scheduler means the leak is the default and remembering is the opt-in.
 *
 * It sweeps in bounded batches with a yield between them, so draining a backlog
 * that accumulated while nothing was sweeping stays a sequence of short writes
 * rather than one long one — on SQLite that matters, since a long write holds
 * the single write lock against every producer and worker on the file.
 */
export class RetentionSweeper {
  /** Whether the scheduled loop is running. */
  private active = false;
  /** Set by stop(), so a drain in progress can cut itself short too. */
  private stopping = false;
  /** Resolves the current sleep early, so stop() need not wait out an interval. */
  private wake: (() => void) | null = null;
  /** The loop itself, awaited by stop() so no purge outlives the store. */
  private loop: Promise<void> | null = null;
  private readonly intervalMs: number;
  private readonly purgeInput: PurgeInput;

  constructor(
    private readonly store: TaskStore,
    private readonly opts: RetentionOptions,
  ) {
    if (!Number.isFinite(opts.olderThanMs) || opts.olderThanMs < 0) {
      throw new Error(`retention.olderThanMs must be >= 0, got ${opts.olderThanMs}`);
    }
    this.intervalMs = opts.intervalMs ?? DEFAULT_INTERVAL_MS;
    if (!Number.isFinite(this.intervalMs) || this.intervalMs < 1) {
      throw new Error(`retention.intervalMs must be >= 1, got ${this.intervalMs}`);
    }
    this.purgeInput = { olderThanMs: opts.olderThanMs, limit: opts.limit ?? DEFAULT_LIMIT };
  }

  start(): void {
    if (this.active) return;
    this.active = true;
    this.stopping = false;
    this.loop = this.run();
  }

  /** Stop sweeping and wait for the sweep in flight, if any. */
  async stop(): Promise<void> {
    this.stopping = true;
    this.active = false;
    this.wake?.();
    await this.loop;
    this.loop = null;
  }

  private async run(): Promise<void> {
    // Sleep first: a process that restarts often would otherwise purge on every
    // boot, which is a write burst exactly when the store is busiest.
    while (!this.stopping) {
      await this.sleep(this.intervalMs);
      if (this.stopping) return;
      try {
        await this.sweep();
      } catch (err) {
        try {
          this.opts.onError?.(err);
        } catch {
          // A reporting hook must never take the sweep down with it — the same
          // rule the worker's onError follows.
        }
      }
    }
  }

  /**
   * Delete everything past the cutoff now, in bounded batches, and return how
   * many rows went. The scheduled loop calls this; call it directly to drain on
   * demand — after a backfill, or from a maintenance command.
   */
  async sweep(): Promise<number> {
    const limit = this.purgeInput.limit as number;
    let deleted = 0;
    for (;;) {
      const ids = await this.store.purge(this.purgeInput);
      deleted += ids.length;
      if (ids.length < limit || this.stopping) return deleted;
      // Hand the loop back between batches: a large drain must not starve the
      // submits and claims sharing this process.
      await this.sleep(0);
    }
  }

  /** Sleep, interruptible by stop(). Unref'd: retention is housekeeping, and a
   * pending sweep must never be the reason a process refuses to exit. */
  private sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, ms);
      timer.unref?.();
      this.wake = () => {
        clearTimeout(timer);
        resolve();
      };
    }).finally(() => {
      this.wake = null;
    });
  }
}
