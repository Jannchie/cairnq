import type { TaskStore } from "./store/base.js";

/** Sweep every hour unless asked otherwise — often enough that a queue with a
 * day of retention never carries more than an hour of extra rows, rare enough
 * that the sweep is invisible next to the task traffic. */
const DEFAULT_INTERVAL_MS = 3_600_000;
/** Rows per purge statement. The same bound `purge` defaults to: big enough that
 * a backlog drains in few statements, small enough that each is a short write. */
const DEFAULT_LIMIT = 1_000;

/**
 * Deletes terminal tasks on a schedule, for as long as the handle is open —
 * `purge(olderThanMs)` on a timer, which is the whole mechanism.
 *
 * `purge` exists because nothing else in CairnQ removes rows, and a queue whose
 * payloads carry real data — an image, a document, a batch of embeddings — turns
 * that into a disk leak measured in gigabytes per backfill. Every deployment
 * that runs longer than a demo needs the sweep; leaving it to an external
 * scheduler means the leak is the default and remembering is the opt-in.
 * A deployment whose retention is tiered (per queue, per status) calls `purge`
 * with those filters from its own scheduler instead.
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
  /**
   * Resolved by stop(); every sleep races it. One signal rather than a handle to
   * the current sleep, because there can be more than one: `sweep()` is public
   * and meant to be called directly for an on-demand drain, and its
   * between-batches yield is a sleep of its own. A single handle let that sleep
   * overwrite the scheduled loop's — and then clear it — leaving stop() nothing
   * to wake and close() blocked until the whole interval (an hour, by default)
   * ran out. Same shape as Worker's `stopped$`, and as the Python twin's
   * asyncio.Event.
   */
  private stopSignal!: Promise<void>;
  private wake!: () => void;
  /** The loop itself, awaited by stop() so no purge outlives the store. */
  private loop: Promise<void> | null = null;
  private readonly intervalMs: number;

  constructor(
    private readonly store: TaskStore,
    private readonly olderThanMs: number,
    opts: { intervalMs?: number } = {},
  ) {
    if (!Number.isFinite(olderThanMs) || olderThanMs < 0) {
      throw new Error(`retentionMs must be >= 0, got ${olderThanMs}`);
    }
    this.intervalMs = opts.intervalMs ?? DEFAULT_INTERVAL_MS;
    if (!Number.isFinite(this.intervalMs) || this.intervalMs < 1) {
      throw new Error(`retention intervalMs must be >= 1, got ${this.intervalMs}`);
    }
    this.arm();
  }

  start(): void {
    if (this.active) return;
    this.active = true;
    // No reset here: stop() clears `stopping` and re-arms the signal before it
    // returns, and the constructor arms the first one — so a sweeper reaching
    // this line always already has a fresh signal and a clear flag. (The Python
    // twin has never had a reset here, for the same reason.)
    this.loop = this.run();
  }

  /** Mint a fresh stop signal. */
  private arm(): void {
    this.stopSignal = new Promise<void>((resolve) => (this.wake = resolve));
  }

  /** Stop sweeping and wait for the sweep in flight, if any. */
  async stop(): Promise<void> {
    this.stopping = true;
    this.active = false;
    this.wake();
    await this.loop;
    this.loop = null;
    // Cleared, and the signal re-armed, now that the loop is provably gone.
    // `stopping` is how a sweep in flight cuts itself short, so leaving it set
    // would silently truncate a later on-demand sweep() — a supported call — to
    // its first batch. Leaving the signal spent would be the subtler half of the
    // same bug: that sweep's between-batches yield would resolve on a microtask
    // instead of handing the event loop back, so a long drain would starve the
    // submits and claims sharing this process — exactly what the yield is for.
    this.stopping = false;
    this.arm();
  }

  private async run(): Promise<void> {
    // Sleep first: a process that restarts often would otherwise purge on every
    // boot, which is a write burst exactly when the store is busiest.
    while (!this.stopping) {
      await this.sleep(this.intervalMs);
      if (this.stopping) return;
      try {
        await this.sweep();
      } catch {
        // A purge that failed because the database was busy is not a reason to
        // stop retaining — the next sweep runs on schedule regardless.
      }
    }
  }

  /**
   * Delete everything past the cutoff now, in bounded batches, and return how
   * many rows went. The scheduled loop calls this; call it directly to drain on
   * demand — after a backfill, or from a maintenance command.
   */
  async sweep(): Promise<number> {
    let deleted = 0;
    for (;;) {
      const ids = await this.store.purge({ olderThanMs: this.olderThanMs, limit: DEFAULT_LIMIT });
      deleted += ids.length;
      if (this.stopping) return deleted;
      if (ids.length < DEFAULT_LIMIT) break;
      // Hand the loop back between batches: a large drain must not starve the
      // submits and claims sharing this process. Ref'd — see sleep().
      await this.sleep(0, true);
    }
    return deleted;
  }

  /**
   * Sleep, interruptible by stop().
   *
   * `holdProcess` is the same distinction the store draws between `claimWake`
   * and `taskDoneWake`: a wait nobody is awaiting must not hold the process
   * open, and a wait somebody IS awaiting must. The scheduled loop's interval is
   * the first — retention is housekeeping, and a pending sweep must never be the
   * reason a process refuses to exit. `sweep()`'s between-batches yield is the
   * second: its caller is awaiting the drain, and an unref'd timer there let
   * Node decide the loop was idle and exit mid-drain, leaving that promise
   * unsettled forever (a maintenance command that swept two rows of seven and
   * exited 13). Python's twin never had this, `asyncio.sleep` having no unref.
   */
  private sleep(ms: number, holdProcess = false): Promise<void> {
    let timer: NodeJS.Timeout;
    const nap = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, ms);
      if (!holdProcess) timer.unref?.();
    });
    // Clear the timer whichever side wins, so a stop is never followed by a
    // leftover sweep timer.
    return Promise.race([nap, this.stopSignal]).finally(() => clearTimeout(timer));
  }
}
