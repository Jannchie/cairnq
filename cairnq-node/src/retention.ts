import type { TerminalStatus } from "./models.js";
import { validatePurgeInput, type PurgeInput, type TaskStore } from "./store/base.js";

/** Sweep every hour unless asked otherwise — often enough that a queue with a
 * day of retention never carries more than an hour of extra rows, rare enough
 * that the sweep is invisible next to the task traffic. */
const DEFAULT_INTERVAL_MS = 3_600_000;
/** Rows per purge statement. The same bound `purge` defaults to: big enough that
 * a backlog drains in few statements, small enough that each is a short write. */
const DEFAULT_LIMIT = 1_000;

/** Per-status cutoffs. A status left out is never swept — granular retention is
 * an explicit statement of what may go, not a default for what wasn't named. */
export type RetentionCutoffs = Partial<Record<TerminalStatus, number>>;

/**
 * One "these rows may go after this long" statement. Each field left out widens
 * what the rule covers; `olderThanMs` is the only required one.
 *
 * A rule is one `purge` call's filters, so the fields are exactly `PurgeInput`'s
 * — deliberately, since a rule the sweeper can express but the store cannot
 * enforce would be a lie about what is being deleted.
 */
export interface RetentionRule {
  /** Only this queue. Absent means every queue. */
  queue?: string;
  /** Only this terminal status. Absent means all three. */
  status?: TerminalStatus;
  /** Only this task name. Absent means every name. */
  name?: string;
  /** How long a row matching this rule is kept after it finished. */
  olderThanMs: number;
}

export interface RetentionOptions {
  /**
   * How long a terminal task is kept after it finished. Required: there is no
   * safe default for how long someone else's results stay readable.
   *
   * Three forms, widening as the deployment does:
   *
   * - A number keeps every terminal row the same time.
   * - A per-status map tiers by outcome — a succeeded row is spent once its
   *   result is consumed, a failed one is worth keeping for diagnosis:
   *   `{ succeeded: 300_000, failed: 86_400_000 }`. A status left out is never
   *   swept.
   * - An array of rules tiers by anything `purge` can filter on, which is what a
   *   store shared by two workloads needs — the recommended way for two
   *   languages to coordinate is one installation, and an RPC queue read once
   *   has nothing in common with a durable queue kept for a week:
   *   `[{ queue: "rpc", olderThanMs: 300_000 },
   *      { queue: "jobs", status: "failed", olderThanMs: 604_800_000 }]`.
   *   Rules are independent, each its own sweep — nothing a rule does not match
   *   is swept, and rules that overlap simply delete the same row once.
   */
  olderThanMs: number | RetentionCutoffs | RetentionRule[];
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

/** The three `olderThanMs` forms as the one form the sweep runs on. The number
 * and the per-status map are the rule array's common cases spelled shorter, so
 * they are widened here rather than handled separately downstream. */
function toRules(spec: number | RetentionCutoffs | RetentionRule[]): RetentionRule[] {
  if (typeof spec === "number") return [{ olderThanMs: spec }];
  if (Array.isArray(spec)) return spec;
  return (Object.entries(spec) as [TerminalStatus, number][]).map(([status, olderThanMs]) => ({
    status,
    olderThanMs,
  }));
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
  /** Rows per purge statement while draining — see DEFAULT_LIMIT. */
  private readonly limit: number;
  /** One purge per rule: a lone entry for a number, one per status for a map,
   * one per element for an array. */
  private readonly purgeInputs: PurgeInput[];

  constructor(
    private readonly store: TaskStore,
    private readonly opts: RetentionOptions,
  ) {
    this.intervalMs = opts.intervalMs ?? DEFAULT_INTERVAL_MS;
    if (!Number.isFinite(this.intervalMs) || this.intervalMs < 1) {
      throw new Error(`retention.intervalMs must be >= 1, got ${this.intervalMs}`);
    }
    this.limit = opts.limit ?? DEFAULT_LIMIT;
    const rules = toRules(opts.olderThanMs);
    // An empty map or array retains nothing and sweeps nothing — almost
    // certainly a bug upstream of this call, so refuse it rather than silently
    // never purging.
    if (!rules.length) {
      throw new Error("retention.olderThanMs must name at least one rule");
    }
    this.arm();
    this.purgeInputs = rules.map((rule) => ({ ...rule, limit: this.limit }));
    // Fail fast on the store's own purge rules (terminal status, cutoff >= 0):
    // the sweep runs an hour from now, and its errors only surface via onError.
    for (const input of this.purgeInputs) validatePurgeInput(input);
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
    let deleted = 0;
    for (const input of this.purgeInputs) {
      for (;;) {
        const ids = await this.store.purge(input);
        deleted += ids.length;
        if (this.stopping) return deleted;
        if (ids.length < this.limit) break;
        // Hand the loop back between batches: a large drain must not starve the
        // submits and claims sharing this process.
        await this.sleep(0);
      }
    }
    return deleted;
  }

  /** Sleep, interruptible by stop(). Unref'd: retention is housekeeping, and a
   * pending sweep must never be the reason a process refuses to exit. */
  private sleep(ms: number): Promise<void> {
    let timer: NodeJS.Timeout;
    const nap = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, ms);
      timer.unref?.();
    });
    // Clear the timer whichever side wins, so a stop is never followed by a
    // leftover sweep timer.
    return Promise.race([nap, this.stopSignal]).finally(() => clearTimeout(timer));
  }
}
