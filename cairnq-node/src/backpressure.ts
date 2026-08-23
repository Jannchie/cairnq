import { setTimeout as delay } from "node:timers/promises";
import { QueueFull } from "./errors.js";
import type { TaskStore } from "./store/base.js";

/**
 * Most tasks a producer may enqueue on one probe's word.
 *
 * The gate probes only when its headroom runs out, so this is what the check
 * costs amortized: one bounded index read per MAX_GRANT submits. It also bounds
 * how far the limit can be overshot — see the class docstring on why several
 * producers make this a soft limit, and why that overshoot is (N-1) * MAX_GRANT
 * rather than unbounded.
 */
const MAX_GRANT = 64;

// Named for probing, not polling: wait.ts exports DEFAULT_POLL_MS / MAX_POLL_MS
// for the get() loop behind wait(), an order of magnitude tighter and answering
// a different question. Two constants of the same name in one SDK would be read
// as one policy.
const INITIAL_PROBE_INTERVAL_MS = 250;
const MAX_PROBE_INTERVAL_MS = 5_000;
const DEFAULT_MAX_WAIT_MS = 600_000;

/** Per-queue depth limits. A number applies one limit to every queue; a record
 * gates only the queues it names and leaves the rest unbounded. */
export type QueueDepthLimit = number | Record<string, number>;

export interface BackpressureOptions {
  /** Queued tasks a queue may hold before `submit` blocks. */
  maxQueueDepth: QueueDepthLimit;
  /** How long a blocked submit waits before raising QueueFull. Default 600_000. */
  maxQueueWaitMs?: number;
}

/**
 * Blocks `submit` while a queue is at its depth limit.
 *
 * Without one of these a producer that outruns its workers is only bounded by
 * disk: the backlog grows, every task's queue wait grows with it, and the
 * failure is a database that filled up rather than a producer that slowed down.
 * A queue is the wrong place to buffer an overload — pushing back on the
 * producer is the point.
 *
 * **A soft limit under several producers.** The check is a read followed by a
 * write that other producers can interleave with, and each holds its own grant,
 * so N producers can overshoot the limit by up to (N-1) * MAX_GRANT tasks. Made
 * exact it would need the depth check inside insert_task's transaction, which
 * puts an unbounded-scan predicate on the hot path of every submit and turns
 * concurrent submits into lock contention — a steep price for a bound whose
 * whole purpose is approximate. Size the limit for the pushback you want, not as
 * a capacity assertion.
 */
export class QueueDepthGate {
  /** Remaining grant per queue: submits allowed before the next probe. */
  private readonly headroom = new Map<string, number>();
  /** In-flight probe per queue, so concurrent submits share one read rather
   * than each issuing their own against a queue that is already known full. */
  private readonly probing = new Map<string, Promise<void>>();
  private readonly limits: QueueDepthLimit;
  private readonly maxWaitMs: number;

  constructor(
    private readonly store: TaskStore,
    opts: BackpressureOptions,
  ) {
    this.limits = opts.maxQueueDepth;
    this.maxWaitMs = opts.maxQueueWaitMs ?? DEFAULT_MAX_WAIT_MS;
    if (typeof this.limits === "number") this.validate("*", this.limits);
    else for (const [q, v] of Object.entries(this.limits)) this.validate(q, v);
  }

  private validate(queue: string, limit: number): void {
    // A limit of 0 would block every submit forever, which is never what a
    // caller means; catching it here beats a first submit that hangs for
    // maxQueueWaitMs and then raises.
    if (!Number.isInteger(limit) || limit < 1) {
      throw new Error(`maxQueueDepth for ${queue} must be an integer >= 1, got ${limit}`);
    }
  }

  /** The limit for `queue`, or null when it is not gated. */
  limitFor(queue: string): number | null {
    if (typeof this.limits === "number") return this.limits;
    return this.limits[queue] ?? null;
  }

  /**
   * Consume one unit of headroom for `queue`, waiting for room if it is full.
   * Returns immediately for an ungated queue. Raises QueueFull on timeout,
   * having enqueued nothing.
   */
  async acquire(queue: string): Promise<void> {
    const limit = this.limitFor(queue);
    if (limit == null) return;

    const startedAt = Date.now();
    let waitMs = INITIAL_PROBE_INTERVAL_MS;
    for (;;) {
      const left = this.headroom.get(queue) ?? 0;
      if (left > 0) {
        this.headroom.set(queue, left - 1);
        return;
      }
      await this.probe(queue, limit);
      if ((this.headroom.get(queue) ?? 0) > 0) continue;

      const waited = Date.now() - startedAt;
      if (waited >= this.maxWaitMs) throw new QueueFull(queue, limit, waited);
      // Back off: a queue at its limit will not drain within one poll interval,
      // and re-probing tightly adds read load to a database already behind.
      await delay(Math.min(waitMs, this.maxWaitMs - waited));
      waitMs = Math.min(waitMs * 2, MAX_PROBE_INTERVAL_MS);
    }
  }

  /**
   * Refresh `queue`'s grant from the store, at most one probe in flight.
   *
   * Callers re-read `headroom` afterwards rather than using a returned value:
   * only the caller that started the probe writes the grant, so waiters that
   * joined it cannot overwrite the units already handed out.
   */
  private probe(queue: string, limit: number): Promise<void> {
    let p = this.probing.get(queue);
    if (!p) {
      p = this.store
        .queueDepth(queue, limit)
        .then((headroom) => {
          this.headroom.set(queue, Math.min(headroom, MAX_GRANT));
        })
        .finally(() => this.probing.delete(queue));
      this.probing.set(queue, p);
    }
    return p;
  }
}
