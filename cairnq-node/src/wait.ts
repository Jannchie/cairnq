import { TaskTimeout } from "./errors.js";
import { nowMs } from "./ids.js";
import { isTerminal, type Task, type TaskRef } from "./models.js";
import type { TaskStore } from "./store/base.js";

export const DEFAULT_WAIT_TIMEOUT_MS = 30_000;
export const DEFAULT_POLL_MS = 100;
export const MAX_POLL_MS = 500;
const GROWTH = 1.5;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export interface PollOptions {
  timeoutMs: number;
  /** The first poll interval (default 100). */
  pollMs?: number;
  /** Ceiling the poll interval backs off to (default 500). Worth raising for a
   * task known to take minutes — fewer reads — or lowering when shaving the
   * average half-interval of completion-detection latency matters. */
  maxPollMs?: number;
}

/**
 * Grow the polling interval towards the ceiling.
 *
 * wait() has no idea whether the task takes 50ms or an hour. Starting tight keeps
 * short tasks snappy; growing keeps a long wait from costing a read every 100ms
 * for its whole duration. The +1 keeps truncation from pinning tiny intervals:
 * Math.floor(1 * 1.5) === 1 would otherwise never grow past 1.
 */
export function nextPollMs(current: number, maxMs: number): number {
  return Math.min(maxMs, Math.max(current + 1, Math.floor(current * GROWTH)));
}

/**
 * Poll `probe` until it reports a terminal status, then return the full task
 * via `read`; or throw once the timeout elapses.
 *
 * The loop's repeated read is the status-only `probe` (see get_status.sql): a
 * waiting caller asks nothing but "is it finished yet", and re-reading the whole
 * row would drag the payload back — and re-parse it — on every beat for the life
 * of the wait. The full row is read once, when the probe turns terminal or, on
 * the timeout beat, for the error's snapshot. Between the probe and that read
 * the row can vanish (purge) or the key repoint (`replace`); a read that comes
 * back empty or non-terminal is simply not finished, and the loop keeps polling.
 *
 * `wake` is what the loop sleeps on between reads: a store with a push channel
 * (Postgres) cuts it short when the task goes terminal, but the re-probe is the
 * source of truth either way, so a plain sleep is always a correct answer.
 */
async function poll(
  probe: () => Promise<TaskRef | null>,
  read: () => Promise<Task | null>,
  wake: (ref: TaskRef | null, ms: number) => Promise<void>,
  subject: string,
  key: string | null,
  { timeoutMs, pollMs = DEFAULT_POLL_MS, maxPollMs = MAX_POLL_MS }: PollOptions,
): Promise<Task> {
  const deadline = nowMs() + timeoutMs;
  let interval = pollMs;
  for (;;) {
    const ref = await probe();
    const remaining = deadline - nowMs();
    // The one full-read site: when the probe says finished, or on the timeout
    // beat for the error's stuck-in-what-state snapshot. No ref means no row,
    // so there is nothing for a read to add to either case.
    const task = ref && (isTerminal(ref) || remaining <= 0) ? await read() : null;
    if (task && isTerminal(task)) return task;
    if (remaining <= 0) throw new TaskTimeout(ref?.id ?? subject, { timeoutMs, task, key });
    await wake(ref, Math.min(interval, remaining));
    interval = nextPollMs(interval, maxPollMs);
  }
}

/** Poll the task's status until terminal or timeout. Returns the terminal Task
 * (any status). Throws TaskTimeout, leaving the task running. `pollMs` is the
 * *first* interval; it backs off towards `maxPollMs`. */
export function pollWait(store: TaskStore, taskId: string, opts: PollOptions): Promise<Task> {
  return poll(
    () => store.getStatus(taskId),
    () => store.get(taskId),
    (_ref, ms) => store.taskDoneWake(taskId, ms),
    taskId,
    null,
    opts,
  );
}

/**
 * The same wait, following a key instead of an id.
 *
 * The key is re-resolved on every probe, because that is what a key means: a
 * pointer to the task that is *current* under it. A `replace` landing mid-wait
 * moves the wait onto the new task rather than reporting the cancellation of the
 * old one, and a key that points at nothing yet is simply not finished — it
 * polls until something appears, the same way waiting on an id that does not
 * exist yet does.
 *
 * There is nothing to subscribe to before the key resolves, so those naps are
 * plain sleeps; once it resolves, the store's push channel applies as usual.
 */
export function pollWaitByKey(store: TaskStore, key: string, opts: PollOptions): Promise<Task> {
  return poll(
    () => store.getStatusByKey(key),
    () => store.getByKey(key),
    (ref, ms) => (ref ? store.taskDoneWake(ref.id, ms) : sleep(ms)),
    key,
    key,
    opts,
  );
}
