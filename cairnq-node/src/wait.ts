import { TaskTimeout } from "./errors.js";
import { nowMs } from "./ids.js";
import { isTerminal, type Task } from "./models.js";
import type { TaskStore } from "./store/base.js";

export const DEFAULT_POLL_MS = 100;
export const MAX_POLL_MS = 500;
const GROWTH = 1.5;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export interface PollOptions {
  timeoutMs: number;
  pollMs?: number;
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
 * Poll `read` until it yields a terminal task, or the timeout elapses.
 *
 * `wake` is what the loop sleeps on between reads: a store with a push channel
 * (Postgres) cuts it short when the task goes terminal, but the re-read is the
 * source of truth either way, so a plain sleep is always a correct answer.
 */
async function poll(
  read: () => Promise<Task | null>,
  wake: (task: Task | null, ms: number) => Promise<void>,
  subject: string,
  key: string | null,
  { timeoutMs, pollMs = DEFAULT_POLL_MS, maxPollMs = MAX_POLL_MS }: PollOptions,
): Promise<Task> {
  const deadline = nowMs() + timeoutMs;
  let interval = pollMs;
  for (;;) {
    const task = await read();
    if (task && isTerminal(task)) return task;
    const remaining = deadline - nowMs();
    if (remaining <= 0) throw new TaskTimeout(task?.id ?? subject, { timeoutMs, task, key });
    await wake(task, Math.min(interval, remaining));
    interval = nextPollMs(interval, maxPollMs);
  }
}

/** Poll get() until terminal or timeout. Returns the terminal Task (any status).
 * Throws TaskTimeout, leaving the task running. `pollMs` is the *first* interval;
 * it backs off towards `maxPollMs`. */
export function pollWait(store: TaskStore, taskId: string, opts: PollOptions): Promise<Task> {
  return poll(
    () => store.get(taskId),
    (_task, ms) => store.taskDoneWake(taskId, ms),
    taskId,
    null,
    opts,
  );
}

/**
 * The same wait, following a key instead of an id.
 *
 * The key is re-resolved on every read, because that is what a key means: a
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
    () => store.getByKey(key),
    (task, ms) => (task ? store.taskDoneWake(task.id, ms) : sleep(ms)),
    key,
    key,
    opts,
  );
}
