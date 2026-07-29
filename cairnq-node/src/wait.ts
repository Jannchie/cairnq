import { TaskTimeout } from "./errors.js";
import { nowMs } from "./ids.js";
import { isTerminal, type Task } from "./models.js";
import type { TaskStore } from "./store/base.js";

export const DEFAULT_POLL_MS = 100;
export const MAX_POLL_MS = 500;
const GROWTH = 1.5;

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Grow the polling interval towards the ceiling.
 *
 * wait() has no idea whether the task takes 50ms or an hour. Starting tight keeps
 * short tasks snappy; growing keeps a long wait from costing a read every 100ms
 * for its whole duration.
 */
export function nextPollMs(current: number, maxMs: number): number {
  return Math.min(maxMs, Math.floor(current * GROWTH));
}

/** Poll get() until terminal or timeout. Returns the terminal Task (any status).
 * Throws TaskTimeout, leaving the task running. `pollMs` is the *first* interval;
 * it backs off towards `maxPollMs`. */
export async function pollWait(
  store: TaskStore,
  taskId: string,
  {
    timeoutMs,
    pollMs = DEFAULT_POLL_MS,
    maxPollMs = MAX_POLL_MS,
  }: { timeoutMs: number; pollMs?: number; maxPollMs?: number },
): Promise<Task> {
  const deadline = nowMs() + timeoutMs;
  let interval = pollMs;
  for (;;) {
    const task = await store.get(taskId);
    if (task && isTerminal(task)) return task;
    const remaining = deadline - nowMs();
    if (remaining <= 0) throw new TaskTimeout(taskId);
    // A store with a push channel (Postgres) cuts the sleep short when the task
    // goes terminal; the re-get above stays the source of truth either way.
    const ms = Math.min(interval, remaining);
    await (store.taskDoneWake(taskId, ms) ?? sleep(ms));
    interval = nextPollMs(interval, maxPollMs);
  }
}
