import { TaskTimeout } from "./errors.js";
import { nowMs } from "./ids.js";
import { isTerminal, type Task } from "./models.js";
import type { TaskStore } from "./store/base.js";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Poll get() until terminal or timeout. Returns the terminal Task (any status).
 * Throws TaskTimeout, leaving the task running. */
export async function pollWait(
  store: TaskStore,
  taskId: string,
  { timeoutMs, pollMs = 150 }: { timeoutMs: number; pollMs?: number },
): Promise<Task> {
  const deadline = nowMs() + timeoutMs;
  for (;;) {
    const task = await store.get(taskId);
    if (task && isTerminal(task)) return task;
    const remaining = deadline - nowMs();
    if (remaining <= 0) throw new TaskTimeout(taskId);
    await sleep(Math.min(pollMs, remaining));
  }
}
