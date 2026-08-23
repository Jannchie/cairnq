import { type CairnQ, isTerminal } from "../src/index.js";

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** A path to a database of its own, in a fresh temp dir. */
export function freshDbPath(): string {
  return join(mkdtempSync(join(tmpdir(), "cairnq-")), "tasks.db");
}

/**
 * Wait until `cond` holds, or the timeout elapses — the timeout is not a failure
 * here, it just stops waiting so the test's own assertion reports what went wrong
 * instead of an opaque timeout.
 */
export async function waitFor(
  cond: () => boolean | Promise<boolean>,
  timeoutMs = 3_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await sleep(10);
  }
}

/**
 * Whether every id has reached a terminal state. The predicate worker tests wait
 * on, so `waitFor(() => allTerminal(client, ids))` reads as the thing it is
 * rather than being respelled per file.
 */
export async function allTerminal(client: CairnQ, ids: string[]): Promise<boolean> {
  for (const id of ids) {
    const task = await client.get(id);
    if (!task || !isTerminal(task)) return false;
  }
  return true;
}

/** Claim the next queued task and succeed it, the way a worker elsewhere would. */
export async function succeedNext(
  c: CairnQ,
  result: unknown = {},
  queue = "default",
): Promise<void> {
  const [task] = await c.store.claim({ queues: [queue], workerId: "w1", leaseMs: 5_000 });
  await c.store.succeed({ taskId: task.id, workerId: "w1", result });
}

/** Run a task to `succeeded` — submitted here, finished as if by a worker. */
export async function finishOne(
  c: CairnQ,
  opts: { name?: string; queue?: string } = {},
): Promise<string> {
  const queue = opts.queue ?? "default";
  const task = await c.submit(opts.name ?? "job", {}, { queue });
  await succeedNext(c, {}, queue);
  return task.id;
}

/** Run a task to terminal `failed`. */
export async function failOne(
  c: CairnQ,
  opts: { name?: string; queue?: string } = {},
): Promise<string> {
  const queue = opts.queue ?? "default";
  const task = await c.submit(opts.name ?? "job", {}, { queue });
  const [claimed] = await c.store.claim({ queues: [queue], workerId: "w1", leaseMs: 5_000 });
  await c.store.fail({
    taskId: claimed.id,
    workerId: "w1",
    error: { message: "boom" },
    retryable: false,
  });
  return task.id;
}

/**
 * A cairnq_tasks row, for the tests that exercise row mapping directly.
 *
 * One fixture rather than one per file: the shape is a fact about the SCHEMA, so
 * three copies meant a migration adding a column had three places to reach — and
 * a signature change to rowToTask had three call sites to find, which is exactly
 * how one of them was left on the old signature, silently taking the wrong branch
 * while its assertions stayed green.
 *
 * `jsonIsText` picks which wire form the JSON columns are written in, so a caller
 * can hand the result to rowToTask with the matching flag and have the two agree
 * by construction.
 */
export function taskRow(
  overrides: Record<string, unknown> = {},
  jsonIsText = true,
): Record<string, unknown> {
  const empty = jsonIsText ? "{}" : {};
  return {
    id: "t1", name: "n", queue: "default", status: "queued",
    payload: empty, metadata: empty, result: null, error: null,
    progress: null, message: null, attempt: 0, max_attempts: 3, priority: 0,
    worker_id: null, lease_until_ms: null, run_at_ms: 0,
    cancel_requested_at_ms: null, parent_id: null, root_id: null,
    correlation_id: null, created_at_ms: 0, updated_at_ms: 0, completed_at_ms: 0,
    ...overrides,
  };
}
