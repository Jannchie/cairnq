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
