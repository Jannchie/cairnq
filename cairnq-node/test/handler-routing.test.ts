// A worker must not claim work it cannot run.
//
// `claim` filters by queue, so two workers with different handler sets on one
// queue both see every task. A worker that wins a task it has no handler for used
// to fail it permanently — which is exactly the mixed-language deployment the
// README sells (a Python API next to a TypeScript worker on the default queue),
// and it destroyed whichever tasks the wrong worker happened to win the race for.
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CairnQ, Worker, type Task } from "../src/index.js";
import { freshDbPath, sleep, waitFor } from "./helpers.js";

let dbPath: string;
let client: CairnQ;

beforeEach(async () => {
  dbPath = freshDbPath();
  client = CairnQ.sqlite(dbPath);
  await client.connect();
});

afterEach(async () => {
  await client.close();
});

describe("handler routing", () => {
  it("leaves tasks it cannot run for the worker that can", async () => {
    const alpha = Worker.sqlite(dbPath, { pollIntervalMs: 10 });
    const beta = Worker.sqlite(dbPath, { pollIntervalMs: 10 });
    alpha.task("alpha", async () => ({ by: "alpha" }));
    beta.task("beta", async () => ({ by: "beta" }));

    let final: (Task | null)[] = [];
    await alpha.background(() =>
      beta.background(async () => {
        // Enough tasks that "the right worker won every race" is not a plausible
        // explanation for them all succeeding.
        const ids: string[] = [];
        for (let i = 0; i < 20; i++) ids.push((await client.submit("beta", { i })).id);
        await waitFor(async () => {
          const all = await Promise.all(ids.map((id) => client.get(id)));
          return all.every((t) => t !== null && t.completed_at_ms !== null);
        }, 10_000);
        final = await Promise.all(ids.map((id) => client.get(id)));
      }),
    );

    const lost = final.filter((t) => t?.status !== "succeeded");
    expect(lost, `tasks lost to the wrong worker: ${JSON.stringify(lost)}`).toEqual([]);
    expect(final.every((t) => t?.result?.by === "beta")).toBe(true);
  });

  it("claims nothing when no handler is registered", async () => {
    // The degenerate case of the same rule: nothing registered, nothing claimed —
    // rather than claiming everything and failing all of it.
    const idle = Worker.sqlite(dbPath, { pollIntervalMs: 10 });
    let current: Task | null = null;
    await idle.background(async () => {
      const task = await client.submit("job", {});
      await sleep(300);
      current = await client.get(task.id);
    });

    expect(current!.status, "an empty worker took the task").toBe("queued");
    expect(current!.attempt).toBe(0);
  });
});
