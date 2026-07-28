// Who owns the process's signals.
//
// serve() is the standalone entry point — it owns the process, so it is the one
// that may take SIGINT/SIGTERM. run() / background() embed the worker in someone
// else's process (the API server it ships next to), where a leftover listener
// suppresses Node's default Ctrl-C handling for the host, long after the worker
// is done.
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { Worker } from "../src/index.js";
import { freshDbPath, sleep, waitFor } from "./helpers.js";

let dbPath: string;
let baseline: { SIGINT: number; SIGTERM: number };

beforeEach(() => {
  dbPath = freshDbPath();
  baseline = {
    SIGINT: process.listenerCount("SIGINT"),
    SIGTERM: process.listenerCount("SIGTERM"),
  };
});

afterEach(() => {
  expect(process.listenerCount("SIGINT")).toBe(baseline.SIGINT);
  expect(process.listenerCount("SIGTERM")).toBe(baseline.SIGTERM);
});

describe("worker signals", () => {
  it("leaves the host's signal listeners alone while embedded", async () => {
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 10 });
    worker.task("job", async () => ({}));

    await worker.background(async () => {
      await sleep(100);
      expect(process.listenerCount("SIGINT")).toBe(baseline.SIGINT);
      expect(process.listenerCount("SIGTERM")).toBe(baseline.SIGTERM);
    });
    // The afterEach hook re-checks: nothing may outlive the worker either.
  });

  it("stops on a signal and takes its listeners back off", async () => {
    // The other half of the same rule: moving the handlers into serve() must not
    // cost serve() the shutdown it documents.
    const worker = Worker.sqlite(dbPath, { pollIntervalMs: 10 });
    worker.task("job", async () => ({}));

    const served = worker.serve();
    await waitFor(() => process.listenerCount("SIGTERM") > baseline.SIGTERM);
    process.emit("SIGTERM" as never);
    await served;
  });
});
