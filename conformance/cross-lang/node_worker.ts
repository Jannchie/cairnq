// Cross-language E2E: a Node worker. Handles notification.send; runs until SIGTERM.
import { Worker } from "../../cairnq-node/src/index.js";

const db = process.argv[2];
const worker = Worker.sqlite(db, {
  queues: ["default", "gpu", "io"],
  concurrency: 4,
  pollIntervalMs: 50,
  leaseMs: 10_000,
});

worker.task("notification.send", async (ctx, payload) => {
  await ctx.progress(0.5, "sending");
  return { sent: true, to: payload.userId, engine: "node" };
});

console.log("NODE_WORKER_READY");
await worker.serve(); // blocking; closes cleanly on SIGTERM
