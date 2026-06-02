// Cross-language end-to-end orchestrator. Runs both headline scenarios against a
// shared tasks.db file on this host (deployment mode B):
//   E2E1: TypeScript API submit -> Python worker -> TS reads result
//   E2E2: Python API submit     -> TS worker     -> Python reads result
// Usage: node conformance/cross-lang/run.mjs
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..", "..");
const pyDir = join(repoRoot, "cairnq-py");
const nodeDir = join(repoRoot, "cairnq-node");
const env = { ...process.env, CAIRNQ_PROTOCOL_DIR: join(repoRoot, "cairnq-protocol") };

const pyWorker = ["uv", ["run", "--project", pyDir, "python", join(here, "py_worker.py")]];
const pySubmit = ["uv", ["run", "--project", pyDir, "python", join(here, "py_submit.py")]];
const nodeWorker = ["pnpm", ["--dir", nodeDir, "exec", "tsx", join(here, "node_worker.ts")]];
const nodeSubmit = ["pnpm", ["--dir", nodeDir, "exec", "tsx", join(here, "node_submit.ts")]];

function newDb(tag) {
  return join(mkdtempSync(join(tmpdir(), `cairnq-${tag}-`)), "tasks.db");
}

function startWorker([cmd, baseArgs], db, readyMarker) {
  return new Promise((resolveReady, reject) => {
    const child = spawn(cmd, [...baseArgs, db], { env });
    let out = "";
    let ready = false;
    child.stdout.on("data", (d) => {
      out += d.toString();
      if (!ready && out.includes(readyMarker)) {
        ready = true;
        resolveReady(child);
      }
    });
    child.stderr.on("data", (d) => process.stderr.write(`[worker] ${d}`));
    child.on("exit", (code) => {
      if (!ready) reject(new Error(`worker exited before ready (code ${code})`));
    });
  });
}

function runSubmit([cmd, baseArgs], db, name, payload) {
  const r = spawnSync(cmd, [...baseArgs, db, name, JSON.stringify(payload)], {
    env,
    encoding: "utf-8",
  });
  if (r.status !== 0) {
    process.stderr.write(r.stderr || "");
    throw new Error(`submit failed (code ${r.status})`);
  }
  const line = (r.stdout || "").split("\n").find((l) => l.startsWith("RESULT "));
  if (!line) throw new Error(`no RESULT line in submit output:\n${r.stdout}`);
  return JSON.parse(line.slice("RESULT ".length));
}

function assert(cond, msg, detail) {
  if (!cond) throw new Error(`${msg}: ${JSON.stringify(detail)}`);
}

async function e2e1_tsApi_pyWorker() {
  const db = newDb("x1");
  const worker = await startWorker(pyWorker, db, "PY_WORKER_READY");
  try {
    const result = runSubmit(nodeSubmit, db, "image.generate", { prompt: "a cat", size: "512x512" });
    assert(result.engine === "python", "E2E1 engine", result);
    assert(result.url === "s3://img/a cat", "E2E1 url", result);
    assert(result.size === "512x512", "E2E1 size", result);
    console.log("E2E1 ok  TS API -> Python worker ->", result);
  } finally {
    worker.kill("SIGTERM");
  }
}

async function e2e2_pyApi_nodeWorker() {
  const db = newDb("x2");
  const worker = await startWorker(nodeWorker, db, "NODE_WORKER_READY");
  try {
    const result = runSubmit(pySubmit, db, "notification.send", { userId: 42, message: "hi" });
    assert(result.engine === "node", "E2E2 engine", result);
    assert(result.sent === true, "E2E2 sent", result);
    assert(result.to === 42, "E2E2 to", result);
    console.log("E2E2 ok  Python API -> TS worker ->", result);
  } finally {
    worker.kill("SIGTERM");
  }
}

async function main() {
  await e2e1_tsApi_pyWorker();
  await e2e2_pyApi_nodeWorker();
  console.log("\nCROSS-LANG OK: 4 language combinations exercised (TS+Py, Py+TS over shared SQLite).");
}

main().then(
  () => process.exit(0),
  (err) => {
    console.error("CROSS-LANG FAILED:", err.message);
    process.exit(1);
  },
);
