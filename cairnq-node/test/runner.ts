// TypeScript interpreter for cairnq-protocol/conformance scenarios — the mirror
// of cairnq-py/tests/_runner.py. Both run the same JSON files verbatim.
import type { CairnQ } from "../src/index.js";
import type { TaskStore } from "../src/store/base.js";

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function field(obj: any, name: string): any {
  if (obj == null) return undefined;
  if (Array.isArray(obj)) return obj[Number(name)];
  return obj[name];
}

function deepEqual(a: any, b: any): boolean {
  if (a === b) return true;
  if (typeof a !== typeof b) return false;
  if (a && b && typeof a === "object") {
    const ka = Object.keys(a);
    const kb = Object.keys(b);
    if (ka.length !== kb.length) return false;
    return ka.every((k) => deepEqual(a[k], b[k]));
  }
  return false;
}

export class Runner {
  private store: TaskStore;
  private saved: Record<string, any> = {};

  constructor(private client: CairnQ) {
    this.store = client.store;
  }

  private resolve(value: any): any {
    if (typeof value === "string" && value.startsWith("$")) return this.ref(value);
    if (Array.isArray(value)) return value.map((v) => this.resolve(v));
    if (value && typeof value === "object") {
      const o: any = {};
      for (const k of Object.keys(value)) o[k] = this.resolve(value[k]);
      return o;
    }
    return value;
  }

  private ref(r: string): any {
    const parts = r.slice(1).split(".");
    let cur = this.saved[parts[0]];
    for (const p of parts.slice(1)) cur = field(cur, p);
    return cur;
  }

  async run(steps: any[]): Promise<void> {
    for (const step of steps) await this.runStep(step);
  }

  private async runStep(step: any): Promise<void> {
    const op = step.op;
    if (op === "expect") {
      const target = "target" in step ? this.resolve(step.target) : undefined;
      this.assert(target, step);
      return;
    }
    const args = this.resolve(step.args ?? {});
    let result: any;
    let error: any;
    try {
      result = await this.dispatch(op, args);
    } catch (e) {
      error = e;
    }
    if (step.expectError) {
      if (!error) throw new Error(`${op}: expected error ${step.expectError}, got success`);
      const name = error?.name ?? error?.constructor?.name;
      if (name !== step.expectError) {
        throw new Error(`${op}: expected ${step.expectError}, got ${name}: ${error}`);
      }
      if (step.save) this.saved[step.save] = { task_id: error.taskId ?? null, type: name };
      return;
    }
    if (error) throw error;
    if (step.save) this.saved[step.save] = result;
    if (step.expect) this.assert(result, step.expect);
  }

  private async dispatch(op: string, a: any): Promise<any> {
    const c = this.client;
    const s = this.store;
    switch (op) {
      case "submit":
        return c.submit(a.name, a.payload ?? {}, {
          key: a.key,
          queue: a.queue,
          conflict: a.conflict,
          maxAttempts: a.max_attempts,
          priority: a.priority,
          metadata: a.metadata,
          correlationId: a.correlation_id,
        });
      case "get":
        return c.get(a.id);
      case "get_by_key":
        return c.getByKey(a.key);
      case "list":
        return c.list({
          status: a.status,
          queue: a.queue,
          name: a.name,
          rootId: a.root_id,
          correlationId: a.correlation_id,
          limit: a.limit,
          offset: a.offset,
        });
      case "cancel":
        return c.cancel(a.id);
      case "cancel_by_key":
        return c.cancelByKey(a.key);
      case "retry":
        return c.retry(a.id, { resetAttempt: a.reset_attempt });
      case "retry_by_key":
        return c.retryByKey(a.key, { resetAttempt: a.reset_attempt });
      case "claim":
        return s.claim({ queues: a.queues, workerId: a.worker_id, leaseMs: a.lease_ms, limit: a.limit });
      case "heartbeat":
        return s.heartbeat({ taskId: a.id, workerId: a.worker_id, leaseMs: a.lease_ms });
      case "progress":
        return s.progress({
          taskId: a.id,
          workerId: a.worker_id,
          progress: a.progress ?? null,
          message: a.message ?? null,
        });
      case "succeed":
        return s.succeed({ taskId: a.id, workerId: a.worker_id, result: a.result });
      case "complete":
        return s.complete({ taskId: a.id, workerId: a.worker_id, result: a.result });
      case "fail":
        return s.fail({
          taskId: a.id,
          workerId: a.worker_id,
          error: a.error,
          retryable: a.retryable,
          delayMs: a.delay_ms,
        });
      case "purge":
        return c.purge({ olderThanMs: a.older_than_ms, limit: a.limit });
      case "sleep":
        await sleep(a.ms);
        return null;
      default:
        throw new Error(`unknown op: ${op}`);
    }
  }

  private assert(target: any, spec: any): void {
    if ("equals" in spec) {
      for (const k of Object.keys(spec.equals)) {
        const actual = field(target, k);
        const expected = this.resolve(spec.equals[k]);
        if (!deepEqual(actual, expected)) {
          throw new Error(`equals ${k}: ${JSON.stringify(actual)} != ${JSON.stringify(expected)}`);
        }
      }
    }
    if ("equalsRef" in spec && !deepEqual(target, this.resolve(spec.equalsRef))) {
      throw new Error("equalsRef mismatch");
    }
    if ("notEqualsRef" in spec && deepEqual(target, this.resolve(spec.notEqualsRef))) {
      throw new Error("notEqualsRef matched");
    }
    if ("greaterThanRef" in spec) {
      const ref = this.resolve(spec.greaterThanRef);
      if (!(Number(target) > Number(ref))) {
        throw new Error(`greaterThanRef: ${target} !> ${ref}`);
      }
    }
    if ("length" in spec && (target?.length ?? -1) !== spec.length) {
      throw new Error(`length ${target?.length} != ${spec.length}`);
    }
    if ("notNull" in spec) {
      for (const n of spec.notNull) if (field(target, n) == null) throw new Error(`${n} is null`);
    }
    if ("isNull" in spec) {
      for (const n of spec.isNull) if (field(target, n) != null) throw new Error(`${n} is not null`);
    }
  }
}
