import { cancelRequested, type Task } from "./models.js";
import type { SubmitOptions } from "./client.js";
import type { TaskStore } from "./store/base.js";
import { type TaskDef, taskName } from "./task.js";
import { pollWait } from "./wait.js";

/** Handed to a task handler. Worker-side capabilities mirror the Python SDK. */
export class TaskContext {
  constructor(
    private readonly store: TaskStore,
    private readonly task: Task,
    public readonly workerId: string,
    private readonly leaseMs: number,
  ) {}

  get taskId(): string {
    return this.task.id;
  }
  get name(): string {
    return this.task.name;
  }
  get queue(): string {
    return this.task.queue;
  }
  get attempt(): number {
    return this.task.attempt;
  }
  get metadata(): any {
    return this.task.metadata;
  }
  get rootId(): string | null {
    return this.task.root_id;
  }
  get correlationId(): string | null {
    return this.task.correlation_id;
  }
  get payload(): any {
    return this.task.payload;
  }

  async progress(value: number | null, message: string | null = null): Promise<Task> {
    return this.store.progress({
      taskId: this.task.id,
      workerId: this.workerId,
      progress: value,
      message,
    });
  }

  async heartbeat(): Promise<Task> {
    return this.store.heartbeat({
      taskId: this.task.id,
      workerId: this.workerId,
      leaseMs: this.leaseMs,
    });
  }

  /** Cooperative cancel check. */
  async canceled(): Promise<boolean> {
    const t = await this.store.get(this.task.id);
    if (!t) return true;
    return cancelRequested(t) || t.status === "canceled";
  }

  /** Submit a child task; parent/root/correlation are wired automatically. */
  submit(name: string, payload?: unknown, opts?: SubmitOptions): Promise<Task>;
  submit<P, R>(task: TaskDef<P, R>, payload?: P, opts?: SubmitOptions): Promise<Task>;
  async submit(task: string | TaskDef, payload?: unknown, opts: SubmitOptions = {}): Promise<Task> {
    return this.store.submit({
      name: taskName(task),
      payload,
      parentId: this.task.id,
      rootId: this.task.root_id,
      correlationId: this.task.correlation_id,
      ...opts,
    });
  }

  async wait(taskId: string, opts: { timeoutMs?: number; pollMs?: number } = {}): Promise<Task> {
    return pollWait(this.store, taskId, {
      timeoutMs: opts.timeoutMs ?? 30_000,
      pollMs: opts.pollMs,
    });
  }
}
