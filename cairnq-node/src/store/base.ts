import type { Task } from "../models.js";

export type Conflict = "reuse" | "reject" | "replace";

export interface SubmitInput {
  name: string;
  payload: unknown;
  queue?: string;
  key?: string | null;
  conflict?: Conflict;
  maxAttempts?: number;
  priority?: number;
  metadata?: unknown;
  parentId?: string | null;
  rootId?: string | null;
  correlationId?: string | null;
  runAtDelayMs?: number;
}

export interface ListInput {
  status?: string | null;
  queue?: string | null;
  name?: string | null;
  rootId?: string | null;
  correlationId?: string | null;
  limit?: number;
  offset?: number;
}

/** The storage seam. SQLiteStore is the only MVP implementation. */
export interface TaskStore {
  connect(): Promise<void>;
  close(): Promise<void>;
  protocolVersion(): Promise<number>;

  submit(input: SubmitInput): Promise<Task>;
  get(taskId: string): Promise<Task | null>;
  getByKey(key: string): Promise<Task | null>;
  list(input?: ListInput): Promise<Task[]>;
  cancel(taskId: string): Promise<Task | null>;
  cancelByKey(key: string): Promise<Task | null>;
  retry(taskId: string, opts?: { resetAttempt?: boolean }): Promise<Task | null>;
  retryByKey(key: string, opts?: { resetAttempt?: boolean }): Promise<Task | null>;

  claim(input: {
    queues: string[];
    workerId: string;
    leaseMs?: number;
    limit?: number;
  }): Promise<Task[]>;
  heartbeat(input: { taskId: string; workerId: string; leaseMs?: number }): Promise<Task>;
  progress(input: {
    taskId: string;
    workerId: string;
    progress: number | null;
    message: string | null;
  }): Promise<Task>;
  succeed(input: { taskId: string; workerId: string; result: unknown }): Promise<Task>;
  complete(input: { taskId: string; workerId: string; result: unknown }): Promise<Task>;
  fail(input: {
    taskId: string;
    workerId: string;
    error: unknown;
    retryable?: boolean;
    delayMs?: number;
  }): Promise<Task>;
}
