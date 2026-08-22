export { CairnQ } from "./client.js";
export type { CallOptions, ClientOptions, SubmitOptions, WaitOptions } from "./client.js";
export { QueueDepthGate } from "./backpressure.js";
export type { BackpressureOptions, QueueDepthLimit } from "./backpressure.js";
export { RetentionSweeper } from "./retention.js";
export type { RetentionCutoffs, RetentionOptions, RetentionRule } from "./retention.js";
export { Worker } from "./worker.js";
export type { BatchHandler, Handler, TypedHandler, WorkerOptions } from "./worker.js";
export { TaskContext } from "./context.js";
export type { TaskContextOptions } from "./context.js";
export { defineTask } from "./task.js";
export type { TaskDef } from "./task.js";
export { SQLiteStore } from "./store/sqlite.js";
export { PostgresStore } from "./store/postgres.js";
export { ListenUnavailable } from "./store/pg-executor.js";
export type { PgExecutor, PgSession, Row } from "./store/pg-executor.js";
export { createPoolExecutor } from "./store/pg-pool.js";
export { TaskStore } from "./store/base.js";
export type {
  ListInput,
  PurgeInput,
  SubmitInput,
  Conflict,
  WatchOptions,
  WatchSignal,
} from "./store/base.js";
export { DEFAULT_WATCH_POLL_MS } from "./store/base.js";
export type { Task, TaskRef, TaskStatus, TerminalStatus } from "./models.js";
export {
  STATUSES,
  isTerminal,
  isTerminalStatus,
  cancelRequested,
  isQueued,
  isRunning,
  isSucceeded,
  isFailed,
  isCanceled,
} from "./models.js";
export {
  CairnQError,
  AlreadyExists,
  QueueFull,
  TaskTimeout,
  TaskFailed,
  TaskCanceled,
  TaskError,
  LostLease,
  EventLoopBlocked,
  ProtocolVersionMismatch,
  SchemaMismatch,
  UnsupportedBackend,
  SerializationError,
} from "./errors.js";
export type { FailReason } from "./errors.js";
