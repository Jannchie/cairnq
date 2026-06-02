/**
 * A typed task handle. `defineTask<Payload, Result>("name")` gives one symbol that
 * the worker (`worker.task(def, handler)`) and the client (`tasks.submit/call(def, …)`)
 * both reference — so the name lives in exactly one place (no string drift), editors
 * autocomplete it and find every caller, and `call(def, …)` infers its `Result` type.
 *
 * It's purely opt-in: every API still accepts a plain name string, and cross-language
 * callers keep using the string (only the name crosses the DB boundary). The `__payload`
 * / `__result` fields are phantom type carriers — they let TypeScript infer `P`/`R` from
 * a `TaskDef<P, R>` argument and are never set or read at runtime.
 */
export interface TaskDef<P = unknown, R = unknown> {
  readonly name: string;
  /** @internal phantom — type inference only, never present at runtime. */
  readonly __payload?: P;
  /** @internal phantom — type inference only, never present at runtime. */
  readonly __result?: R;
}

export function defineTask<P = unknown, R = unknown>(name: string): TaskDef<P, R> {
  return { name };
}

/** Resolve a name from either a plain string or a TaskDef. */
export function taskName(task: string | TaskDef): string {
  return typeof task === "string" ? task : task.name;
}
