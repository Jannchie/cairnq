/**
 * The seam between PostgresStore and whatever actually talks to Postgres.
 *
 * PostgresStore owns the *dialect* — the protocol's named-parameter SQL rewritten
 * to `$n`, the migration ledger, the LISTEN policy. It does not own the
 * *connection*. Applications that already run a Postgres driver (an ORM, a pool
 * they size themselves) pass their own executor and cairnq joins that session
 * instead of opening a second one, which is what makes a task's settlement
 * commit in the same transaction as the rows the task produced.
 *
 * Implementing one is small — see `createPoolExecutor` in pg-pool.ts for the
 * reference implementation over `pg`. An adapter passes rows through as its
 * driver produced them: cairnq normalizes both column types the drivers disagree
 * about (jsonb decoded or not, int8 as text or number) in `rowToTask`, so no
 * adapter has to reconfigure its driver — and none has to change how the
 * application's OWN columns come back in order to satisfy cairnq.
 *
 * ONE thing an adapter does have to get right: enable the driver's
 * prepared-statement path if it is not already the default. The store issues a
 * small, FIXED set of statement texts — each is loaded from a file at startup,
 * `specialize` may return one variant of it per set of optional filters a caller
 * supplies, and the `:name` -> `$n` rewrite is memoized on the result. So the
 * same handful of strings is submitted for the life of the process, reached
 * within the first few calls, and nothing here ever interpolates a value into
 * SQL. That is exactly the shape a server-side
 * prepared statement is for. A driver that instead describes each statement
 * before binding pays an extra round trip and a re-parse every time.
 *
 * The cost is **per statement**, and that is the number to reason with: measured
 * on postgres.js, whose `unsafe()` defaults to `prepare: false`, one statement
 * run 2000 times went 0.52ms -> 0.19ms once preparation was on — about 2.8x, or
 * ~0.33ms a statement. Per task it disappears: the same measurement through a
 * whole `call` round trip moved 104.9ms -> 102.9ms, and against a real handler
 * 17ms -> 16ms. A task costs what its handler costs, and a couple of statements
 * either way is noise next to that.
 *
 * Where it does land is the bookkeeping that runs whether or not there is any
 * work — the claim loop, `recover_leases`, heartbeats, `wait`'s polling. Those
 * are pure statement cost with no handler to hide behind, which is also why this
 * multiplies with `claimable_probe` rather than being independent of it: the
 * probe cuts how many statements an idle poll issues, this cuts what each one
 * costs. A fleet that is mostly idle pays almost nothing else.
 *
 * The reference implementation happens not to be affected — `pg` uses the
 * extended protocol by default — which is why this is stated here rather than
 * left to be discovered.
 *
 * A second thing, smaller but just as easy to get wrong: whatever this executor
 * does with json/jsonb, it must do CONSISTENTLY. The store measures the wire
 * form once at connect, with a `select '"cairnq"'::jsonb` probe, and maps every
 * row it later reads according to the answer — so an executor that decodes
 * jsonb for one statement and hands back text for another (or answers the probe
 * differently from how it returns task rows) will have its payloads read the
 * wrong way. Decoding or not are both fine; disagreeing with yourself is not.
 * The probe exists because the two forms cannot be told apart from a value: a
 * JSON string arrives as a string either way, so a guess parses the decoded one
 * twice — `"s3://…"` throws and `"42"` silently becomes the number 42.
 */

/** A row as the driver hands it back: column name -> value. */
export type Row = Record<string, unknown>;

/**
 * Somewhere statements can run. The same shape whether it is a pool (each call
 * on some connection) or one transaction's dedicated connection — the store's
 * statements do not care, and this is what lets `tx` hand the same interface to
 * its callback.
 */
export interface PgSession {
  /**
   * One parameterised statement, `$1`-style. `values` is positional and may
   * legitimately contain nulls — a null parameter is "this filter is off" in
   * several protocol statements, not a missing argument.
   */
  query(text: string, values: readonly unknown[]): Promise<Row[]>;
  /**
   * Parameterless SQL that may hold several statements, for migration DDL.
   * Separate from `query` because it must go over the simple query protocol:
   * the extended protocol a parameterised call uses accepts only one statement,
   * and every migration is a script.
   */
  exec(sql: string): Promise<void>;
}

/** A session that can also open transactions, listen, and be shut down. */
export interface PgExecutor extends PgSession {
  /**
   * Run `fn` inside one transaction on one dedicated connection, committing if
   * it returns and rolling back if it throws. The store relies on both halves:
   * a claim that cannot see its own `recover_leases` is a double-dispatch, and a
   * keyed submit that commits half way poisons the key.
   */
  tx<T>(fn: (session: PgSession) => Promise<T>): Promise<T>;

  /**
   * Subscribe a dedicated connection to `channels`. Optional: an executor that
   * omits it (or a Postgres that refuses LISTEN) costs latency, never
   * correctness — the store falls back to plain polling, which is the contract
   * PROTOCOL.md gives for push wakeups.
   *
   * Resolves with a function that stops listening. `onClose` reports a
   * connection that dropped on its own, so the store can degrade and retry.
   *
   * Throw `ListenUnavailable` when this Postgres will never accept LISTEN — a
   * transaction-mode pooler, say. Any other rejection is read as transient and
   * retried with backoff, so a permanent condition raised as a plain Error
   * becomes a reconnect loop that cannot succeed.
   */
  listen?(
    channels: readonly string[],
    onNotify: (channel: string, payload: string | undefined) => void,
    onClose: () => void,
  ): Promise<() => void>;

  /**
   * Release this executor's resources. Called by PostgresStore.close() ONLY for
   * an executor the store created itself: an injected one belongs to the caller,
   * whose other work would not survive cairnq closing it.
   */
  close(): Promise<void>;
}

/**
 * LISTEN will not work on this connection, and retrying cannot change that.
 * See `PgExecutor.listen`.
 */
export class ListenUnavailable extends Error {
  constructor(message = "LISTEN is not available on this connection") {
    super(message);
    this.name = "ListenUnavailable";
  }
}
