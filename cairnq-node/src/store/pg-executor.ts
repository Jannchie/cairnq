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
