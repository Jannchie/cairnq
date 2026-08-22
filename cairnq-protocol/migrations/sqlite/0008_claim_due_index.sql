-- Serve the claim's whole ORDER BY from the index, and order by when a task
-- became DUE rather than by when it was created.
--
-- The two halves are one fix. `run_at_ms <= :now_ms` was a residual on the old
-- (queue, status, priority desc, created_at_ms) index, and a not-yet-due row —
-- a retry waiting out its backoff, a task submitted with a delay — sorts by the
-- created_at it has always had, which puts it AHEAD of the rows that are
-- actually claimable. So every draw walked the whole backoff pile before
-- reaching anything it could take, inside the transaction that holds the claim.
-- The pile is largest exactly when a downstream dependency has just failed and
-- thousands of tasks are backing off together.
--
-- Ordering by run_at_ms instead puts every not-yet-due row AFTER the due ones,
-- so the scan reaches its rows first, and appending `id` lets the index carry the
-- tie-break as well, which is what lets the planner satisfy the ORDER BY from the
-- index alone. Measured through the real statement (`claim_one_queue`, one queue,
-- four names, 20k queued rows backing off), one claim of one task, on the three
-- SQLite builds that happened to be on the authoring machine:
--
--     SQLite   linked by                       one due row       an empty draw
--     3.39.4   a system python3.11             1166us ->   22us  1110us ->  350us
--     3.47.1   a python3.13 (this repo's venv)  3378us ->  116us  3128us -> 1006us
--     3.53.4   better-sqlite3 13 (bundled)     1100us ->   72us  1060us ->   50us
--
-- Read the columns, not the rows: the win holds on every build, and the absolute
-- cost swings more between SQLite versions than between before and after on some
-- of them. better-sqlite3 bundles its own SQLite, so the TypeScript SDK's version
-- is a dependency; the Python SDK links whatever the interpreter was built
-- against, so its version belongs to the user's Python and cannot be assumed.
--
-- The plan differs by SQLite version and the win does not come from one single
-- mechanism: 3.39 drops the sorter outright, while 3.53 keeps one and instead
-- reaches the due rows through a skip-scan. What both have in common is the
-- ordering — the due rows are now at the front of the range instead of behind
-- every not-yet-due one. Numbers are SQLite; Postgres was not measured (no
-- server in the authoring environment), so treat the Postgres side as reasoned
-- rather than benchmarked.
--
-- What this does NOT fix: an empty draw still walks the whole range looking for
-- rows that are not there (350us above), and `claimable_probe` — whose two-armed
-- EXISTS cannot use the index at all — is unchanged at ~2.2ms on that backlog.
-- A queue deep in backoff still costs a worker real time per poll.
--
-- For a task that was never delayed or retried the change is invisible in
-- practice: insert_task sets run_at_ms = now + delay, so the two columns hold the
-- same millisecond for an ordinary submit. (On Postgres "the same millisecond" is
-- not quite an identity — insert_task evaluates clock_timestamp() once per column
-- and it advances within a statement, so a submit landing on a millisecond
-- boundary can differ by 1ms. That reorders two tasks submitted in the same
-- millisecond, which claim.sql already calls decided by the id's random half
-- rather than by submit order.) It changes real ordering only for tasks whose
-- delivery was deferred, and for those "oldest due first" is the fairer answer —
-- a task that failed and backed off should not cut ahead of everything submitted
-- while it waited, which is what its original created_at_ms bought it.
--
-- Both indexes are rebuilt inside the migration's write transaction, so every
-- other process's writes wait while they build — and they wait only as long as
-- their busy budget (`busyTimeoutMs` / `busy_timeout_ms`, 5s by default) before
-- failing. Measured on a warm local SSD: ~2.5s + ~3.4s for a 1M-row, 500MB
-- database, i.e. past that default. Small databases do not notice; see
-- "Upgrading" in the README for what to do about a large one.
--
-- Same names, new definitions: these indexes ARE "the ones the claim reads", and
-- every reference to them in the SQL comments and PROTOCOL.md still points at
-- the right object.
--
-- An SDK older than this migration keeps working: it orders by created_at_ms,
-- finds no index in that order, and sorts. Slower, exactly where this migration
-- is faster — but note the sharper consequence while a fleet is mixed. Two SDK
-- versions against one database then disagree about which task is NEXT for any
-- delayed or retried work: each takes a valid claimable task, no task is lost or
-- run twice, but the documented claim order holds only within one version. That
-- is a difference in fairness, not in correctness, which is why protocol_version
-- stays at 1 — and it is a reason to keep the mixed window short.
drop index if exists cairnq_tasks_claim_idx;
create index cairnq_tasks_claim_idx
    on cairnq_tasks (queue, status, priority desc, run_at_ms, id);

-- The per-name twin (see 0006), rebuilt on the same principle.
drop index if exists cairnq_tasks_claim_name_idx;
create index cairnq_tasks_claim_name_idx
    on cairnq_tasks (queue, status, name, priority desc, run_at_ms, id);

update cairnq_meta set value = '8' where key = 'schema_version';
