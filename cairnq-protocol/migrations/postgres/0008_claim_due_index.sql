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
-- index alone.
--
-- The measurements behind this live in the SQLite twin of this file, and they are
-- SQLite numbers: no Postgres was available where this was written, so this side
-- is reasoned from the same index shape rather than benchmarked. What holds
-- across both is the ordering — the due rows are now at the front of the index
-- range instead of behind every not-yet-due one.
--
-- What this does NOT fix, in either dialect: an empty draw still walks the whole
-- range looking for rows that are not there, and `claimable_probe` — whose
-- two-armed EXISTS cannot use the index at all — is untouched. A queue deep in
-- backoff still costs a worker real time per poll.
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
-- Both indexes are rebuilt inside the migration's transaction, which holds a
-- lock that blocks writes to cairnq_tasks while they build — seconds on a large
-- table. There is no CONCURRENTLY here: it cannot run inside a transaction, and
-- the ledger's check-and-apply is one. Upgrade a large deployment in a window
-- where that pause is affordable.
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
