-- Serves recover_leases.sql: find tasks whose lease expired. That statement runs
-- on every worker's every poll, inside the claim transaction, so it is the one
-- recovery read on the hot path.
--
-- Partial, because the rows it looks for are a tiny slice of the table: only
-- 'running' rows can hold a lease, and their count is bounded by total worker
-- concurrency, while terminal rows accumulate until purge. A partial index stays
-- that small no matter how large the table grows, and — unlike the plain
-- cairnq_tasks_status_idx it replaces on this path — it carries lease_until_ms,
-- so the expiry test is a range scan rather than a row visit per running task.
--
-- Both predicate terms appear verbatim in recover_leases.sql's WHERE clause:
-- SQLite only uses a partial index when it can match each term syntactically.
-- It also needs statistics to prefer it, which is why both SDKs run
-- `PRAGMA optimize` when they open a database.
--
-- On the missing 0003 and the schema_version jump, see PROTOCOL.md §Versioning.
create index if not exists cairnq_tasks_lease_idx
    on cairnq_tasks (lease_until_ms)
    where status = 'running' and lease_until_ms is not null;

update cairnq_meta set value = '4' where key = 'schema_version';
