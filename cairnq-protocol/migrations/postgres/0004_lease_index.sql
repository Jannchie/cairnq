-- Serves recover_leases.sql: find tasks whose lease expired. That statement runs
-- on every worker's every poll, inside the claim transaction, so it is the one
-- recovery read on the hot path. See the SQLite twin for the reasoning; it holds
-- here too, and additionally keeps the FOR UPDATE SKIP LOCKED subquery from
-- taking row locks it will immediately skip.
--
-- Not CONCURRENTLY: migrations run inside the same transaction as their
-- bookkeeping insert (see PROTOCOL.md), and CREATE INDEX CONCURRENTLY cannot run
-- in one. On a table this size the plain form's lock is brief; a deployment large
-- enough to care can build it by hand ahead of the upgrade, and IF NOT EXISTS
-- makes this migration a no-op then.
create index if not exists cairnq_tasks_lease_idx
    on cairnq_tasks (lease_until_ms)
    where status = 'running' and lease_until_ms is not null;

update cairnq_meta set value = '4' where key = 'schema_version';
