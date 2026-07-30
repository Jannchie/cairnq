-- Make the lease invariant true of the whole table, not just of rows written from
-- here on: `lease_until_ms is not null` if and only if `status = 'running'`.
--
-- succeed.sql and complete.sql used to leave the dead attempt's lease behind, so a
-- succeeded task reported a lease into the future and nobody owned it. The crash
-- path (recover_leases.sql) always cleared it, so the two ways out of 'running'
-- disagreed on what a terminal row looks like. The statements now agree; this
-- catches up the rows they already wrote.
--
-- Scoped to `status <> 'running'` rather than to the three terminal states: that is
-- the invariant itself, and it also covers a 'queued' row should one ever be left
-- holding a lease.
update cairnq_tasks
set lease_until_ms = null
where status <> 'running' and lease_until_ms is not null;

update cairnq_meta set value = '5' where key = 'schema_version';
