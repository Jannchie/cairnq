-- Extend the lease. Ownership-checked: 0 rows -> caller raises LostLease.
-- Returns the row (incl. cancel_requested_at_ms) so ctx.canceled() can ride on it.
-- params: id, worker_id, now_ms, lease_until_ms
update cairnq_tasks
set lease_until_ms = :lease_until_ms, updated_at_ms = :now_ms
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > :now_ms
returning *;
