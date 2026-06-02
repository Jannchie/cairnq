-- Extend the lease (Postgres dialect). Ownership-checked: 0 rows -> caller raises
-- LostLease. Returns the row (incl. cancel_requested_at_ms) so ctx.canceled() can
-- ride on it. New lease (now + :lease_ms) and time come from the DB clock.
-- params: id, worker_id, lease_ms
update cairnq_tasks
set lease_until_ms = (extract(epoch from now()) * 1000)::bigint + :lease_ms,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > (extract(epoch from now()) * 1000)::bigint
returning *;
