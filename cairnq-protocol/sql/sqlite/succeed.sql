-- Mark succeeded. Ownership-checked. worker_id kept for audit.
-- params: id, worker_id, now_ms, result (JSON text), message
update cairnq_tasks
set
    status = 'succeeded',
    result = :result,
    progress = 1.0,
    message = coalesce(:message, message),
    completed_at_ms = :now_ms,
    updated_at_ms = :now_ms
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > :now_ms
returning *;
