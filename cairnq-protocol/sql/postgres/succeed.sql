-- Mark succeeded (Postgres dialect). Ownership-checked. worker_id kept for audit.
-- :result is bound as jsonb. Time comes from the DB clock.
-- params: id, worker_id, result (jsonb), message
update cairnq_tasks
set
    status = 'succeeded',
    result = :result::jsonb,
    progress = 1.0,
    message = coalesce(:message, message),
    completed_at_ms = (extract(epoch from now()) * 1000)::bigint,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > (extract(epoch from now()) * 1000)::bigint
returning *;
