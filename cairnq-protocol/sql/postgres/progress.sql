-- Update progress/message (Postgres dialect). Ownership-checked. Does not change
-- status. message is coalesced so progress(value) without a message keeps the
-- prior one. Time comes from the DB clock.
-- params: id, worker_id, progress, message
update cairnq_tasks
set progress = :progress,
    message = coalesce(:message, message),
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > (extract(epoch from now()) * 1000)::bigint
returning *;
