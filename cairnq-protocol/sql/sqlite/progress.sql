-- Update progress/message. Ownership-checked. Does not change status.
-- params: id, worker_id, now_ms, progress, message
-- message is coalesced so progress(value) without a message keeps the prior one.
update cairnq_tasks
set progress = :progress, message = coalesce(:message, message), updated_at_ms = :now_ms
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > :now_ms
returning *;
