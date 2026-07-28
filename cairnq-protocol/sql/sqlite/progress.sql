-- Update progress/message. Ownership-checked. Does not change status.
-- params: id, worker_id, now_ms, progress, message
-- Both fields are coalesced, symmetrically: progress(value) keeps the prior
-- message, progress(null, message) keeps the prior fraction. Passing null means
-- "leave this alone", never "clear it".
update cairnq_tasks
set progress = coalesce(:progress, progress),
    message = coalesce(:message, message),
    updated_at_ms = :now_ms
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > :now_ms
returning *;
