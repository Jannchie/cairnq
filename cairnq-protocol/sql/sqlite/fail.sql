-- Fail a task. Ownership-checked. Single CASE-based statement handles both
-- branches atomically: retryable && attempt < max_attempts -> requeue with
-- backoff (run_at = now + delay_ms); otherwise -> terminal 'failed'.
-- :retryable is 0/1. :error is a JSON envelope text.
-- params: id, worker_id, now_ms, error, retryable, delay_ms
update cairnq_tasks
set
    status = case when :retryable = 1 and attempt < max_attempts then 'queued' else 'failed' end,
    error = :error,
    worker_id = case when :retryable = 1 and attempt < max_attempts then null else worker_id end,
    lease_until_ms = case when :retryable = 1 and attempt < max_attempts then null else lease_until_ms end,
    run_at_ms = case when :retryable = 1 and attempt < max_attempts then :now_ms + :delay_ms else run_at_ms end,
    completed_at_ms = case when :retryable = 1 and attempt < max_attempts then null else :now_ms end,
    updated_at_ms = :now_ms
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > :now_ms
returning *;
