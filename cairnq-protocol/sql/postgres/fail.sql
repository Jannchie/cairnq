-- Fail a task (Postgres dialect). Ownership-checked. Single CASE-based statement
-- handles both branches atomically: retryable && attempt < max_attempts -> requeue
-- with backoff (run_at = now + :delay_ms); otherwise -> terminal 'failed'.
-- :retryable is a native boolean. :error is bound as jsonb. Time from the DB clock.
-- params: id, worker_id, error (jsonb), retryable (boolean), delay_ms
update cairnq_tasks
set
    status = case when :retryable and attempt < max_attempts then 'queued' else 'failed' end,
    error = :error::jsonb,
    worker_id = case when :retryable and attempt < max_attempts then null else worker_id end,
    lease_until_ms = case when :retryable and attempt < max_attempts then null else lease_until_ms end,
    run_at_ms = case when :retryable and attempt < max_attempts
                     then (extract(epoch from now()) * 1000)::bigint + :delay_ms else run_at_ms end,
    completed_at_ms = case when :retryable and attempt < max_attempts
                           then null else (extract(epoch from now()) * 1000)::bigint end,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > (extract(epoch from now()) * 1000)::bigint
returning *;
