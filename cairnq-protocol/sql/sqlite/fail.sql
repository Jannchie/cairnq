-- Fail a task. Ownership-checked. One CASE-based statement decides all three
-- outcomes atomically:
--   1. a cancel was requested while it ran -> terminal 'canceled'. Cancel wins,
--      exactly as in complete.sql: a task the user cancelled must never be
--      redelivered, whether the attempt ended in a return or in an exception.
--   2. retryable && attempt < max_attempts -> requeue with backoff
--      (run_at = now + delay_ms).
--   3. otherwise -> terminal 'failed'.
-- The error envelope is recorded on every branch, so a canceled-while-failing
-- task still carries why its last attempt failed. progress/message describe the
-- attempt in flight, so only the requeue branch clears them: a terminal record
-- keeps how far the last attempt got, a re-queued one must not advertise a dead
-- attempt's progress bar until the next attempt overwrites it.
-- :retryable is 0/1. :error is a JSON envelope text.
-- params: id, worker_id, now_ms, error, retryable, delay_ms
update cairnq_tasks
set
    status = case
        when cancel_requested_at_ms is not null then 'canceled'
        when :retryable = 1 and attempt < max_attempts then 'queued'
        else 'failed'
    end,
    error = :error,
    worker_id = case when cancel_requested_at_ms is null and :retryable = 1 and attempt < max_attempts
                     then null else worker_id end,
    lease_until_ms = case when cancel_requested_at_ms is null and :retryable = 1 and attempt < max_attempts
                          then null else lease_until_ms end,
    run_at_ms = case when cancel_requested_at_ms is null and :retryable = 1 and attempt < max_attempts
                     then :now_ms + :delay_ms else run_at_ms end,
    completed_at_ms = case when cancel_requested_at_ms is null and :retryable = 1 and attempt < max_attempts
                           then null else :now_ms end,
    progress = case when cancel_requested_at_ms is null and :retryable = 1 and attempt < max_attempts
                    then null else progress end,
    message = case when cancel_requested_at_ms is null and :retryable = 1 and attempt < max_attempts
                   then null else message end,
    updated_at_ms = :now_ms
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > :now_ms
returning *;
