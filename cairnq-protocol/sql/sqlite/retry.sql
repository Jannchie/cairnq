-- Manually re-enqueue a failed/canceled task. :reset_attempt (0/1) controls
-- whether the attempt counter resets to 0.
-- params: id, now_ms, reset_attempt
update cairnq_tasks
set
    status = 'queued',
    error = null,
    worker_id = null,
    lease_until_ms = null,
    run_at_ms = :now_ms,
    cancel_requested_at_ms = null,
    completed_at_ms = null,
    -- The previous attempt's progress bar dies with the attempt.
    progress = null,
    message = null,
    attempt = case when :reset_attempt = 1 then 0 else attempt end,
    updated_at_ms = :now_ms
where id = :id and status in ('failed', 'canceled')
returning *;
