-- Manually re-enqueue a failed/canceled task (Postgres dialect). :reset_attempt
-- (native boolean) controls whether the attempt counter resets to 0. Time from
-- the DB clock.
-- params: id, reset_attempt (boolean)
update cairnq_tasks
set
    status = 'queued',
    error = null,
    worker_id = null,
    lease_until_ms = null,
    run_at_ms = (extract(epoch from now()) * 1000)::bigint,
    cancel_requested_at_ms = null,
    completed_at_ms = null,
    attempt = case when :reset_attempt then 0 else attempt end,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where id = :id and status in ('failed', 'canceled')
returning *;
