-- Reclaim tasks whose lease expired. Run inside the same write transaction as
-- claim, just before it. Three outcomes, mirroring fail.sql:
--   1. a cancel was requested before the worker died -> terminal 'canceled'
--      (a cancelled task must never be redelivered by the crash path either);
--   2. attempt < max_attempts -> back to 'queued' for redelivery;
--   3. otherwise -> terminal 'failed' with a lease-expired error envelope.
-- params: now_ms, lease_expired_error (JSON envelope text)
update cairnq_tasks
set
    status = case
        when cancel_requested_at_ms is not null then 'canceled'
        when attempt < max_attempts then 'queued'
        else 'failed'
    end,
    worker_id = case when cancel_requested_at_ms is null and attempt < max_attempts
                     then null else worker_id end,
    lease_until_ms = null,
    run_at_ms = case when cancel_requested_at_ms is null and attempt < max_attempts
                     then :now_ms else run_at_ms end,
    -- Only the failed branch records lease expiry: a canceled task did not fail.
    error = case when cancel_requested_at_ms is null and attempt >= max_attempts
                 then :lease_expired_error else error end,
    completed_at_ms = case when cancel_requested_at_ms is null and attempt < max_attempts
                           then completed_at_ms else :now_ms end,
    updated_at_ms = :now_ms
where status = 'running'
  and lease_until_ms is not null
  and lease_until_ms <= :now_ms
returning *;
