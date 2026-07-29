-- Reclaim tasks whose lease expired (Postgres dialect). Run inside the same write
-- transaction as claim, just before it. Three outcomes, mirroring fail.sql:
--   1. a cancel was requested before the worker died -> terminal 'canceled'
--      (a cancelled task must never be redelivered by the crash path either);
--   2. attempt < max_attempts -> back to 'queued' for redelivery;
--   3. otherwise -> terminal 'failed' with a lease-expired error envelope.
-- Time is clock_timestamp() (see claim.sql). FOR UPDATE SKIP LOCKED keeps every
-- worker's recovery pass non-blocking: a row another worker is already
-- recovering — or that its owner is finalizing right now — is simply skipped,
-- that transaction's outcome stands, and (unlike a plain set UPDATE, whose
-- lock order follows the scan) no two recoverers can deadlock.
-- params: lease_expired_error (jsonb envelope)
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
                     then (extract(epoch from clock_timestamp()) * 1000)::bigint else run_at_ms end,
    -- Only the failed branch records lease expiry: a canceled task did not fail.
    error = case when cancel_requested_at_ms is null and attempt >= max_attempts
                 then :lease_expired_error::jsonb else error end,
    completed_at_ms = case when cancel_requested_at_ms is null and attempt < max_attempts
                           then completed_at_ms else (extract(epoch from clock_timestamp()) * 1000)::bigint end,
    -- Only the requeue branch clears them: they describe the dead attempt, and a
    -- task waiting to be redelivered must not report its progress bar.
    progress = case when cancel_requested_at_ms is null and attempt < max_attempts
                    then null else progress end,
    message = case when cancel_requested_at_ms is null and attempt < max_attempts
                   then null else message end,
    updated_at_ms = (extract(epoch from clock_timestamp()) * 1000)::bigint
where id in (
    select id from cairnq_tasks
    where status = 'running'
      and lease_until_ms is not null
      and lease_until_ms <= (extract(epoch from clock_timestamp()) * 1000)::bigint
    for update skip locked
)
returning *;
