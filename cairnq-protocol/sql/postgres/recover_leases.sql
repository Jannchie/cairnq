-- Reclaim tasks whose lease expired (Postgres dialect). Run inside the same write
-- transaction as claim, just before it. attempt < max_attempts -> back to 'queued'
-- for redelivery; otherwise -> 'failed' with a lease-expired error envelope. Time
-- comes from the DB clock.
-- params: lease_expired_error (jsonb envelope)
update cairnq_tasks
set
    status = case when attempt < max_attempts then 'queued' else 'failed' end,
    worker_id = case when attempt < max_attempts then null else worker_id end,
    lease_until_ms = null,
    run_at_ms = case when attempt < max_attempts
                     then (extract(epoch from now()) * 1000)::bigint else run_at_ms end,
    error = case when attempt >= max_attempts then :lease_expired_error::jsonb else error end,
    completed_at_ms = case when attempt >= max_attempts
                           then (extract(epoch from now()) * 1000)::bigint else completed_at_ms end,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where status = 'running'
  and lease_until_ms is not null
  and lease_until_ms <= (extract(epoch from now()) * 1000)::bigint
returning *;
