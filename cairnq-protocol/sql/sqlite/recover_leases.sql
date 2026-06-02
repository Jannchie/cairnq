-- Reclaim tasks whose lease expired. Run inside the same write transaction as
-- claim, just before it. attempt < max_attempts -> back to 'queued' for redelivery;
-- otherwise -> 'failed' with a lease-expired error envelope.
-- params: now_ms, lease_expired_error (JSON envelope text)
update cairnq_tasks
set
    status = case when attempt < max_attempts then 'queued' else 'failed' end,
    worker_id = case when attempt < max_attempts then null else worker_id end,
    lease_until_ms = null,
    run_at_ms = case when attempt < max_attempts then :now_ms else run_at_ms end,
    error = case when attempt >= max_attempts then :lease_expired_error else error end,
    completed_at_ms = case when attempt >= max_attempts then :now_ms else completed_at_ms end,
    updated_at_ms = :now_ms
where status = 'running'
  and lease_until_ms is not null
  and lease_until_ms <= :now_ms
returning *;
