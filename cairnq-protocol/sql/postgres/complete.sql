-- Finalize a task the worker finished running (Postgres dialect). Ownership-checked
-- like succeed. Atomic cancel-vs-success decision: if a cancel was requested while
-- the task ran, it finalizes as 'canceled' (the result is discarded — cancel wins);
-- otherwise 'succeeded' with the given result. Time comes from the DB clock.
-- params: id, worker_id, result (jsonb or null)
update cairnq_tasks
set
    status = case when cancel_requested_at_ms is not null then 'canceled' else 'succeeded' end,
    result = case when cancel_requested_at_ms is not null then result else :result::jsonb end,
    progress = case when cancel_requested_at_ms is not null then progress else 1.0 end,
    completed_at_ms = (extract(epoch from now()) * 1000)::bigint,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > (extract(epoch from now()) * 1000)::bigint
returning *;
