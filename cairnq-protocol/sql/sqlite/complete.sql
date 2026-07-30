-- Finalize a task the worker finished running. Ownership-checked like succeed.
-- Atomic cancel-vs-success decision: if a cancel was requested while the task
-- ran, it finalizes as 'canceled' (the result is discarded — cancel wins);
-- otherwise 'succeeded' with the given result. This is how a running task
-- reaches the 'canceled' terminal state (cooperative cancel, §7).
-- params: id, worker_id, now_ms, result (JSON text or null)
update cairnq_tasks
set
    status = case when cancel_requested_at_ms is not null then 'canceled' else 'succeeded' end,
    result = case when cancel_requested_at_ms is not null then result else :result end,
    progress = case when cancel_requested_at_ms is not null then progress else 1.0 end,
    -- Terminal on both branches, so unconditional: a lease describes an attempt in
    -- flight and this one just ended (see succeed.sql).
    lease_until_ms = null,
    completed_at_ms = :now_ms,
    updated_at_ms = :now_ms
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > :now_ms
returning *;
