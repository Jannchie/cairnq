-- Mark succeeded (Postgres dialect). Ownership-checked. worker_id kept for audit.
-- :result is bound as jsonb. Time comes from the DB clock.
-- params: id, worker_id, result (jsonb), message
update cairnq_tasks
set
    status = 'succeeded',
    result = :result::jsonb,
    progress = 1.0,
    message = coalesce(:message, message),
    -- A lease describes an attempt in flight; this one just ended. worker_id is
    -- what carries the audit trail (who ran it), so nothing is lost by clearing
    -- it, and the terminal-lease invariant holds — see PROTOCOL.md §Lease model.
    lease_until_ms = null,
    completed_at_ms = (extract(epoch from now()) * 1000)::bigint,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > (extract(epoch from now()) * 1000)::bigint
returning *;
