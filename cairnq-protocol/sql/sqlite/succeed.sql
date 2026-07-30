-- Mark succeeded. Ownership-checked. worker_id kept for audit.
-- params: id, worker_id, now_ms, result (JSON text), message
update cairnq_tasks
set
    status = 'succeeded',
    result = :result,
    progress = 1.0,
    message = coalesce(:message, message),
    -- A lease describes an attempt in flight; this one just ended. worker_id is
    -- what carries the audit trail (who ran it), so nothing is lost by clearing
    -- it, and the terminal-lease invariant holds — see PROTOCOL.md §Lease model.
    lease_until_ms = null,
    completed_at_ms = :now_ms,
    updated_at_ms = :now_ms
where id = :id
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > :now_ms
returning *;
