-- Cancel. queued -> canceled immediately; running -> set cancel_requested_at_ms
-- for cooperative exit (worker checks ctx.canceled()). Single statement covers
-- both. No-op (0 rows) for terminal tasks.
-- params: id, now_ms
update cairnq_tasks
set
    status = case when status = 'queued' then 'canceled' else status end,
    cancel_requested_at_ms = case when status = 'running' then :now_ms else cancel_requested_at_ms end,
    completed_at_ms = case when status = 'queued' then :now_ms else completed_at_ms end,
    updated_at_ms = :now_ms
where id = :id and status in ('queued', 'running')
returning *;
