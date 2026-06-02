-- Cancel (Postgres dialect). queued -> canceled immediately; running -> set
-- cancel_requested_at_ms for cooperative exit (worker checks ctx.canceled()).
-- Single statement covers both. No-op (0 rows) for terminal tasks. Time from the
-- DB clock.
-- params: id
update cairnq_tasks
set
    status = case when status = 'queued' then 'canceled' else status end,
    cancel_requested_at_ms = case when status = 'running'
                                  then (extract(epoch from now()) * 1000)::bigint
                                  else cancel_requested_at_ms end,
    completed_at_ms = case when status = 'queued'
                           then (extract(epoch from now()) * 1000)::bigint else completed_at_ms end,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where id = :id and status in ('queued', 'running')
returning *;
