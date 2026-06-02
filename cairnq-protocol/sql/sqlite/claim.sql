-- Atomic claim. :queues is a JSON array of queue names. :lease_until_ms is
-- precomputed by the SDK (= now_ms + lease_ms). Single UPDATE ... RETURNING
-- under BEGIN IMMEDIATE is the SQLite equivalent of FOR UPDATE SKIP LOCKED.
-- params: queues (JSON array text), now_ms, worker_id, lease_until_ms, limit
update cairnq_tasks
set
    status = 'running',
    worker_id = :worker_id,
    lease_until_ms = :lease_until_ms,
    attempt = attempt + 1,
    updated_at_ms = :now_ms
where id in (
    select id from cairnq_tasks
    where status = 'queued'
      and queue in (select value from json_each(:queues))
      and run_at_ms <= :now_ms
    order by priority desc, created_at_ms asc
    limit :limit
)
returning *;
