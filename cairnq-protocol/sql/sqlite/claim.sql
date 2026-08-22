-- Atomic claim. :queues is a JSON array of queue names. :lease_until_ms is
-- precomputed by the SDK (= now_ms + lease_ms). Single UPDATE ... RETURNING
-- under BEGIN IMMEDIATE is the SQLite equivalent of FOR UPDATE SKIP LOCKED.
--
-- :names is a JSON array of the task names the caller can actually run, or NULL
-- for no filter. A worker passes its registered handler names: queues alone do
-- not partition work, so without this a worker claims a task it has no handler
-- for and fails it permanently — two workers with different handler sets on one
-- queue would destroy each other's tasks. An empty array claims nothing.
-- params: queues (JSON array text), names (JSON array text or null), now_ms,
--         worker_id, lease_until_ms, limit
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
      and (:names is null or name in (select value from json_each(:names)))
      and run_at_ms <= :now_ms
    -- Ordered by when a task became DUE, not when it was created: see migration
    -- 0008 for why, for what it costs a mixed-version fleet, and for which of
    -- these four statements the index can serve without a sort.
    -- id breaks run_at_ms ties (same-millisecond submits), so claim order is
    -- deterministic: FIFO at millisecond granularity; within one millisecond
    -- the id's random half decides, stably but not in submit order.
    order by priority desc, run_at_ms asc, id asc
    limit :limit
)
returning *;
