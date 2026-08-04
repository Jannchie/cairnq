-- claim, for a caller drawing exactly ONE task name. Byte-for-byte claim.sql
-- except that the name filter is an equality on :name instead of an IN over
-- :names — a drift-guard test asserts precisely that, so treat claim.sql as the
-- source and re-derive this file when it changes.
--
-- It exists because `name in (select value from json_each(:names))` cannot reach
-- cairnq_tasks_claim_name_idx, even for a single-element list: SQLite builds a
-- bloom filter over the subquery and falls back to cairnq_tasks_claim_idx, where
-- the name is a residual and a draw for a name with nothing queued walks the
-- whole claimable backlog. Measured on a 20k backlog: 1446us for the json_each
-- form, 8.8us for this one. See migration 0006.
--
-- Used for the per-name quotas a worker draws for names that size themselves —
-- a `batch`, or their own concurrency. See "Batch delivery" in PROTOCOL.md.
-- params: queues (JSON array text), name, now_ms, worker_id, lease_until_ms,
--         limit
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
      and name = :name
      and run_at_ms <= :now_ms
    -- id breaks created_at_ms ties (same-millisecond submits), so claim order
    -- is deterministic: FIFO at millisecond granularity; within one millisecond
    -- the id's random half decides, stably but not in submit order.
    order by priority desc, created_at_ms asc, id asc
    limit :limit
)
returning *;
