-- claim, for a caller watching exactly ONE queue. Byte-for-byte claim.sql except
-- that the queue filter is an equality on :queue instead of an IN over :queues —
-- a drift-guard test asserts precisely that, so treat claim.sql as the source and
-- re-derive this file when it changes.
--
-- It exists because the IN form costs a full sort. json_each() hides the list's
-- length from the planner, so SQLite must merge several index ranges and can no
-- longer read rows in claim order; it materializes every claimable row into a
-- temp B-tree just to take LIMIT of them. Cost then grows with the queued
-- backlog, inside the write transaction, on every claim: measured at 21us / 239us
-- / 1792us for a backlog of 50 / 2000 / 20000. The equality form reads
-- cairnq_tasks_claim_idx in claim order — the whole ORDER BY, id tie-break
-- included, since migration 0008 — so it needs no sort at all and stays flat.
--
-- params: queue, names (JSON array text or null), now_ms, worker_id,
--         lease_until_ms, limit
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
      and queue = :queue
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
