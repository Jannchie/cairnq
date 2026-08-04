-- claim, for a caller drawing exactly ONE task name (Postgres dialect).
-- Byte-for-byte claim.sql except that the name filter is an equality on :name
-- instead of `= any(:names)` — a drift-guard test asserts precisely that, so
-- treat claim.sql as the source and re-derive this file when it changes.
--
-- It exists so the draw can reach cairnq_tasks_claim_name_idx, whose leading
-- columns are (queue, status, name): an array-valued name filter cannot be read
-- in claim order against it, so the name falls back to a residual on
-- cairnq_tasks_claim_idx and a draw for a name with nothing queued scans the
-- whole claimable backlog — inside the transaction, holding its row locks. See
-- migration 0006.
--
-- Used for the per-name quotas a worker draws for names that size themselves —
-- a `batch`, or their own concurrency. See "Batch delivery" in PROTOCOL.md.
-- params: queues (text[]), name, worker_id, lease_ms, limit
update cairnq_tasks t
set
    status = 'running',
    worker_id = :worker_id,
    lease_until_ms = (extract(epoch from clock_timestamp()) * 1000)::bigint + :lease_ms,
    attempt = attempt + 1,
    updated_at_ms = (extract(epoch from clock_timestamp()) * 1000)::bigint
from (
    select id from cairnq_tasks
    where status = 'queued'
      and queue = any(:queues::text[])
      and name = :name
      and run_at_ms <= (extract(epoch from clock_timestamp()) * 1000)::bigint
    -- id breaks created_at_ms ties (same-millisecond submits), so claim order
    -- is deterministic: FIFO at millisecond granularity; within one millisecond
    -- the id's random half decides, stably but not in submit order.
    order by priority desc, created_at_ms asc, id asc
    limit :limit
    for update skip locked
) sel
where t.id = sel.id
returning t.*;
