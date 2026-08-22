-- claim, for a caller watching exactly ONE queue. Byte-for-byte claim.sql except
-- that the queue filter is an equality on :queue instead of `= any(:queues)` — a
-- drift-guard test asserts precisely that, so treat claim.sql as the source and
-- re-derive this file when it changes.
--
-- It exists because the array form cannot be read in claim order: with ORDER BY
-- + LIMIT over `= any(...)` the planner falls back to a sequential scan and a
-- full sort of every claimable row (measured on 20k queued: Seq Scan 20000 rows,
-- quicksort 1861kB), inside the transaction that holds the claim. The equality
-- form index-scans cairnq_tasks_claim_idx, which since migration 0008 carries the
-- id tie-break too and so needs no sort node at all — 33 rows read for the same
-- query.
--
-- params: queue, names (text[] or null), worker_id, lease_ms, limit
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
      and queue = :queue
      and (:names::text[] is null or name = any(:names::text[]))
      and run_at_ms <= (extract(epoch from clock_timestamp()) * 1000)::bigint
    -- Ordered by when a task became DUE, not when it was created: see migration
    -- 0008 for why, for what it costs a mixed-version fleet, and for which of
    -- these four statements the index can serve without a sort.
    -- id breaks run_at_ms ties (same-millisecond submits), so claim order is
    -- deterministic: FIFO at millisecond granularity; within one millisecond
    -- the id's random half decides, stably but not in submit order.
    order by priority desc, run_at_ms asc, id asc
    limit :limit
    for update skip locked
) sel
where t.id = sel.id
returning t.*;
