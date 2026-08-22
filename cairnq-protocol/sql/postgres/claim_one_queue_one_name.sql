-- claim, for a caller watching ONE queue and drawing ONE task name (Postgres
-- dialect) — the common shape for a batched worker. Byte-for-byte claim.sql
-- except that both the queue and the name filters are equalities; a drift-guard
-- test asserts precisely that, so treat claim.sql as the source and re-derive
-- this file when it changes.
--
-- It is the combination of claim_one_queue.sql's queue equality and
-- claim_one_name.sql's name equality, and each is there for the reason that file
-- gives. Together they pin both leading columns of cairnq_tasks_claim_name_idx,
-- so the draw is an index scan in claim order that stops at :limit rows however
-- deep the backlog is.
-- params: queue, name, worker_id, lease_ms, limit
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
      and name = :name
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
