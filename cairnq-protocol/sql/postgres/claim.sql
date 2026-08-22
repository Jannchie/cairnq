-- Atomic claim (Postgres dialect). Uses native FOR UPDATE SKIP LOCKED so
-- concurrent workers never contend on the same task — the native equivalent of
-- SQLite's single-writer BEGIN IMMEDIATE serialization. No claimable_probe is
-- needed (PG readers don't block writers). :queues is a text[]; time and the new
-- lease (now + :lease_ms) come from the DB clock.
-- recover_leases MUST run first in the SAME transaction. READ COMMITTED suffices:
-- each UPDATE re-checks its WHERE against the latest committed row, so racing
-- claims/recovers can neither double-dispatch a task nor double-recover a lease.
-- Time is clock_timestamp(), not now(): now() freezes at BEGIN, and this runs
-- after recover_leases in a transaction that may have waited on row locks — a
-- lease stamped from the transaction start would already be short by that wait.
--
-- :names is the set of task names the caller can actually run, or NULL for no
-- filter. A worker passes its registered handler names: queues alone do not
-- partition work, so without this a worker claims a task it has no handler for
-- and fails it permanently — two workers with different handler sets on one queue
-- would destroy each other's tasks. An empty array claims nothing.
-- params: queues (text[]), names (text[] or null), worker_id, lease_ms, limit
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
