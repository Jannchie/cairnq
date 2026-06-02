-- Atomic claim (Postgres dialect). Uses native FOR UPDATE SKIP LOCKED so
-- concurrent workers never contend on the same task — the native equivalent of
-- SQLite's single-writer BEGIN IMMEDIATE serialization. No claimable_probe is
-- needed (PG readers don't block writers). :queues is a text[]; time and the new
-- lease (now + :lease_ms) come from the DB clock.
-- recover_leases MUST run first in the SAME transaction. READ COMMITTED suffices:
-- each UPDATE re-checks its WHERE against the latest committed row, so racing
-- claims/recovers can neither double-dispatch a task nor double-recover a lease.
-- params: queues (text[]), worker_id, lease_ms, limit
update cairnq_tasks t
set
    status = 'running',
    worker_id = :worker_id,
    lease_until_ms = (extract(epoch from now()) * 1000)::bigint + :lease_ms,
    attempt = attempt + 1,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
from (
    select id from cairnq_tasks
    where status = 'queued'
      and queue = any(:queues::text[])
      and run_at_ms <= (extract(epoch from now()) * 1000)::bigint
    order by priority desc, created_at_ms asc
    limit :limit
    for update skip locked
) sel
where t.id = sel.id
returning t.*;
