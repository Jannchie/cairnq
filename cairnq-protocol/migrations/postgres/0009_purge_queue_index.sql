-- Give the retention sweep a queue dimension it can actually read.
--
-- purge gained an optional `:queue` filter, and without an index for it queue is
-- a residual on cairnq_tasks_completed_idx (completed_at_ms) or
-- cairnq_tasks_status_completed_idx (status, completed_at_ms): the scan walks in
-- completion order and throws away every row belonging to another queue. `limit`
-- bounds what comes back, not what is read — and the shape that makes the filter
-- worth having in the first place is exactly the worst case for it. One store
-- carrying an RPC queue kept for minutes and a durable queue kept for a week is
-- the cross-language coordination this project recommends; sweeping the RPC
-- queue then means walking the week's worth of older rows the other queue is
-- deliberately holding onto, over and over, once per batch.
--
-- Partial, on the terminal statuses, for two reasons. It is the smaller half of
-- the table — a busy queue's live rows never enter it — and, more to the point,
-- rows enter it only when a task settles, so the index is not a tax on the claim
-- path the way a full index on (queue, ...) would be. purge.sql always carries
-- the literal `status in ('succeeded','failed','canceled')`, whether or not the
-- caller narrowed to one status, so the predicate matches exactly and the
-- planner never has to prove anything subtler to use it.
--
-- Measured on SQLite 3.39.4, 20k rows over two queues and four statuses, with
-- 0002's and 0007's indexes present alongside it: every filter combination purge
-- can issue is served by one index scan with the ORDER BY satisfied from the
-- index (no temp b-tree), and the two pre-existing shapes — unfiltered, and
-- status-only — still choose their old indexes, so nothing that worked before
-- got slower. Postgres is reasoned from the same shape rather than benchmarked;
-- see 0008 for why that caveat keeps appearing.
--
-- Unlike 0008 this only CREATES an index, so an older SDK is unaffected: it
-- never passes `:queue`, its statements are unchanged, and it pays only the
-- write-side cost of an index it does not read. The build still holds a lock for
-- as long as it takes (no CONCURRENTLY — the ledger's check-and-apply is one
-- transaction), but over terminal rows only, which is the smaller set.
create index if not exists cairnq_tasks_queue_completed_idx
    on cairnq_tasks (queue, completed_at_ms)
    where status in ('succeeded', 'failed', 'canceled');

update cairnq_meta set value = '9' where key = 'schema_version';
