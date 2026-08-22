-- Serves the per-name claim: claim_one_name.sql and claim_one_queue_one_name.sql,
-- which a worker uses to draw a separate quota for each task name that sizes
-- itself (a `batch`, or its own concurrency). See "Batch delivery" in PROTOCOL.md.
--
-- cairnq_tasks_claim_idx does not cover it: `name` is not in that index, so a
-- name filter is a residual applied while walking the queue in claim order. The
-- cost lands hardest on a name with *nothing* queued — the claim walks every
-- claimable row in the queue looking for `limit` matches and finds none — and a
-- worker makes one such draw per registered name, per poll, inside the claim's
-- transaction, holding its FOR UPDATE row locks for the whole of it.
--
-- Both indexes are needed. `name` sits before the ORDER BY columns here, so this
-- one can only be read in claim order when `name` is an equality — exactly the
-- per-name draws. A claim with no name filter, or an array-valued one, still
-- reads cairnq_tasks_claim_idx and is unaffected.
--
-- NOT built CONCURRENTLY: migrations run inside a transaction (see the runner's
-- `lock table cairnq_migrations`), and CREATE INDEX CONCURRENTLY cannot. On an
-- existing large cairnq_tasks this takes a write lock for the build. Deploying
-- into a busy database is the case to watch; build it by hand with CONCURRENTLY
-- first if that matters, and this statement then becomes a no-op.
-- The measurements above belong to the shape defined here; migration 0008
-- rebuilds it around run_at_ms and carries the current numbers.
create index if not exists cairnq_tasks_claim_name_idx
    on cairnq_tasks (queue, status, name, priority desc, created_at_ms);

update cairnq_meta set value = '6' where key = 'schema_version';
