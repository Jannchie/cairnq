-- Serves the per-name claim: claim_one_name.sql and claim_one_queue_one_name.sql,
-- which a worker uses to draw a separate quota for each task name that sizes
-- itself (a `batch`, or its own concurrency). See "Batch delivery" in PROTOCOL.md.
--
-- cairnq_tasks_claim_idx does not cover it: `name` is not in that index, so a
-- name filter is a residual applied while walking the queue in claim order. The
-- cost lands hardest on a name with *nothing* queued — the claim walks every
-- claimable row in the queue looking for `limit` matches and finds none — and a
-- worker makes one such draw per registered name, per poll, inside the claim's
-- write transaction. Measured on a 20k backlog: 1116us for an empty name's draw
-- against cairnq_tasks_claim_idx, 8.8us against this one.
--
-- Both indexes are needed. `name` sits before the ORDER BY columns here, so this
-- one can only be read in claim order when `name` is an equality — exactly the
-- per-name draws. A claim with no name filter, or a list-valued one, still reads
-- cairnq_tasks_claim_idx and is unaffected (measured flat at ~13-16us either way).
--
-- The equality is what makes it usable: `name in (select value from json_each(?))`
-- does NOT reach this index even for a single-element list — SQLite builds a
-- bloom filter over the subquery and falls back to cairnq_tasks_claim_idx
-- (measured 1446us, i.e. no improvement at all). That is why the per-name
-- statements exist as separate files rather than the shared one being reused.
-- The measurements above belong to the shape defined here; migration 0008
-- rebuilds it around run_at_ms and carries the current numbers.
create index if not exists cairnq_tasks_claim_name_idx
    on cairnq_tasks (queue, status, name, priority desc, created_at_ms);

update cairnq_meta set value = '6' where key = 'schema_version';
