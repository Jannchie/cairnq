-- How many more tasks fit in one queue under :max_depth. Read-only.
--
-- Returns headroom, not depth: a producer needs "may I enqueue, and how many
-- more" — and bounding the scan at :max_depth is what keeps this affordable.
-- COUNT over the whole backlog would read every queued row, so the cost of
-- asking would grow with exactly the pile-up the caller is trying to stop.
-- Wrapped as a LIMIT subquery it reads at most :max_depth index entries off
-- cairnq_tasks_claim_idx (queue, status leading), and headroom saturates at 0
-- once the queue is full — which is all a gate needs to know.
--
-- Counts 'queued' only. A running task already has a worker and is bounded by
-- that worker's concurrency; the backlog worth pushing back on is the work
-- nobody has picked up. Delayed tasks (run_at_ms in the future) count: they are
-- queued work that will run, and excluding them would let an unbounded pile of
-- them through the gate.
--
-- Read-only, and it must stay that way: isWriteStatement/_is_write_statement
-- route a non-select into the group commit, which would put a gate probe behind
-- SQLite's write lock — the opposite of what a backpressure check is for.
-- params: queue, max_depth
select :max_depth - count(*) as headroom
from (
    select 1 from cairnq_tasks
    where queue = :queue and status = 'queued'
    limit :max_depth
) probe;
