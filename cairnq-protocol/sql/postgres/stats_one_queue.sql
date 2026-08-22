-- stats, for a caller asking about ONE queue. Byte-for-byte stats.sql except
-- that the queue filter is an equality instead of an optional `is null or` — a
-- drift-guard test asserts precisely that, so treat stats.sql as the source and
-- re-derive this file when it changes.
--
-- It exists for the same reason purge_one_queue.sql does. SQLite plans a
-- statement before its parameters have values, so the optional form has to be
-- planned for both branches: it reads the whole table (as a covering index
-- scan) and groups it, which is exactly the cost narrowing to one queue was
-- meant to avoid. The equality form seeks the (queue, status) prefix of
-- cairnq_tasks_claim_idx and reads only that queue's entries.
--
-- This still COUNTS what it reports, so it costs what it counts: one queue's
-- rows, terminal ones included. Narrower than the unfiltered form, still not a
-- poll-loop question — queue_depth.sql is the bounded one.
-- params: queue
select queue, status, count(*) as count
from cairnq_tasks
where queue = :queue
group by queue, status
order by queue asc, status asc;
