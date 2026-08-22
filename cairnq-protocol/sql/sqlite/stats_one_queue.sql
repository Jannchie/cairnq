-- stats, for a caller asking about one queue. Byte-for-byte stats.sql except
-- that the queue filter is an equality instead of an optional `is null or` — a
-- drift-guard test asserts precisely that, so treat stats.sql as the source and
-- re-derive this file when it changes.
-- --
-- Same reason purge has specializations, spelled out in purge.sql: the optional
-- form is planned before the parameter has a value and reaches no index, so it
-- reads the whole table as a covering-index scan — exactly the cost narrowing to
-- one queue was meant to avoid. The equality form seeks the (queue, status)
-- prefix of cairnq_tasks_claim_idx. It still COUNTS what it reports, so it costs
-- one queue's rows; queue_depth.sql is the bounded question to ask on an
-- interval.
-- params: queue
select queue, status, count(*) as count
from cairnq_tasks
where queue = :queue
group by queue, status
order by queue asc, status asc;
