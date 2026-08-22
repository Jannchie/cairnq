-- Task counts grouped by queue and status. Read-only.
-- A queue appears only while it has rows — terminal tasks count until purge
-- removes them. The SDK zero-fills the statuses a queue has no rows in.
--
-- :queue is optional (pass NULL for every queue). Unfiltered, this reads every
-- row in the table, so its cost grows with everything the installation has ever
-- run — and one store carrying two workloads is the coordination cairnq
-- recommends, so a caller asking about its own queue should not pay for the
-- other's backlog. Filtered to one queue it can be served from
-- cairnq_tasks_claim_idx's (queue, status) prefix instead.
--
-- Filtered or not, this still COUNTS: the cost is proportional to the rows being
-- counted, which is the whole queue, terminal rows included. That is fine for a
-- dashboard and wrong for a poll loop — queue_depth.sql is the bounded question,
-- and the one to ask on an interval.
-- params: queue
select queue, status, count(*) as count
from cairnq_tasks
where (:queue is null or queue = :queue)
group by queue, status
order by queue asc, status asc;
