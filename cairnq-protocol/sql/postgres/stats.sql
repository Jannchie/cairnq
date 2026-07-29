-- Queue depth at a glance: task counts grouped by queue and status. Read-only.
-- A queue appears only while it has rows — terminal tasks count until purge
-- removes them. The SDK zero-fills the statuses a queue has no rows in.
-- params: (none)
select queue, status, count(*) as count
from cairnq_tasks
group by queue, status
order by queue asc, status asc;
