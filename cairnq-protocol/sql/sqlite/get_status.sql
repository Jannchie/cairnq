-- The status-only probe behind wait/call polling. A pending task's whole row is
-- dead weight to a loop that only asks "is it finished yet" — with a large
-- payload it re-reads and re-parses megabytes per second of waiting. The full
-- row is fetched once, via get.sql, when this reports a terminal status.
-- params: id
select id, status from cairnq_tasks where id = :id;
