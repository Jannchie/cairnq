-- Drop cairnq_tasks_status_idx, which nothing can use.
--
-- 0001 shipped it for list's status filter. 0007 later added
-- (status, completed_at_ms) for purge, and a one-column index on the same
-- leading column is strictly contained in that: any lookup the narrow one could
-- serve, the wider one serves from its prefix. Confirmed on the statements as
-- they run — with the status filter specialized to an equality, SQLite picks
-- cairnq_tasks_status_completed_idx for list and for purge, and never the
-- narrow index, on a 20k-row table with statistics.
--
-- Until specialization existed neither was reachable at all, so this was
-- invisible: both indexes looked equally unused, and dropping either looked
-- equally safe or unsafe. It is only once the filters reach an index that one
-- of them is demonstrably the one reached.
--
-- The write cost is what makes it worth removing rather than leaving: every
-- insert and every status transition — submit, claim, each settle, each retry —
-- maintains it, which is the hottest write path there is, for a structure no
-- read consults. The other three of 0001's filter indexes (name, root_id,
-- correlation_id) stay: each is the only index that serves its filter, and each
-- is demonstrably read now.
--
-- Dropping an index needs no CONCURRENTLY on either dialect and holds a lock
-- only long enough to unlink it, so unlike 0008 this is not an upgrade window
-- to plan around. An older SDK is unaffected: it never named the index, and the
-- statement that used to want it could not reach it anyway.
drop index if exists cairnq_tasks_status_idx;

update cairnq_meta set value = '10' where key = 'schema_version';
