-- Retention: delete terminal tasks that completed before a cutoff. Nothing else
-- ever removes rows, so without this a long-lived database only grows.
-- Bounded by :limit so a large backlog is drained in short transactions instead
-- of one long write that blocks every other writer. The key pointer of a purged
-- task goes with it via cairnq_task_keys' ON DELETE CASCADE.
-- The LIMIT lives in a subquery: plain `delete ... limit` needs a non-default
-- SQLite build option.
-- The status/name filters are optional (pass NULL to skip, as in list.sql):
-- retention needs are tiered — a succeeded row is spent once its result is
-- consumed, while a failed one is worth keeping for diagnosis — and without
-- them the shortest-lived tier sets the retention for every row.
-- params: before_ms, status, name, limit
delete from cairnq_tasks
where id in (
    select id from cairnq_tasks
    where status in ('succeeded', 'failed', 'canceled')
      and (:status is null or status = :status)
      and (:name is null or name = :name)
      and completed_at_ms is not null
      and completed_at_ms < :before_ms
    order by completed_at_ms asc
    limit :limit
)
returning id;
