-- purge_one_status, for a sweep bounded to one status. Byte-for-byte purge.sql
-- except that the status filter is an equality instead of an optional `is null
-- or` — a drift-guard test asserts precisely that, so treat purge.sql as the
-- source and re-derive this file when it changes.
--
-- purge.sql explains why the optional form reaches no index; the equality form
-- is what lets this seek cairnq_tasks_status_completed_idx (migration 0007) and
-- read only its own range.
-- params: before_ms, queue, status, name, limit
delete from cairnq_tasks
where id in (
    select id from cairnq_tasks
    where status in ('succeeded', 'failed', 'canceled')
      and (:queue is null or queue = :queue)
      and status = :status
      and (:name is null or name = :name)
      and completed_at_ms is not null
      and completed_at_ms < :before_ms
    order by completed_at_ms asc
    limit :limit
)
returning id;
