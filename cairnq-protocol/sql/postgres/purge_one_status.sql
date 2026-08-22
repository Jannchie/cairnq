-- purge_one_status, for a sweep bounded to one status. Byte-for-byte purge.sql
-- except that the status filter is an equality instead of an optional `is null
-- or` — a drift-guard test asserts precisely that, so treat purge.sql as the
-- source and re-derive this file when it changes.
--
-- purge.sql explains why the optional form reaches no index; the equality form
-- is what lets this seek cairnq_tasks_status_completed_idx (migration 0007) and
-- read only its own range.
-- params: older_than_ms, queue, status, name, limit
delete from cairnq_tasks
where id in (
    select id from cairnq_tasks
    where status in ('succeeded', 'failed', 'canceled')
      and (:queue::text is null or queue = :queue)
      and status = :status
      and (:name::text is null or name = :name)
      and completed_at_ms is not null
      and completed_at_ms < (extract(epoch from now()) * 1000)::bigint - :older_than_ms
    order by completed_at_ms asc
    limit :limit
    for update skip locked
)
returning id;
