-- purge, for a sweep bounded to one queue AND one terminal status. It is the
-- combination of purge_one_queue.sql's queue equality and purge_one_status.sql's
-- status equality, and each is there for the reason that file gives. Together
-- they pin both leading columns of cairnq_tasks_queue_completed_idx, so the
-- sweep seeks one queue's terminal rows in completion order and stops at :limit.
-- params: older_than_ms, queue, status, name, limit
delete from cairnq_tasks
where id in (
    select id from cairnq_tasks
    where status in ('succeeded', 'failed', 'canceled')
      and queue = :queue
      and status = :status
      and (:name::text is null or name = :name)
      and completed_at_ms is not null
      and completed_at_ms < (extract(epoch from now()) * 1000)::bigint - :older_than_ms
    order by completed_at_ms asc
    limit :limit
    for update skip locked
)
returning id;
