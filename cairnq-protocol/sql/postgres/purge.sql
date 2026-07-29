-- Retention: delete terminal tasks that completed before a cutoff (Postgres
-- dialect). Nothing else ever removes rows, so without this a long-lived database
-- only grows. Bounded by :limit so a large backlog is drained in short
-- transactions. The key pointer of a purged task goes with it via
-- cairnq_task_keys' ON DELETE CASCADE. The cutoff is relative (:older_than_ms)
-- because time comes from the DB clock.
-- FOR UPDATE SKIP LOCKED matters for correctness, not just throughput: without
-- it the subselect materializes on the statement snapshot and the outer
-- DELETE's only re-checked qual is the immutable id — so a task retried (and
-- even re-claimed) after the snapshot would still be deleted, destroying a
-- live task. Locking the rows in the subselect freezes them terminal until the
-- delete commits; a concurrent retry then re-evaluates against the deleted row
-- and correctly finds nothing.
-- params: older_than_ms, limit
delete from cairnq_tasks
where id in (
    select id from cairnq_tasks
    where status in ('succeeded', 'failed', 'canceled')
      and completed_at_ms is not null
      and completed_at_ms < (extract(epoch from now()) * 1000)::bigint - :older_than_ms
    order by completed_at_ms asc
    limit :limit
    for update skip locked
)
returning id;
