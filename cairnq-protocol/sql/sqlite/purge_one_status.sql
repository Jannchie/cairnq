-- purge, for a sweep bounded to ONE terminal status. Byte-for-byte purge.sql
-- except that the status filter is an equality instead of an optional `is null
-- or` — a drift-guard test asserts precisely that, so treat purge.sql as the
-- source and re-derive this file when it changes.
--
-- It exists because the optional-filter form cannot be indexed. SQLite plans a
-- statement when it is prepared, before any parameter has a value, so `(:status
-- is null or status = :status)` has to be planned for BOTH branches and the
-- planner falls back to cairnq_tasks_completed_idx, walking every row past the
-- cutoff in completion order and discarding the ones belonging to another
-- status. `limit` bounds what comes back, never what is read, so the cost grows
-- with exactly the rows the filter was meant to skip — and the deployment the
-- filter exists for (one installation, two workloads on different clocks) is the
-- one where those rows are most numerous. Measured on 20k rows over two queues
-- and four statuses: the optional form chooses cairnq_tasks_completed_idx for
-- every filter combination, the equality form chooses
-- cairnq_tasks_status_completed_idx (0007) and reads only its own range. Same
-- reason claim.sql has specializations, same shape.
--
-- Postgres does not have SQLite's problem — it re-plans with the parameter
-- values for the first executions and folds the null branch away — but it ships
-- the variant too, because both dialects carry the same statement set and a
-- caller that had to know which dialect indexes which form would be a worse
-- contract.
--
-- :name stays optional in every variant: nothing indexes it, so it is a residual
-- predicate either way and specializing it would buy nothing.
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
