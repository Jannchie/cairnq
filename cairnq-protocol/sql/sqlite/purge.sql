-- Retention: delete terminal tasks that completed before a cutoff. Nothing else
-- ever removes rows, so without this a long-lived database only grows.
-- Bounded by :limit so a large backlog is drained in short transactions instead
-- of one long write that blocks every other writer. The key pointer of a purged
-- task goes with it via cairnq_task_keys' ON DELETE CASCADE.
-- The LIMIT lives in a subquery: plain `delete ... limit` needs a non-default
-- SQLite build option.
-- The queue/status/name filters are optional (pass NULL to skip, as in
-- list.sql): retention needs are tiered — a succeeded row is spent once its
-- result is consumed, while a failed one is worth keeping for diagnosis — and
-- without them the shortest-lived tier sets the retention for every row.
-- `queue` is the same argument one level up: a single installation is how this
-- project recommends two languages coordinate, so it routinely carries two
-- workloads whose rows have nothing to do with each other's lifetimes — an RPC
-- result read once and a durable job's log kept for a week. Migration 0009 adds
-- the index that makes the queue filter read only its own queue's rows rather
-- than skipping past every other queue's.
-- The optional form written here is not what runs when a filter IS supplied:
-- `(:p is null or col = :p)` is planned before the parameter has a value, so
-- SQLite must plan both branches, reaches no index at all, and walks every row
-- past the cutoff in completion order. The SDK rewrites each supplied filter to
-- a plain equality before preparing the statement (`specialize`), which is what
-- lets the queue and status indexes be reached. That rewrite replaced the three
-- hand-written variant files this comment used to name.
--
-- Postgres does not need the rewrite — it re-plans with the parameter values for
-- a statement's first executions and folds the null branch away — but it costs
-- nothing there, and one behaviour is easier to reason about than two.
--
-- :name has no specialization: no index covers it, so it is a residual predicate
-- either way and an equality form would buy nothing.
-- params: before_ms, queue, status, name, limit
delete from cairnq_tasks
where id in (
    select id from cairnq_tasks
    where status in ('succeeded', 'failed', 'canceled')
      and (:queue is null or queue = :queue)
      and (:status is null or status = :status)
      and (:name is null or name = :name)
      and completed_at_ms is not null
      and completed_at_ms < :before_ms
    order by completed_at_ms asc
    limit :limit
)
returning id;
