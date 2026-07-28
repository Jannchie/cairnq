-- Serves purge.sql: scan terminal tasks in completion order. Without it the
-- retention sweep is a full table scan of exactly the rows that accumulate most.
create index if not exists cairnq_tasks_completed_idx
    on cairnq_tasks (completed_at_ms);

update cairnq_meta set value = '2' where key = 'schema_version';
