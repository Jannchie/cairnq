-- Point a key at a task (initial pointer, or repoint on replace). Postgres dialect.
-- Time comes from the DB clock.
-- params: key, task_id
insert into cairnq_task_keys (key, task_id, created_at_ms, updated_at_ms)
values (
    :key, :task_id,
    (extract(epoch from now()) * 1000)::bigint,
    (extract(epoch from now()) * 1000)::bigint
)
on conflict (key) do update set
    task_id = excluded.task_id,
    updated_at_ms = excluded.updated_at_ms;
