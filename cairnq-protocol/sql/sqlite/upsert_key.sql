-- Point a key at a task (initial pointer, or repoint on replace).
-- params: key, task_id, now_ms
insert into cairnq_task_keys (key, task_id, created_at_ms, updated_at_ms)
values (:key, :task_id, :now_ms, :now_ms)
on conflict(key) do update set
    task_id = excluded.task_id,
    updated_at_ms = excluded.updated_at_ms;
