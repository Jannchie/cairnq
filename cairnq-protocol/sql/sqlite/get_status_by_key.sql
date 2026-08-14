-- get_status.sql, following a key instead of an id — the probe behind
-- wait_by_key. Resolves the key on every read, so a `replace` landing mid-wait
-- moves the wait onto the new task.
-- params: key
select t.id, t.status from cairnq_tasks t
join cairnq_task_keys k on k.task_id = t.id
where k.key = :key;
