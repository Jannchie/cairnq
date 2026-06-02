-- params: key
select t.* from cairnq_tasks t
join cairnq_task_keys k on k.task_id = t.id
where k.key = :key;
