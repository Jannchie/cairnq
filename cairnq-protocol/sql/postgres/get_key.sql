-- Resolve the current task_id a key points at (for conflict handling).
-- params: key
select task_id from cairnq_task_keys where key = :key;
