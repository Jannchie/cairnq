-- Insert a brand-new task. Caller (SDK) generates :id (ULID) and :root_id
-- (= :id for top-level tasks) and provides :now_ms.
-- params: id, name, queue, payload, metadata, max_attempts, priority,
--         run_at_ms, parent_id, root_id, correlation_id, now_ms
insert into cairnq_tasks (
    id, name, queue, status, payload, metadata,
    max_attempts, priority, run_at_ms,
    parent_id, root_id, correlation_id,
    created_at_ms, updated_at_ms
) values (
    :id, :name, :queue, 'queued', :payload, :metadata,
    :max_attempts, :priority, :run_at_ms,
    :parent_id, :root_id, :correlation_id,
    :now_ms, :now_ms
)
returning *;
