-- List tasks with optional filters. Every filter param must be bound; pass
-- NULL to skip it. Supports chain queries via root_id / correlation_id (§22).
-- params: status, queue, name, root_id, correlation_id, limit, offset
select * from cairnq_tasks
where (:status is null or status = :status)
  and (:queue is null or queue = :queue)
  and (:name is null or name = :name)
  and (:root_id is null or root_id = :root_id)
  and (:correlation_id is null or correlation_id = :correlation_id)
-- id breaks created_at_ms ties, as in claim.sql: without it, paginating with
-- offset across same-millisecond rows could repeat or skip a task.
order by created_at_ms desc, id desc
limit :limit offset :offset;
