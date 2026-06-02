-- List tasks with optional filters (Postgres dialect). Every filter param must be
-- bound; pass NULL to skip it. The `::text` cast on each filter pins the param's
-- type so PG can plan the `IS NULL` branch (an untyped param there is ambiguous).
-- Supports chain queries via root_id / correlation_id.
-- params: status, queue, name, root_id, correlation_id, limit, offset
select * from cairnq_tasks
where (:status::text is null or status = :status)
  and (:queue::text is null or queue = :queue)
  and (:name::text is null or name = :name)
  and (:root_id::text is null or root_id = :root_id)
  and (:correlation_id::text is null or correlation_id = :correlation_id)
order by created_at_ms desc
limit :limit offset :offset;
