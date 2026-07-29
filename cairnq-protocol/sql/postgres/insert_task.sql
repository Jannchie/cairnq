-- Insert a brand-new task (Postgres dialect). The SDK generates :id (ULID) and
-- :root_id (= :id for top-level tasks). Time comes from the DB clock, so the SDK
-- passes a relative :delay_ms (not an absolute run_at_ms): run_at = now + delay.
-- :payload / :metadata are bound as jsonb.
-- Time is clock_timestamp(), not now(): a keyed submit runs this after waiting
-- on the key's advisory lock, and now() freezes at BEGIN — a delayed task's
-- run_at stamped from the transaction start would fire early by that wait.
-- params: id, name, queue, payload, metadata, max_attempts, priority,
--         delay_ms, parent_id, root_id, correlation_id
insert into cairnq_tasks (
    id, name, queue, status, payload, metadata,
    max_attempts, priority, run_at_ms,
    parent_id, root_id, correlation_id,
    created_at_ms, updated_at_ms
) values (
    :id, :name, :queue, 'queued', :payload::jsonb, :metadata::jsonb,
    :max_attempts, :priority,
    (extract(epoch from clock_timestamp()) * 1000)::bigint + :delay_ms,
    :parent_id, :root_id, :correlation_id,
    (extract(epoch from clock_timestamp()) * 1000)::bigint,
    (extract(epoch from clock_timestamp()) * 1000)::bigint
)
returning *;
