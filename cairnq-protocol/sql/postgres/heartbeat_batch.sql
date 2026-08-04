-- Extend the lease on several tasks at once (Postgres dialect) — the heartbeat
-- for batch delivery. Ownership-checked per row, exactly as heartbeat.sql: a task
-- whose lease this worker no longer holds simply does not come back, so the
-- caller learns which ones it lost by which ids are absent from the result rather
-- than by an error. One statement per beat replaces one round trip per leased
-- task, which for a 256-task batch is the difference between one write and 256.
--
-- Returns only what the beat needs, unlike heartbeat.sql's `returning *`: the
-- singular statement hands its row to the caller as `ctx.heartbeat()`'s public
-- return value, while this one's rows never leave the worker — they answer "still
-- mine?" (presence) and "cancelled?" (the one column). Whole rows here would pull
-- every payload back on every beat: 256 tasks * 4KB is a megabyte re-read every
-- lease/3 for the life of the call, and a JSON parse per row to discard it.
--
-- New lease (now + :lease_ms) and time come from the DB clock.
-- params: ids (text[]), worker_id, lease_ms
update cairnq_tasks
set lease_until_ms = (extract(epoch from now()) * 1000)::bigint + :lease_ms,
    updated_at_ms = (extract(epoch from now()) * 1000)::bigint
where id = any(:ids::text[])
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > (extract(epoch from now()) * 1000)::bigint
returning id, cancel_requested_at_ms;
