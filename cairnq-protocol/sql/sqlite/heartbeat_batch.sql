-- Extend the lease on several tasks at once — the heartbeat for batch delivery.
-- Ownership-checked per row, exactly as heartbeat.sql: a task whose lease this
-- worker no longer holds simply does not come back, so the caller learns which
-- ones it lost by which ids are absent from the result rather than by an error.
-- One statement per beat is the point: a batch handler holding 256 leases would
-- otherwise write 256 rows every heartbeat interval, which on SQLite means 256
-- turns of the single write lock for work nobody is waiting on.
--
-- Returns only what the beat needs, unlike heartbeat.sql's `returning *`: the
-- singular statement hands its row to the caller as `ctx.heartbeat()`'s public
-- return value, while this one's rows never leave the worker — they answer "still
-- mine?" (presence) and "cancelled?" (the one column). Whole rows here would pull
-- every payload back on every beat: 256 tasks * 4KB is a megabyte re-read every
-- lease/3 for the life of the call, and a JSON parse per row to discard it.
-- Unlike heartbeat.sql, this one's plan depends on statistics: json_each() hides
-- the id list's length, so without sqlite_stat1 the planner drives the update off
-- cairnq_tasks_status_idx and walks every 'running' row in the database per beat
-- instead of doing primary-key lookups. Both SDKs ANALYZE on open and revisit
-- once a minute per connection (see "Planner statistics" in PROTOCOL.md), so this
-- is a warm-up window rather than a standing cost — but the statement it replaced
-- had no such dependency, which is why it is called out here.
-- params: ids (JSON array text), worker_id, now_ms, lease_until_ms
update cairnq_tasks
set lease_until_ms = :lease_until_ms, updated_at_ms = :now_ms
where id in (select value from json_each(:ids))
  and status = 'running'
  and worker_id = :worker_id
  and lease_until_ms > :now_ms
returning id, cancel_requested_at_ms;
