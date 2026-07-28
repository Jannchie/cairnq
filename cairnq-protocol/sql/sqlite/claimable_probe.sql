-- Read-only check: is there anything worth opening a write transaction for?
-- Run before claim so idle workers don't take a write lock every poll (which
-- would serialize all idle workers on SQLite's single writer). Returns has_work
-- = 1 if any task this caller can run is claimable, or any lease has expired.
-- Mirrors claim.sql's filters, so the probe never promises work claim will skip.
-- The expired-lease arm stays unfiltered on purpose: recovering a dead worker's
-- task is every worker's job, whatever names it happens to handle.
-- params: queues (JSON array text), names (JSON array text or null), now_ms
select exists(
    select 1 from cairnq_tasks
    where (status = 'queued'
           and queue in (select value from json_each(:queues))
           and (:names is null or name in (select value from json_each(:names)))
           and run_at_ms <= :now_ms)
       or (status = 'running'
           and lease_until_ms is not null
           and lease_until_ms <= :now_ms)
) as has_work;
