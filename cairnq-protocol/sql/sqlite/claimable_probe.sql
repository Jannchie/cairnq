-- Read-only check: is there anything worth opening a write transaction for?
-- Run before claim so idle workers don't take a write lock every poll (which
-- would serialize all idle workers on SQLite's single writer). Returns has_work
-- = 1 if any task in these queues is claimable, or any lease has expired.
-- params: queues (JSON array text), now_ms
select exists(
    select 1 from cairnq_tasks
    where (status = 'queued'
           and queue in (select value from json_each(:queues))
           and run_at_ms <= :now_ms)
       or (status = 'running'
           and lease_until_ms is not null
           and lease_until_ms <= :now_ms)
) as has_work;
