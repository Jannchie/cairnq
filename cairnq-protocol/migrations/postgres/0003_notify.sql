-- Push-based wakeups (Postgres only). A row trigger emits:
--   cairnq_queued  (payload: queue name)  when a task becomes claimable-soon:
--                                         inserted queued, or requeued by a
--                                         retryable fail / retry / recovery;
--   cairnq_done    (payload: task id)     when a task reaches a terminal status.
-- The trigger lives in the database, not in the SDKs, so every writer — either
-- SDK, any version, even hand-run SQL — wakes listeners. See PROTOCOL.md
-- ("Push wakeups") for the contract; in short, notifications only cut a poll
-- sleep short and are never required for correctness. Additive:
-- protocol_version stays 1.
--
-- Trigger guards, hottest write first:
--   - WHEN keeps claim (-> 'running', the most frequent status write) from
--     entering plpgsql at all;
--   - UPDATE OF status keeps heartbeat/progress from firing the trigger;
--   - the IS DISTINCT FROM checks skip a SET that rewrites the same value
--     (e.g. cancel.sql on an already-running task).

create or replace function cairnq_notify() returns trigger as $$
begin
    if new.status = 'queued'
       and (tg_op = 'INSERT' or old.status is distinct from new.status) then
        perform pg_notify('cairnq_queued', new.queue);
    elsif tg_op = 'UPDATE'
          and new.status in ('succeeded', 'failed', 'canceled')
          and old.status not in ('succeeded', 'failed', 'canceled') then
        perform pg_notify('cairnq_done', new.id);
    end if;
    return null;
end;
$$ language plpgsql;

drop trigger if exists cairnq_tasks_notify on cairnq_tasks;
create trigger cairnq_tasks_notify
after insert or update of status on cairnq_tasks
for each row
when (new.status is distinct from 'running')
execute function cairnq_notify();
