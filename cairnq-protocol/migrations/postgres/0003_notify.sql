-- Push-based wakeups (Postgres only). A row trigger emits:
--   cairnq_queued  (payload: queue name)  when a task becomes claimable-soon:
--                                         inserted queued, or requeued by a
--                                         retryable fail / retry / recovery;
--   cairnq_done    (payload: task id)     when a task reaches a terminal status.
--
-- The trigger lives in the database, not in the SDKs, so every writer — either
-- SDK, any version, even hand-run SQL — wakes listeners. Notifications are an
-- accelerator, never a correctness mechanism: NOTIFY delivers only to currently
-- connected listeners, so SDKs keep their polling as the fallback and merely cut
-- the sleep short when a notification arrives. Purely additive: protocol_version
-- stays 1, and SDKs that never LISTEN are unaffected.
--
-- Guarded on a status transition (UPDATE OF status still fires when SET merely
-- rewrites the same value, e.g. cancel.sql on a running task) so heartbeats,
-- progress and no-op status writes stay silent.

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
for each row execute function cairnq_notify();
