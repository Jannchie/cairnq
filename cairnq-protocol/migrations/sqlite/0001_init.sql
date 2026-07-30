-- CairnQ canonical schema (SQLite dialect) — protocol_version 1
-- Single source of truth for the task table. Idempotent; safe to re-run.
-- Time is stored as integer epoch milliseconds (`*_ms`). JSON columns are
-- TEXT validated by json_valid(). There is no separate schema.sql: ordered
-- migrations are canonical.

create table if not exists cairnq_tasks (
    id text primary key,

    name text not null,
    queue text not null default 'default',

    status text not null check (
        status in ('queued', 'running', 'succeeded', 'failed', 'canceled')
    ),

    payload text not null check (json_valid(payload)),
    result text check (result is null or json_valid(result)),
    error text check (error is null or json_valid(error)),
    metadata text not null default '{}' check (json_valid(metadata)),

    progress real,
    message text,

    attempt integer not null default 0,
    max_attempts integer not null default 3,

    priority integer not null default 0,

    worker_id text,
    lease_until_ms integer,

    run_at_ms integer not null,

    cancel_requested_at_ms integer,

    parent_id text,
    root_id text,
    correlation_id text,

    created_at_ms integer not null,
    updated_at_ms integer not null,
    completed_at_ms integer
);

-- Serves the claim query: WHERE queue=? AND status='queued' ORDER BY priority
-- desc, created_at_ms asc (run_at_ms applied as a residual filter). Leading with
-- queue+status then the ORDER BY columns is what lets claim_one_queue.sql read
-- rows in claim order; claim.sql's list-valued queue filter has to merge several
-- ranges of this index, so it sorts instead.
create index if not exists cairnq_tasks_claim_idx
    on cairnq_tasks (queue, status, priority desc, created_at_ms);
create index if not exists cairnq_tasks_status_idx on cairnq_tasks (status);
create index if not exists cairnq_tasks_name_idx on cairnq_tasks (name);
create index if not exists cairnq_tasks_root_idx on cairnq_tasks (root_id);
create index if not exists cairnq_tasks_correlation_idx on cairnq_tasks (correlation_id);

-- key = business-stable pointer to the *current* task for that key.
-- task_id = one concrete execution. Kept separate (not a unique constraint on
-- tasks) so reuse / reject / replace are natural.
create table if not exists cairnq_task_keys (
    key text primary key,
    task_id text not null references cairnq_tasks(id) on delete cascade,
    created_at_ms integer not null,
    updated_at_ms integer not null
);

create table if not exists cairnq_meta (
    key text primary key,
    value text not null
);

insert into cairnq_meta (key, value) values ('protocol_version', '1')
    on conflict(key) do nothing;
insert into cairnq_meta (key, value) values ('schema_version', '1')
    on conflict(key) do nothing;
