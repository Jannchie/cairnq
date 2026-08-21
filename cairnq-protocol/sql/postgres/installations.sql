-- Where cairnq already lives in this database, and where this connection is
-- pointing. Read-only; runs once per connect, before migrations.
--
-- Exists because `search_path` is out-of-band configuration: two processes given
-- the same DSN can still resolve to different schemas, and because every
-- migration is `create table if not exists`, the second one to start does not
-- fail — it quietly builds a parallel, empty installation. Nothing downstream can
-- tell: protocol_version reads from whichever cairnq_meta the connection sees, so
-- the version check passes on both sides while the API's tasks are invisible to
-- the worker forever. The only way to catch that is to look OUTSIDE the
-- connection's own search_path, which is what this does.
--
-- pg_class is per-database, so `installations` is every schema in this database
-- holding a cairnq task table. `current_schema()` is null when the search_path
-- names nothing that exists, in which case the caller cannot conclude anything.
-- params: (none)
select
    current_schema() as current_schema,
    array(
        select n.nspname
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where c.relname = 'cairnq_tasks' and c.relkind = 'r'
        order by n.nspname
    ) as installations;
