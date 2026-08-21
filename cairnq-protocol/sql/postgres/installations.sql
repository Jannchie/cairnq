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
-- One row per installation, never zero: the LEFT JOIN keeps `current_schema`
-- readable on a database that holds no cairnq yet, where `schema` is null.
-- Deliberately NOT an array column — pg_namespace.nspname is `name`, and which
-- drivers decode a `name[]` (or a text[]) into a list is exactly the kind of
-- disagreement this protocol keeps out of the SDKs. Scalar columns behave the
-- same everywhere.
--
-- `current_schema()` is null when the search_path names nothing that exists, in
-- which case the caller cannot conclude anything.
-- params: (none)
select
    current_schema()::text as current_schema,
    found.schema
from (select 1) one
left join (
    select n.nspname::text as schema
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where c.relname = 'cairnq_tasks' and c.relkind = 'r'
) found on true
order by found.schema;
