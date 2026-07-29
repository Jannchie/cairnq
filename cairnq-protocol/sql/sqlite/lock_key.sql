-- No-op on SQLite: BEGIN IMMEDIATE already serializes every keyed transaction
-- on the database's single write lock, so there is nothing further to lock.
-- Exists so the shared TaskStore logic can take the key lock unconditionally;
-- see the postgres dialect for the real one.
select 1 as locked;
