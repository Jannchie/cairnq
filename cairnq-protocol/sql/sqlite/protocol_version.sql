-- The storage's protocol major, from cairnq_meta (written by the migrations).
-- Returns no row on a database from before the first migration.
-- params: (none)
select value from cairnq_meta where key = 'protocol_version';
