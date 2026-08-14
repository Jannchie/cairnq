-- Serves purge.sql's optional status filter: without it a filtered sweep walks
-- cairnq_tasks_completed_idx and visits the table row of every terminal task
-- older than the cutoff just to discard the wrong statuses — worst exactly in
-- the tiered configuration the filter exists for (a minutes-scale succeeded
-- cutoff scanning a day's worth of retained failed rows, every sweep). With
-- (status, completed_at_ms) each filtered sweep is a bounded range seek already
-- in completion order. Unfiltered purge keeps using cairnq_tasks_completed_idx.
create index if not exists cairnq_tasks_status_completed_idx
    on cairnq_tasks (status, completed_at_ms);

update cairnq_meta set value = '7' where key = 'schema_version';
