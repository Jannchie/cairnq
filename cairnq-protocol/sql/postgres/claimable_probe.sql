-- Read-only check: is there anything worth opening the claim transaction for?
-- Run before claim so an idle worker's poll costs one statement instead of a
-- transaction. Mirrors claim.sql's filters, so the probe never promises work
-- claim will skip. The expired-lease arm stays unfiltered on purpose: recovering
-- a dead worker's task is every worker's job, whatever names it happens to
-- handle.
--
-- The SQLite twin exists for a reason that does not apply here — keeping idle
-- workers off the single write lock — and this dialect went without one on that
-- basis. What survives the difference is the rest of the poll: without a probe
-- every empty poll still opens a transaction, runs recover_leases, and then runs
-- one claim statement per self-limiting name. A worker declaring a dozen such
-- names pays a dozen statements to learn there is nothing to do, and on this
-- dialect the empty poll is the COMMON case precisely because LISTEN wakes the
-- worker on the rare one.
--
-- Two separate EXISTS, not one select with an OR: an OR across two different
-- index shapes gets no index at all (see migration 0008's note on the SQLite
-- twin), while each EXISTS here chooses its own — cairnq_tasks_claim_idx for the
-- queued arm, cairnq_tasks_lease_idx (0004, partial on running rows) for the
-- lease arm — and stops at the first row it finds.
--
-- Time is clock_timestamp() wrapped in a scalar subselect, for the reason spelled
-- out at length in recover_leases.sql: inlined, a VOLATILE function becomes a
-- per-row filter and the scan degrades to reading every candidate row. Wrapped,
-- it is an InitPlan evaluated once and usable as an index bound.
--
-- What this does NOT make free: the queued arm still walks the (queue, status)
-- range looking for a due row when every row in it is backing off, the same cost
-- an empty claim draw pays. The saving is the transaction and the other N-1
-- statements, not the range scan.
-- params: queues (text[]), names (text[] or null)
select (
    exists (
        select 1 from cairnq_tasks
        where status = 'queued'
          and queue = any(:queues::text[])
          and (:names::text[] is null or name = any(:names::text[]))
          and run_at_ms <= (select (extract(epoch from clock_timestamp()) * 1000)::bigint)
    )
    or exists (
        select 1 from cairnq_tasks
        where status = 'running'
          and lease_until_ms is not null
          and lease_until_ms <= (select (extract(epoch from clock_timestamp()) * 1000)::bigint)
    )
) as has_work;
