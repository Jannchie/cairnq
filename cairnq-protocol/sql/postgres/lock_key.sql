-- Serialize all keyed operations on one key (submit-with-key and the *_by_key
-- ops). READ COMMITTED gives those read-then-write sequences nothing to lock
-- when the key row does not exist yet, so two concurrent same-key submits could
-- both see "no existing task" and both insert — two live tasks under one key.
-- An advisory transaction lock on the key's hash closes that window; it
-- releases with the transaction. A hash collision only over-serializes two
-- unrelated keys — it can never under-lock.
-- params: key
select pg_advisory_xact_lock(hashtextextended(:key::text, 0));
