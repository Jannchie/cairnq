# CairnQ conformance scenarios

Language-neutral behavior specs. Both SDKs ship a small interpreter that runs
these **verbatim** against a fresh `tasks.db`, so Python and TypeScript are held
to identical behavior. The shared `sql/sqlite/*.sql` pins the SQL; these pin the
semantics layered on top (conflict branches, retry, lease recovery, cancel).

## Scenario shape

```json
{
  "name": "key_reuse",
  "description": "...",
  "steps": [ { "op": "...", "args": {...}, "save": "alias", "expect": {...} } ]
}
```

A step runs one `op`, optionally saves its result under `save`, and optionally
asserts on the result with an inline `expect` (sugar for an `expect` step whose
`target` is this step's result).

## References

Any string argument of the form `$alias`, `$alias.field`, or `$alias.0.field`
(array index) is resolved against previously saved results before the op runs.

## Ops

Client-side: `submit`, `get`, `get_by_key`, `list`, `cancel`, `cancel_by_key`,
`retry`, `retry_by_key`. Worker-side: `claim` (runs lease recovery then claim,
like the worker loop; returns an array), `heartbeat`, `progress`, `succeed`,
`complete`, `fail`. Control: `sleep` (`{ "ms": n }`), `expect`.

`args` keys mirror the SDK call (e.g. `submit` takes `name`, `payload`, `key`,
`queue`, `conflict`, `max_attempts`, `priority`, `correlation_id`; `claim` takes
`queues`, `worker_id`, `lease_ms`, `limit`).

### Expected errors

A step may assert it throws: `"expectError": "AlreadyExists"`. When it does and a
`save` is present, the **error object** is saved (its `task_id` field normalized),
so later steps can reference e.g. `$timeout.task_id`.

## Assertions (`expect`)

Inline `expect` (on the step's own result) or a standalone `{ "op": "expect",
"target": "$alias", ... }` step supports:

- `equals`: subset match — every key/value must equal the target's field.
- `equalsRef` / `notEqualsRef`: target equals / differs from a resolved reference.
- `greaterThanRef`: target is numerically greater than a resolved reference.
- `length`: target array length.
- `notNull` / `isNull`: array of field names on the target that must be non-null / null.

## Out of band

Timing/async-heavy client behavior — `call`/`wait` timeout (§32.7) — is covered
by SDK-native tests rather than this format, since it asserts on polling and
elapsed time. Each SDK's test suite must cover it.
