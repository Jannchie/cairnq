"""Python interpreter for cairnq-protocol/conformance scenarios. The TS SDK ships
an equivalent; both run the same JSON files verbatim. Spec: conformance/README.md."""

from __future__ import annotations

import asyncio
from typing import Any

from cairnq import CairnQ
from cairnq.store.base import TaskStore


def _field(obj: Any, name: str) -> Any:
    if obj is None:  # mirror the TS runner: field of a null target is null
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    if isinstance(obj, (list, tuple)):
        return obj[int(name)]
    return getattr(obj, name)


class Runner:
    def __init__(self, client: CairnQ):
        self.client = client
        self.store: TaskStore = client.store
        self.saved: dict[str, Any] = {}

    # --- references ---
    def resolve(self, value: Any) -> Any:
        if isinstance(value, str) and value.startswith("$"):
            return self._ref(value)
        if isinstance(value, dict):
            return {k: self.resolve(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.resolve(v) for v in value]
        return value

    def _ref(self, ref: str) -> Any:
        parts = ref[1:].split(".")
        cur = self.saved[parts[0]]
        for p in parts[1:]:
            cur = _field(cur, p)
        return cur

    # --- execution ---
    async def run(self, steps: list[dict[str, Any]]) -> None:
        for step in steps:
            await self.run_step(step)

    async def run_step(self, step: dict[str, Any]) -> None:
        op = step["op"]
        if op == "expect":
            target = self.resolve(step["target"]) if "target" in step else None
            self._assert(target, step)
            return

        args = self.resolve(step.get("args", {}))
        expect_error = step.get("expectError")
        save = step.get("save")
        result = None
        error: Exception | None = None
        try:
            result = await self._dispatch(op, args)
        except Exception as exc:  # noqa: BLE001 — scenarios assert on error type
            error = exc

        if expect_error:
            assert error is not None, f"{op}: expected error {expect_error}, got success"
            assert type(error).__name__ == expect_error, (
                f"{op}: expected {expect_error}, got {type(error).__name__}: {error}"
            )
            if save:
                self.saved[save] = {
                    "task_id": getattr(error, "task_id", None),
                    "type": type(error).__name__,
                }
            return

        if error is not None:
            raise error
        if save:
            self.saved[save] = result
        if "expect" in step:
            self._assert(result, step["expect"])

    async def _dispatch(self, op: str, a: dict[str, Any]) -> Any:
        c, s = self.client, self.store
        if op == "submit":
            # The scenario arg keeps the protocol's spelling; the SDK option is
            # `delay_ms`.
            opts = dict(
                key=a.get("key"), queue=a.get("queue", "default"),
                conflict=a.get("conflict", "reuse"),
                max_attempts=a.get("max_attempts", 3), priority=a.get("priority", 0),
                metadata=a.get("metadata"),
                delay_ms=a.get("run_at_delay_ms", 0),
            )
            # correlation_id is a store-level submit field: the public client
            # submit no longer carries the parent/root/correlation trio, which
            # TaskContext.submit wires for a child. A scenario that sets one goes
            # through the store — the same submit the client delegates to.
            if a.get("correlation_id") is not None:
                return await s.submit(
                    name=a["name"], payload=a.get("payload", {}),
                    correlation_id=a["correlation_id"], **opts,
                )
            return await c.submit(a["name"], a.get("payload", {}), **opts)
        if op == "get":
            return await c.get(a["id"])
        if op == "get_by_key":
            return await c.get_by_key(a["key"])
        if op == "get_status":
            return await c.get_status(a["id"])
        if op == "get_status_by_key":
            return await c.get_status_by_key(a["key"])
        if op == "list":
            return await c.list(**a)
        if op == "cancel":
            return await c.cancel(a["id"])
        if op == "cancel_by_key":
            return await c.cancel_by_key(a["key"])
        if op == "retry":
            return await c.retry(a["id"], reset_attempt=a.get("reset_attempt", False))
        if op == "retry_by_key":
            return await c.retry_by_key(a["key"], reset_attempt=a.get("reset_attempt", False))
        if op == "claim":
            return await s.claim(
                queues=a["queues"], worker_id=a["worker_id"],
                lease_ms=a.get("lease_ms", 30_000), limit=a.get("limit", 1),
                # Absent (not []) means "no name filter" — the two are different claims.
                names=a.get("names"),
            )
        if op == "heartbeat":
            return await s.heartbeat(
                task_id=a["id"], worker_id=a["worker_id"], lease_ms=a.get("lease_ms", 30_000)
            )
        if op == "heartbeat_batch":
            ids = a["ids"]
            renewed = await s.heartbeat_batch(
                task_ids=ids, worker_id=a["worker_id"], lease_ms=a.get("lease_ms", 30_000)
            )
            # The statement answers with a mapping, which scenarios cannot index
            # by a saved id. Flatten it to the rows that came back, in the order
            # the ids were asked for: a task absent from the mapping lost its
            # lease, so it is absent here too and `length` reads as "how many are
            # still ours".
            return [{"id": i, "cancel_requested": renewed[i]} for i in ids if i in renewed]
        if op == "progress":
            return await s.progress(
                task_id=a["id"], worker_id=a["worker_id"],
                progress=a.get("progress"), message=a.get("message"),
            )
        if op == "succeed":
            return await s.succeed(task_id=a["id"], worker_id=a["worker_id"], result=a.get("result"))
        if op == "complete":
            return await s.complete(task_id=a["id"], worker_id=a["worker_id"], result=a.get("result"))
        if op == "fail":
            return await s.fail(
                task_id=a["id"], worker_id=a["worker_id"], error=a["error"],
                retryable=a.get("retryable", True), delay_ms=a.get("delay_ms", 0),
            )
        if op == "purge":
            return await c.purge(
                older_than_ms=a.get("older_than_ms", 0),
                queue=a.get("queue"),
                status=a.get("status"),
                name=a.get("name"),
                limit=a.get("limit", 1000),
            )
        if op == "stats":
            return await c.stats(a.get("queue"))
        if op == "queue_depth":
            return await c.queue_depth(a["queue"], a["max_depth"])
        if op == "sleep":
            await asyncio.sleep(a["ms"] / 1000)
            return None
        raise ValueError(f"unknown op: {op}")

    @staticmethod
    def _deep_equal(a: Any, b: Any) -> bool:
        """Value equality that also pins the type, mirroring the TypeScript
        runner's deepEqual (which compares `typeof` before values).

        Plain `==` is looser in Python than in JS in exactly the direction this
        suite cannot afford: `True == 1` and `1 == 1.0` both hold, so a scenario
        written to pin that a payload stays a *string* (or an int, or a bool)
        would pass on one SDK and mean something weaker on the other. An
        assertion primitive that is stricter in one runner than the other is a
        hole in the thing this suite exists to guarantee."""
        if isinstance(a, bool) != isinstance(b, bool):
            return False
        if isinstance(a, dict) and isinstance(b, dict):
            return a.keys() == b.keys() and all(
                Runner._deep_equal(a[k], b[k]) for k in a
            )
        if isinstance(a, list) and isinstance(b, list):
            return len(a) == len(b) and all(
                Runner._deep_equal(x, y) for x, y in zip(a, b, strict=True)
            )
        if type(a) is not type(b):
            return False
        return bool(a == b)

    def _assert(self, target: Any, spec: dict[str, Any]) -> None:
        if "equals" in spec:
            for k, v in spec["equals"].items():
                actual = _field(target, k)
                assert self._deep_equal(actual, self.resolve(v)), (
                    f"equals {k}: {actual!r} != {v!r}"
                )
        if "equalsRef" in spec:
            assert self._deep_equal(target, self.resolve(spec["equalsRef"])), (
                f"equalsRef: {target!r} != {self.resolve(spec['equalsRef'])!r}"
            )
        if "notEqualsRef" in spec:
            assert not self._deep_equal(target, self.resolve(spec["notEqualsRef"])), (
                "notEqualsRef matched"
            )
        if "greaterThanRef" in spec:
            ref = self.resolve(spec["greaterThanRef"])
            assert target > ref, f"greaterThanRef: {target!r} !> {ref!r}"
        if "length" in spec:
            assert len(target) == spec["length"], f"length {len(target)} != {spec['length']}"
        if "notNull" in spec:
            for name in spec["notNull"]:
                assert _field(target, name) is not None, f"{name} is null"
        if "isNull" in spec:
            for name in spec["isNull"]:
                assert _field(target, name) is None, f"{name} is not null"
