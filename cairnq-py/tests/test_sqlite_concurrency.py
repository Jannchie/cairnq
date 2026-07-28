"""Several handles on one database, inside one process.

The TypeScript SDK holds SQLite's write lock across `await`s on a synchronous
driver, so two handles on one file there could deadlock the only thread. Python's
driver runs each connection on its own thread and has no such inversion — this
pins that, so the two SDKs are known to agree rather than assumed to."""

from __future__ import annotations

import asyncio

from cairnq import CairnQ


async def test_two_handles_on_one_file_write_concurrently(db_path):
    a, b = CairnQ.sqlite(db_path), CairnQ.sqlite(db_path)
    await a.connect()
    await b.connect()
    try:
        # Keyed submits, so each goes through a transaction rather than a single
        # statement — that is what holds the write lock across an await.
        tasks = await asyncio.wait_for(
            asyncio.gather(
                *(a.submit("job", {"i": i}, key=f"a{i}") for i in range(10)),
                *(b.submit("job", {"i": i}, key=f"b{i}") for i in range(10)),
            ),
            timeout=30,
        )
        assert len({t.id for t in tasks}) == 20
    finally:
        await a.close()
        await b.close()
