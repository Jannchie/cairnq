"""Cross-language E2E: Python API submits and waits, prints `RESULT <json>`."""

import asyncio
import json
import sys

from cairnq import CairnQ


async def main() -> None:
    db, name, payload_json = sys.argv[1], sys.argv[2], sys.argv[3]
    tasks = CairnQ.sqlite(db)
    await tasks.connect()
    try:
        result = await tasks.call(
            name, json.loads(payload_json), wait_timeout_ms=15_000, poll_ms=50
        )
        print("RESULT " + json.dumps(result), flush=True)
    finally:
        await tasks.close()


asyncio.run(main())
