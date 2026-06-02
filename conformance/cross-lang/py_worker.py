"""Cross-language E2E: a Python worker. Handles image.generate; runs until SIGTERM."""

import sys

from cairnq import Worker


def main() -> None:
    db = sys.argv[1]
    worker = Worker.sqlite(
        db, queues=["default", "gpu", "io"], concurrency=4, poll_interval_ms=50, lease_ms=10_000
    )

    @worker.task("image.generate")
    async def image_generate(ctx, payload):
        await ctx.progress(0.5, "rendering")
        return {
            "url": f"s3://img/{payload['prompt']}",
            "size": payload.get("size", "1024x1024"),
            "engine": "python",
        }

    print("PY_WORKER_READY", flush=True)
    worker.serve()  # blocking; closes cleanly on SIGTERM


main()
