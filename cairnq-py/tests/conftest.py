import pytest_asyncio

from cairnq import CairnQ


@pytest_asyncio.fixture
async def db_path(tmp_path):
    return str(tmp_path / "tasks.db")


@pytest_asyncio.fixture
async def client(db_path):
    c = CairnQ.sqlite(db_path)
    await c.connect()
    try:
        yield c
    finally:
        await c.close()
