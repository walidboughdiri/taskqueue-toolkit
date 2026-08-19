"""This test genuinely needs a real Postgres — unlike the five broker
adapters (taskqueue_toolkit.testing), SELECT ... FOR UPDATE SKIP LOCKED is a
row-level locking guarantee that only means something under real concurrent
transactions. SQLite (file or memory) serializes writes through a single
global lock regardless of SKIP LOCKED, so a SQLite-backed version of this
test would pass even if SKIP LOCKED were deleted from the code — it
wouldn't be testing the thing it exists to test. This test intentionally
skips (not fakes) when Postgres isn't reachable.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from taskqueue_toolkit.outbox.orm import Base

_POSTGRES_DSN = "postgresql+asyncpg://symfony:secret@localhost:5432/taskqueue_toolkit_test"


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(_POSTGRES_DSN)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        await engine.dispose()
        pytest.skip(f"Postgres not reachable at {_POSTGRES_DSN}: {exc}")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE outbox"))
    await engine.dispose()
