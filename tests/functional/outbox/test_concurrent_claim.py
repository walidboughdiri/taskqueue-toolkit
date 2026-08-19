from __future__ import annotations

import asyncio

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskqueue_toolkit.outbox.repository import OutboxRepository


def _encode(task: str) -> bytes:
    return task.encode()


def _decode(body: bytes) -> str:
    return body.decode()


async def test_concurrent_claim_pending_never_double_claims(session: AsyncSession) -> None:
    """Regression test for SELECT ... FOR UPDATE SKIP LOCKED: five relay
    instances race to claim the same 20 pending entries. Every entry must be
    claimed by exactly one of them."""
    engine = session.bind
    assert engine is not None
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    setup_repo = OutboxRepository(session=session, encode=_encode, decode=_decode)
    for i in range(20):
        await setup_repo.enqueue(f"task-{i}")
    await session.commit()

    async def claim_from_own_session() -> list[str]:
        async with session_factory() as relay_session:
            repo = OutboxRepository(session=relay_session, encode=_encode, decode=_decode)
            entries = await repo.claim_pending(limit=20)
            return [str(entry.id) for entry in entries]

    results = await asyncio.gather(*(claim_from_own_session() for _ in range(5)))

    all_claimed_ids = [entry_id for batch in results for entry_id in batch]
    assert len(all_claimed_ids) == 20
    assert len(set(all_claimed_ids)) == 20
