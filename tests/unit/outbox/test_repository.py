from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue_toolkit.outbox.orm import OutboxRow
from taskqueue_toolkit.outbox.repository import OutboxRepository


def _encode(task: str) -> bytes:
    return task.encode()


def _decode(body: bytes) -> str:
    return body.decode()


async def test_enqueue_then_claim_pending_roundtrips_the_task(session: AsyncSession) -> None:
    repo = OutboxRepository(session=session, encode=_encode, decode=_decode)

    await repo.enqueue("hello")
    await session.commit()

    entries = await repo.claim_pending(limit=10)

    assert len(entries) == 1
    assert entries[0].task == "hello"
    assert entries[0].attempts == 0


async def test_claim_pending_does_not_return_already_claimed_entries(
    session: AsyncSession,
) -> None:
    repo = OutboxRepository(session=session, encode=_encode, decode=_decode)
    await repo.enqueue("hello")
    await session.commit()

    first_claim = await repo.claim_pending(limit=10)
    second_claim = await repo.claim_pending(limit=10)

    assert len(first_claim) == 1
    assert len(second_claim) == 0


async def test_mark_published_removes_entry_from_future_claims(session: AsyncSession) -> None:
    repo = OutboxRepository(session=session, encode=_encode, decode=_decode)
    await repo.enqueue("hello")
    await session.commit()
    [entry] = await repo.claim_pending(limit=10)

    await repo.mark_published(entry.id)

    # Nothing left pending or claimed-but-unpublished to reclaim.
    assert await repo.claim_pending(limit=10) == []


async def test_mark_failed_below_max_attempts_returns_entry_to_pending(
    session: AsyncSession,
) -> None:
    repo = OutboxRepository(session=session, encode=_encode, decode=_decode)
    await repo.enqueue("hello")
    await session.commit()
    [entry] = await repo.claim_pending(limit=10)

    await repo.mark_failed(entry.id, "boom")

    entries = await repo.claim_pending(limit=10)
    assert len(entries) == 1
    assert entries[0].attempts == 1


async def test_mark_failed_at_max_attempts_stops_reclaiming(session: AsyncSession) -> None:
    repo = OutboxRepository(session=session, encode=_encode, decode=_decode)
    await repo.enqueue("hello")
    await session.commit()

    for _ in range(5):
        [entry] = await repo.claim_pending(limit=10)
        await repo.mark_failed(entry.id, "boom")

    assert await repo.claim_pending(limit=10) == []


async def test_enqueue_stores_correlation_id_when_extractor_given(session: AsyncSession) -> None:
    repo = OutboxRepository(
        session=session,
        encode=_encode,
        decode=_decode,
        correlation_id=lambda task: f"corr-{task}",
    )

    await repo.enqueue("hello")
    await session.commit()
    [entry] = await repo.claim_pending(limit=10)

    row = await session.get(OutboxRow, entry.id)
    assert row is not None
    assert row.correlation_id == "corr-hello"


async def test_enqueue_leaves_correlation_id_null_when_no_extractor_given(
    session: AsyncSession,
) -> None:
    repo = OutboxRepository(session=session, encode=_encode, decode=_decode)

    await repo.enqueue("hello")
    await session.commit()
    [entry] = await repo.claim_pending(limit=10)

    row = await session.get(OutboxRow, entry.id)
    assert row is not None
    assert row.correlation_id is None
