from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue_toolkit.outbox.relay import OutboxRelay
from taskqueue_toolkit.outbox.repository import OutboxRepository
from taskqueue_toolkit.queue.task_queue import QueuedTask


def _encode(task: str) -> bytes:
    return task.encode()


def _decode(body: bytes) -> str:
    return body.decode()


@dataclass(slots=True)
class _FakeTaskQueue:
    published: list[str] = field(default_factory=list)
    fail_next: bool = False

    async def publish(self, task: str) -> None:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("broker unreachable")
        self.published.append(task)

    def consume(self) -> AsyncIterator[QueuedTask[str]]:
        raise NotImplementedError


async def test_relay_once_publishes_pending_entries_and_marks_them_published(
    session: AsyncSession,
) -> None:
    outbox = OutboxRepository(session=session, encode=_encode, decode=_decode)
    await outbox.enqueue("hello")
    await session.commit()
    queue = _FakeTaskQueue()
    relay = OutboxRelay(outbox=outbox, task_queue=queue)

    claimed = await relay.relay_once()

    assert claimed == 1
    assert queue.published == ["hello"]
    assert await outbox.claim_pending(limit=10) == []


async def test_relay_once_marks_entry_failed_and_leaves_it_reclaimable(
    session: AsyncSession,
) -> None:
    outbox = OutboxRepository(session=session, encode=_encode, decode=_decode)
    await outbox.enqueue("hello")
    await session.commit()
    queue = _FakeTaskQueue(fail_next=True)
    relay = OutboxRelay(outbox=outbox, task_queue=queue)

    claimed = await relay.relay_once()

    assert claimed == 1
    assert queue.published == []
    entries = await outbox.claim_pending(limit=10)
    assert len(entries) == 1
    assert entries[0].attempts == 1


async def test_relay_once_returns_zero_when_nothing_pending(session: AsyncSession) -> None:
    outbox = OutboxRepository(session=session, encode=_encode, decode=_decode)
    queue = _FakeTaskQueue()
    relay = OutboxRelay(outbox=outbox, task_queue=queue)

    assert await relay.relay_once() == 0
