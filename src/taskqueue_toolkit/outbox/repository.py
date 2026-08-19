from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue_toolkit.outbox.orm import (
    STATUS_CLAIMED,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_PUBLISHED,
    OutboxRow,
)
from taskqueue_toolkit.queue.task_queue import Decoder, Encoder

_MAX_ATTEMPTS = 5


@dataclass(frozen=True, slots=True)
class OutboxEntry[T]:
    id: uuid.UUID
    task: T
    attempts: int


@dataclass(slots=True)
class OutboxRepository[T]:
    session: AsyncSession
    encode: Encoder[T]
    decode: Decoder[T]
    # Optional: derive a caller-meaningful correlation_id (e.g. an aggregate
    # id) from the task, purely for indexed lookups — this package never
    # interprets it itself. Leave unset if the task type has no natural key.
    correlation_id: Callable[[T], str] | None = None

    async def enqueue(self, task: T) -> None:
        """Add a task to the outbox within the caller's transaction.

        Deliberately does not commit — the caller writes this in the same
        transaction as whatever state change the task follows from, so
        either both land or neither does. The relay is what actually
        delivers the task to the broker later.
        """
        self.session.add(
            OutboxRow(
                id=uuid.uuid4(),
                correlation_id=self.correlation_id(task) if self.correlation_id else None,
                payload=self.encode(task),
                status=STATUS_PENDING,
                attempts=0,
                created_at=datetime.now(UTC),
            )
        )

    async def claim_pending(self, limit: int) -> list[OutboxEntry[T]]:
        """Atomically mark up to `limit` pending entries as claimed and
        return them, skipping rows already locked by another relay
        (SKIP LOCKED) so multiple relay instances can run concurrently
        without claiming the same task twice. The row lock is only held for
        this one UPDATE — not across the network call to the broker that
        follows — callers report the outcome via mark_published()/
        mark_failed() afterwards.

        Only verified against Postgres (this package's own test suite races
        multiple relays against a real Postgres to confirm no double-claim).
        MySQL 8.0+/MariaDB 10.6+ support the same SKIP LOCKED syntax via
        SQLAlchemy but aren't tested here — treat that combination as
        unverified. Older MySQL/MariaDB and SQLite don't give this method
        its concurrency guarantee at all.
        """
        result = await self.session.execute(
            select(OutboxRow)
            .where(OutboxRow.status == STATUS_PENDING)
            .order_by(OutboxRow.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = result.scalars().all()
        entries = [
            OutboxEntry(id=row.id, task=self.decode(row.payload), attempts=row.attempts)
            for row in rows
        ]
        for row in rows:
            row.status = STATUS_CLAIMED
        await self.session.commit()
        return entries

    async def mark_published(self, entry_id: uuid.UUID) -> None:
        row = await self.session.get(OutboxRow, entry_id)
        if row is None:
            return
        row.status = STATUS_PUBLISHED
        row.published_at = datetime.now(UTC)
        await self.session.commit()

    async def mark_failed(self, entry_id: uuid.UUID, error: str) -> None:
        row = await self.session.get(OutboxRow, entry_id)
        if row is None:
            return
        row.attempts += 1
        row.last_error = error
        row.status = STATUS_FAILED if row.attempts >= _MAX_ATTEMPTS else STATUS_PENDING
        await self.session.commit()
