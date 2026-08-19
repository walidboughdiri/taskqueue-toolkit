from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from taskqueue_toolkit.outbox.repository import OutboxRepository
from taskqueue_toolkit.queue.task_queue import TaskQueue

logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 20
_DEFAULT_POLL_INTERVAL_SECONDS = 2.0


@dataclass(slots=True)
class OutboxRelay[T]:
    """Delivers outbox entries to the broker.

    This is the only bridge between the outbox's storage (the durability
    guarantee — an entry committed in the same transaction as the state
    change it follows from is never lost even if the broker is briefly
    unreachable) and TaskQueue[T] (the actual delivery mechanism, whichever
    broker is configured). Neither side ever talks to the other directly.
    """

    outbox: OutboxRepository[T]
    task_queue: TaskQueue[T]
    batch_size: int = _DEFAULT_BATCH_SIZE

    async def relay_once(self) -> int:
        """Claim and publish one batch of pending entries. Returns how many
        were claimed, so a caller can decide whether to poll again
        immediately (batch was full) or wait (batch was empty/partial)."""
        entries = await self.outbox.claim_pending(self.batch_size)
        for entry in entries:
            try:
                await self.task_queue.publish(entry.task)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "outbox entry failed to publish",
                    extra={"outbox_id": str(entry.id)},
                    exc_info=exc,
                )
                await self.outbox.mark_failed(entry.id, str(exc))
            else:
                await self.outbox.mark_published(entry.id)
                logger.info("outbox entry published", extra={"outbox_id": str(entry.id)})
        return len(entries)

    async def run_forever(
        self, poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS
    ) -> None:
        """Poll indefinitely. Intended to run as a background task/process,
        not inside a request handler."""
        while True:
            claimed = await self.relay_once()
            if claimed < self.batch_size:
                await asyncio.sleep(poll_interval_seconds)
