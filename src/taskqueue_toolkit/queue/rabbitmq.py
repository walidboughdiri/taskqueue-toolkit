from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field

import aio_pika

from taskqueue_toolkit.queue.dsn import RabbitMqDsn
from taskqueue_toolkit.queue.task_queue import Decoder, Encoder

logger = logging.getLogger(__name__)

Connect = Callable[[str], Awaitable["aio_pika.abc.AbstractRobustConnection"]]


@dataclass(slots=True)
class RabbitMqQueuedTask[T]:
    _message: aio_pika.abc.AbstractIncomingMessage
    task: T

    async def ack(self) -> None:
        await self._message.ack()

    async def nack(self, *, requeue: bool) -> None:
        await self._message.nack(requeue=requeue)


@dataclass(slots=True)
class RabbitMqTaskQueue[T]:
    dsn: RabbitMqDsn
    encode: Encoder[T]
    decode: Decoder[T]
    # Overridable for tests (inject a fake connection) or to share/pool a
    # connection strategy the caller already has; defaults to the real SDK.
    connect: Connect = field(default=aio_pika.connect_robust)

    async def publish(self, task: T) -> None:
        connection = await self.connect(self.dsn.url)
        try:
            channel = await connection.channel()
            queue = await channel.declare_queue(self.dsn.queue_name, durable=True)
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=self.encode(task),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=queue.name,
            )
            logger.info("task published", extra={"queue": self.dsn.queue_name})
        finally:
            await connection.close()

    async def consume(self) -> AsyncIterator[RabbitMqQueuedTask[T]]:
        connection = await self.connect(self.dsn.url)
        try:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            queue = await channel.declare_queue(self.dsn.queue_name, durable=True)

            async for message in queue.iterator():
                yield RabbitMqQueuedTask(_message=message, task=self.decode(message.body))
        finally:
            # The consumer (a worker's `async for`) can stop iterating at any
            # point — graceful shutdown, an unhandled error, etc. — which
            # raises GeneratorExit right here, inside the yield above. At
            # that point there's no time left to round-trip a clean
            # basic.cancel RPC with the broker (the event loop is usually
            # already shutting down too), so don't try — just drop the
            # connection. RabbitMQ notices the disconnect and requeues
            # whatever message was left unacknowledged.
            with suppress(Exception):
                await connection.close()
