from __future__ import annotations

from taskqueue_toolkit.queue.dsn import SnsDsn
from taskqueue_toolkit.queue.sns import ClientFactory, SnsTaskQueue


def _encode(task: str) -> bytes:
    return task.encode()


def _decode(body: bytes) -> str:
    return body.decode()


async def test_publish_then_consume_roundtrips_the_task(
    dsn: SnsDsn, client_factory: ClientFactory
) -> None:
    queue = SnsTaskQueue(dsn=dsn, encode=_encode, decode=_decode, client_factory=client_factory)

    await queue.publish("hello")

    async for queued in queue.consume():
        assert queued.task == "hello"
        await queued.ack()
        break


async def test_nack_with_requeue_redelivers_the_task(
    dsn: SnsDsn, client_factory: ClientFactory
) -> None:
    queue = SnsTaskQueue(dsn=dsn, encode=_encode, decode=_decode, client_factory=client_factory)
    await queue.publish("hello")

    consumer = queue.consume()
    first = await anext(consumer)
    await first.nack(requeue=True)

    second = await anext(consumer)
    assert second.task == "hello"
    await second.ack()
