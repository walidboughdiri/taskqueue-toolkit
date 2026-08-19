from __future__ import annotations

from taskqueue_toolkit.queue.dsn import RedisStreamsDsn
from taskqueue_toolkit.queue.redis_streams import ClientFactory, RedisStreamsTaskQueue


def _encode(task: str) -> bytes:
    return task.encode()


def _decode(body: bytes) -> str:
    return body.decode()


async def test_publish_then_consume_roundtrips_the_task(
    dsn: RedisStreamsDsn, client_factory: ClientFactory
) -> None:
    queue = RedisStreamsTaskQueue(
        dsn=dsn, encode=_encode, decode=_decode, client_factory=client_factory
    )

    await queue.publish("hello")

    async for queued in queue.consume():
        assert queued.task == "hello"
        await queued.ack()
        break


async def test_nack_with_requeue_redelivers_the_pending_entry(
    dsn: RedisStreamsDsn, client_factory: ClientFactory
) -> None:
    queue = RedisStreamsTaskQueue(
        dsn=dsn, encode=_encode, decode=_decode, client_factory=client_factory
    )
    await queue.publish("hello")

    consumer = queue.consume()
    first = await anext(consumer)
    await first.nack(requeue=True)

    # A fresh consume() call re-reads this consumer's still-pending entries
    # (id="0") before new ones.
    second_consumer = queue.consume()
    second = await anext(second_consumer)
    assert second.task == "hello"
    await second.ack()
