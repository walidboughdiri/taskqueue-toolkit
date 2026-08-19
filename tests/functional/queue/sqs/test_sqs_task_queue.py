from __future__ import annotations

from taskqueue_toolkit.queue.dsn import SqsDsn
from taskqueue_toolkit.queue.sqs import ClientFactory, SqsTaskQueue


def _encode(task: str) -> bytes:
    return task.encode()


def _decode(body: bytes) -> str:
    return body.decode()


async def test_publish_then_consume_roundtrips_the_task(
    dsn: SqsDsn, client_factory: ClientFactory
) -> None:
    queue = SqsTaskQueue(dsn=dsn, encode=_encode, decode=_decode, client_factory=client_factory)

    await queue.publish("hello")

    async for queued in queue.consume():
        assert queued.task == "hello"
        await queued.ack()
        break


async def test_nack_with_requeue_redelivers_the_task(
    dsn: SqsDsn, client_factory: ClientFactory
) -> None:
    queue = SqsTaskQueue(dsn=dsn, encode=_encode, decode=_decode, client_factory=client_factory)
    await queue.publish("hello")

    consumer = queue.consume()
    first = await anext(consumer)
    await first.nack(requeue=True)

    second = await anext(consumer)
    assert second.task == "hello"
    await second.ack()


async def test_roundtrips_binary_unsafe_payload(dsn: SqsDsn, client_factory: ClientFactory) -> None:
    """SQS's MessageBody is a str field — this exercises the base64 boundary
    that lets an arbitrary bytes payload (not just UTF-8-safe text) survive
    the trip, since Encoder[T]/Decoder[T] promise bytes in/out."""
    queue = SqsTaskQueue(
        dsn=dsn,
        encode=lambda n: bytes([n, 0, 255, 128]),
        decode=lambda body: body[0],
        client_factory=client_factory,
    )

    await queue.publish(42)

    async for queued in queue.consume():
        assert queued.task == 42
        await queued.ack()
        break
