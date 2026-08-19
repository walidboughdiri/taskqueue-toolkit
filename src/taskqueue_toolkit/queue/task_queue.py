from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import Protocol, TypeVar

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

Encoder = Callable[[T], bytes]
Decoder = Callable[[bytes], T]


class QueuedTask(Protocol[T_co]):
    """A task handed to a consumer, together with how to close it out.

    ack() and nack() are the only broker-specific detail a consumer needs:
    the concrete queue implementation decides what "acknowledged" or
    "requeue this" actually means (delete from RabbitMQ, delete from SQS,
    reset a Redis Streams pending entry, ...).
    """

    @property
    def task(self) -> T_co: ...

    async def ack(self) -> None:
        """Mark the task as successfully processed."""
        ...

    async def nack(self, *, requeue: bool) -> None:
        """Mark the task as failed. requeue=True retries it; False drops it
        for good (or routes it to a dead-letter destination, if the broker
        and its configuration support one — this call itself never
        configures that, only triggers the broker's existing behavior)."""
        ...


class TaskQueue(Protocol[T]):
    """A broker-agnostic queue of tasks of type T.

    Deliberately minimal — publish(), consume() (an AsyncIterator of
    QueuedTask[T]), and ack()/nack(requeue=...) on each received task — so it
    stays implementable by brokers with very different delivery models
    (AMQP queues, SQS, SNS fan-out, Redis Streams consumer groups, Pub/Sub)
    without the contract assuming anything specific to one of them: no
    routing keys, no topics/partitions, no consumer groups, no FIFO groups.

    T is never assumed to be any particular shape. Each concrete
    implementation is handed an Encoder[T]/Decoder[T] pair at construction
    time — this package has no opinion on how your task type serializes,
    only on how it moves through a queue once serialized.
    """

    async def publish(self, task: T) -> None:
        """Enqueue a task for a consumer to pick up."""
        ...

    def consume(self) -> AsyncIterator[QueuedTask[T]]:
        """Yield tasks as they become available, one at a time. Each yielded
        task must be ack()'d or nack()'d by the consumer — the queue
        implementation decides what happens to a task that's never
        acknowledged (redelivery, visibility timeout expiry, etc.)."""
        ...
