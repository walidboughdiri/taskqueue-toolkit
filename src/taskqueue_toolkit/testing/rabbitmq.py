"""An in-memory double of the slice of aio_pika that RabbitMqTaskQueue
calls, for testing code that uses this package without a real broker.

Usage:
    broker = FakeRabbitMqBroker()
    queue = RabbitMqTaskQueue(dsn=dsn, encode=encode, decode=decode, connect=broker.connect)

Each FakeRabbitMqBroker instance owns its own isolated state — create a new
one per test (or per broker you want to simulate) rather than sharing one,
and there is nothing to reset between tests.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field

__all__ = ["FakeRabbitMqBroker"]


@dataclass(slots=True)
class _Queue:
    """One named queue's backlog plus messages currently delivered but not
    yet ack()'d/nack()'d — mirrors a real broker's per-queue state."""

    pending: deque[bytes] = field(default_factory=deque)
    unacked: dict[int, bytes] = field(default_factory=dict)
    _next_id: int = 0

    def push(self, body: bytes) -> None:
        self.pending.append(body)

    def requeue(self, delivery_id: int) -> None:
        body = self.unacked.pop(delivery_id, None)
        if body is not None:
            self.pending.appendleft(body)

    def drop(self, delivery_id: int) -> None:
        self.unacked.pop(delivery_id, None)

    def next_delivery_id(self) -> int:
        self._next_id += 1
        return self._next_id


@dataclass(slots=True)
class _FakeIncomingMessage:
    body: bytes
    _delivery_id: int
    _queue: _Queue

    async def ack(self) -> None:
        self._queue.drop(self._delivery_id)

    async def nack(self, *, requeue: bool = True) -> None:
        if requeue:
            self._queue.requeue(self._delivery_id)
        else:
            self._queue.drop(self._delivery_id)


@dataclass(slots=True)
class _FakeAioPikaQueue:
    name: str
    _queue: _Queue

    def iterator(self) -> _FakeAioPikaQueue:
        return self

    def __aiter__(self) -> _FakeAioPikaQueue:
        return self

    async def __anext__(self) -> _FakeIncomingMessage:
        while not self._queue.pending:
            await asyncio.sleep(0)
        body = self._queue.pending.popleft()
        delivery_id = self._queue.next_delivery_id()
        self._queue.unacked[delivery_id] = body
        return _FakeIncomingMessage(body=body, _delivery_id=delivery_id, _queue=self._queue)


@dataclass(slots=True)
class _FakeExchange:
    _queue: _Queue

    async def publish(self, message: object, *, routing_key: str) -> None:
        body = getattr(message, "body", b"")
        self._queue.push(body)


@dataclass(slots=True)
class _FakeChannel:
    _broker: FakeRabbitMqBroker
    # Bound by declare_queue() — this fake routes directly to a named queue
    # rather than modeling real exchange/routing-key bindings, so there's
    # nothing meaningful to set until then.
    default_exchange: _FakeExchange | None = field(default=None, init=False)

    async def set_qos(self, *, prefetch_count: int) -> None:
        return None

    async def declare_queue(self, name: str, *, durable: bool = True) -> _FakeAioPikaQueue:
        queue = self._broker._queue_for(name)
        self.default_exchange = _FakeExchange(_queue=queue)
        return _FakeAioPikaQueue(name=name, _queue=queue)

    async def queue_delete(self, name: str) -> None:
        self._broker._queues.pop(name, None)


@dataclass(slots=True)
class _FakeConnection:
    _broker: FakeRabbitMqBroker
    closed: bool = False

    async def channel(self) -> _FakeChannel:
        return _FakeChannel(_broker=self._broker)

    async def close(self) -> None:
        self.closed = True


@dataclass(slots=True)
class FakeRabbitMqBroker:
    """An isolated in-memory RabbitMQ double. Pass `broker.connect` as the
    `connect=` argument to RabbitMqTaskQueue in place of aio_pika.connect_robust."""

    _queues: dict[str, _Queue] = field(default_factory=dict, init=False)

    def _queue_for(self, name: str) -> _Queue:
        return self._queues.setdefault(name, _Queue())

    async def connect(self, url: str) -> _FakeConnection:
        return _FakeConnection(_broker=self)
