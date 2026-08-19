"""An in-memory double of the slice of aioboto3's SQS client that
SqsTaskQueue calls, for testing code that uses this package without a real
queue.

Usage:
    broker = FakeSqsBroker()
    queue = SqsTaskQueue(dsn=dsn, encode=encode, decode=decode, client_factory=broker.client)

Each FakeSqsBroker instance owns its own isolated state — create a new one
per test rather than sharing one, and there is nothing to reset between
tests.
"""

from __future__ import annotations

import uuid
from collections import deque
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field

__all__ = ["FakeSqsBroker"]


class QueueDoesNotExist(Exception):
    """Raised by the fake client the same way the real SDK's
    client.exceptions.QueueDoesNotExist would be — SqsTaskQueue catches
    this via `except client.exceptions.QueueDoesNotExist`, which this fake
    client exposes as an instance attribute for exactly that reason."""


@dataclass(slots=True)
class _Exceptions:
    QueueDoesNotExist: type[Exception] = QueueDoesNotExist


@dataclass(slots=True)
class _Queue:
    pending: deque[dict[str, str]] = field(default_factory=deque)
    in_flight: dict[str, dict[str, str]] = field(default_factory=dict)


@dataclass(slots=True)
class _FakeSqsClient:
    _broker: FakeSqsBroker
    exceptions: _Exceptions = field(default_factory=_Exceptions)

    async def get_queue_url(self, *, QueueName: str) -> dict[str, str]:
        if QueueName not in self._broker._queues:
            raise self.exceptions.QueueDoesNotExist(QueueName)
        return {"QueueUrl": QueueName}

    async def create_queue(self, *, QueueName: str) -> dict[str, str]:
        self._broker._queues.setdefault(QueueName, _Queue())
        return {"QueueUrl": QueueName}

    async def delete_queue(self, *, QueueUrl: str) -> None:
        self._broker._queues.pop(QueueUrl, None)

    async def send_message(self, *, QueueUrl: str, MessageBody: str) -> dict[str, str]:
        queue = self._broker._queues.setdefault(QueueUrl, _Queue())
        queue.pending.append({"Body": MessageBody})
        return {"MessageId": str(uuid.uuid4())}

    async def receive_message(
        self, *, QueueUrl: str, MaxNumberOfMessages: int = 1, WaitTimeSeconds: int = 0
    ) -> dict[str, list[dict[str, str]]]:
        queue = self._broker._queues.setdefault(QueueUrl, _Queue())
        messages = []
        for _ in range(min(MaxNumberOfMessages, len(queue.pending))):
            body = queue.pending.popleft()
            receipt_handle = str(uuid.uuid4())
            queue.in_flight[receipt_handle] = body
            messages.append({"Body": body["Body"], "ReceiptHandle": receipt_handle})
        return {"Messages": messages}

    async def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> None:
        queue = self._broker._queues.setdefault(QueueUrl, _Queue())
        queue.in_flight.pop(ReceiptHandle, None)

    async def change_message_visibility(
        self, *, QueueUrl: str, ReceiptHandle: str, VisibilityTimeout: int
    ) -> None:
        queue = self._broker._queues.setdefault(QueueUrl, _Queue())
        body = queue.in_flight.pop(ReceiptHandle, None)
        if body is not None and VisibilityTimeout == 0:
            queue.pending.appendleft(body)


class _FakeClientContext(AbstractAsyncContextManager["_FakeSqsClient"]):
    def __init__(self, broker: FakeSqsBroker) -> None:
        self._broker = broker

    async def __aenter__(self) -> _FakeSqsClient:
        return _FakeSqsClient(_broker=self._broker)

    async def __aexit__(self, *exc_info: object) -> None:
        return None


@dataclass(slots=True)
class FakeSqsBroker:
    """An isolated in-memory SQS double. Pass `broker.client` as the
    `client_factory=` argument to SqsTaskQueue in place of the default
    aioboto3-session-based factory."""

    _queues: dict[str, _Queue] = field(default_factory=dict, init=False)

    def client(self, dsn: object) -> AbstractAsyncContextManager[_FakeSqsClient]:
        return _FakeClientContext(self)
