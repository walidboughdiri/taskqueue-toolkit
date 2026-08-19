"""An in-memory double of the slice of aioboto3's SNS+SQS clients that
SnsTaskQueue calls, for testing code that uses this package without a real
topic/queue.

Usage:
    broker = FakeSnsBroker()
    queue = SnsTaskQueue(dsn=dsn, encode=encode, decode=decode, client_factory=broker.client)

publish() fans out directly into whichever SQS queues are subscribed to the
topic, wrapping the body in the same {"Type": ..., "Message": ...} envelope
real SNS-to-SQS delivery uses, since SnsTaskQueue.consume() unwraps that
envelope.

Each FakeSnsBroker instance owns its own isolated state — create a new one
per test rather than sharing one, and there is nothing to reset between
tests.
"""

from __future__ import annotations

import json
import uuid
from collections import deque
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field

__all__ = ["FakeSnsBroker"]


class QueueDoesNotExist(Exception):
    """Raised by the fake SQS client the same way the real SDK's
    client.exceptions.QueueDoesNotExist would be."""


@dataclass(slots=True)
class _Exceptions:
    QueueDoesNotExist: type[Exception] = QueueDoesNotExist


@dataclass(slots=True)
class _Queue:
    pending: deque[dict[str, str]] = field(default_factory=deque)
    in_flight: dict[str, dict[str, str]] = field(default_factory=dict)


def _topic_arn(name: str) -> str:
    return f"arn:aws:sns:fake:000000000000:{name}"


@dataclass(slots=True)
class _FakeSnsClient:
    _broker: FakeSnsBroker

    async def create_topic(self, *, Name: str) -> dict[str, str]:
        arn = _topic_arn(Name)
        self._broker._topics.setdefault(arn, set())
        return {"TopicArn": arn}

    async def delete_topic(self, *, TopicArn: str) -> None:
        self._broker._topics.pop(TopicArn, None)

    async def subscribe(self, *, TopicArn: str, Protocol: str, Endpoint: str) -> dict[str, str]:
        self._broker._topics.setdefault(TopicArn, set()).add(Endpoint)
        return {"SubscriptionArn": f"{TopicArn}:{uuid.uuid4()}"}

    async def publish(self, *, TopicArn: str, Message: str) -> dict[str, str]:
        envelope = json.dumps(
            {
                "Type": "Notification",
                "MessageId": str(uuid.uuid4()),
                "TopicArn": TopicArn,
                "Message": Message,
            }
        )
        for queue_url in self._broker._topics.get(TopicArn, set()):
            queue = self._broker._queues.setdefault(queue_url, _Queue())
            queue.pending.append({"Body": envelope})
        return {"MessageId": str(uuid.uuid4())}


@dataclass(slots=True)
class _FakeSqsClient:
    _broker: FakeSnsBroker
    exceptions: _Exceptions = field(default_factory=_Exceptions)

    async def get_queue_url(self, *, QueueName: str) -> dict[str, str]:
        if QueueName not in self._broker._queues:
            raise self.exceptions.QueueDoesNotExist(QueueName)
        return {"QueueUrl": QueueName}

    async def create_queue(self, *, QueueName: str) -> dict[str, str]:
        self._broker._queues.setdefault(QueueName, _Queue())
        return {"QueueUrl": QueueName}

    async def get_queue_attributes(
        self, *, QueueUrl: str, AttributeNames: list[str]
    ) -> dict[str, dict[str, str]]:
        # This fake uses the queue URL as its own ARN too — good enough
        # since nothing here parses ARN structure, only threads the value
        # through to subscribe()'s Endpoint, which this fake also keys by
        # queue URL.
        return {"Attributes": {"QueueArn": QueueUrl}}

    async def set_queue_attributes(self, *, QueueUrl: str, Attributes: dict[str, str]) -> None:
        return None

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


class _FakeSnsContext(AbstractAsyncContextManager[tuple["_FakeSnsClient", "_FakeSqsClient"]]):
    def __init__(self, broker: FakeSnsBroker) -> None:
        self._broker = broker

    async def __aenter__(self) -> tuple[_FakeSnsClient, _FakeSqsClient]:
        return _FakeSnsClient(_broker=self._broker), _FakeSqsClient(_broker=self._broker)

    async def __aexit__(self, *exc_info: object) -> None:
        return None


@dataclass(slots=True)
class FakeSnsBroker:
    """An isolated in-memory SNS+SQS double. Pass `broker.client` as the
    `client_factory=` argument to SnsTaskQueue in place of the default
    aioboto3-session-based factory."""

    _topics: dict[str, set[str]] = field(default_factory=dict, init=False)
    _queues: dict[str, _Queue] = field(default_factory=dict, init=False)

    def client(
        self, dsn: object
    ) -> AbstractAsyncContextManager[tuple[_FakeSnsClient, _FakeSqsClient]]:
        return _FakeSnsContext(self)
