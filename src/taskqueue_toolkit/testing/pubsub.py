"""An in-memory double of the slice of google-cloud-pubsub that
PubsubTaskQueue calls, for testing code that uses this package without a
real Pub/Sub project or emulator.

Usage:
    broker = FakePubsubBroker()
    queue = PubsubTaskQueue(
        dsn=dsn,
        encode=encode,
        decode=decode,
        publisher_factory=broker.publisher,
        subscriber_factory=broker.subscriber,
    )

All methods on the fake clients are synchronous, matching the real (sync
gRPC) SDK — PubsubTaskQueue always calls through asyncio.to_thread, never
awaits these directly.

Each FakePubsubBroker instance owns its own isolated state — create a new
one per test rather than sharing one, and there is nothing to reset between
tests.
"""

from __future__ import annotations

import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import NamedTuple

from google.api_core.exceptions import AlreadyExists

__all__ = ["FakePubsubBroker"]


class _Message(NamedTuple):
    data: bytes


class _ReceivedMessage(NamedTuple):
    ack_id: str
    message: _Message


class _PullResponse(NamedTuple):
    received_messages: list[_ReceivedMessage]


class _PublishFuture:
    def __init__(self, message_id: str) -> None:
        self._message_id = message_id

    def result(self, timeout: float | None = None) -> str:
        return self._message_id


@dataclass(slots=True)
class _Subscription:
    topic_path: str
    pending: deque[bytes] = field(default_factory=deque)
    in_flight: dict[str, bytes] = field(default_factory=dict)


@dataclass(slots=True)
class _FakePublisherClient:
    _broker: FakePubsubBroker

    def topic_path(self, project: str, topic: str) -> str:
        return f"projects/{project}/topics/{topic}"

    def create_topic(self, *, name: str) -> None:
        if name in self._broker._topics:
            raise AlreadyExists(f"Topic already exists: {name}")  # type: ignore[no-untyped-call]
        self._broker._topics.add(name)

    def delete_topic(self, *, topic: str) -> None:
        self._broker._topics.discard(topic)

    def publish(self, topic_path: str, data: bytes) -> _PublishFuture:
        for sub in self._broker._subscriptions.values():
            if sub.topic_path == topic_path:
                sub.pending.append(data)
        return _PublishFuture(str(uuid.uuid4()))


@dataclass(slots=True)
class _FakeSubscriberClient:
    _broker: FakePubsubBroker

    def subscription_path(self, project: str, subscription: str) -> str:
        return f"projects/{project}/subscriptions/{subscription}"

    def create_subscription(self, *, name: str, topic: str) -> None:
        if name in self._broker._subscriptions:
            raise AlreadyExists(  # type: ignore[no-untyped-call]
                f"Subscription already exists: {name}"
            )
        self._broker._subscriptions[name] = _Subscription(topic_path=topic)

    def delete_subscription(self, *, subscription: str) -> None:
        self._broker._subscriptions.pop(subscription, None)

    def pull(
        self, *, subscription: str, max_messages: int = 1, timeout: float | None = None
    ) -> _PullResponse:
        sub = self._broker._subscriptions.setdefault(
            subscription, _Subscription(topic_path=subscription)
        )
        received = []
        for _ in range(min(max_messages, len(sub.pending))):
            data = sub.pending.popleft()
            ack_id = str(uuid.uuid4())
            sub.in_flight[ack_id] = data
            received.append(_ReceivedMessage(ack_id=ack_id, message=_Message(data=data)))
        return _PullResponse(received_messages=received)

    def acknowledge(self, *, subscription: str, ack_ids: list[str]) -> None:
        sub = self._broker._subscriptions.get(subscription)
        if sub is None:
            return
        for ack_id in ack_ids:
            sub.in_flight.pop(ack_id, None)

    def modify_ack_deadline(
        self, *, subscription: str, ack_ids: list[str], ack_deadline_seconds: int
    ) -> None:
        sub = self._broker._subscriptions.get(subscription)
        if sub is None:
            return
        for ack_id in ack_ids:
            data = sub.in_flight.pop(ack_id, None)
            if data is not None and ack_deadline_seconds == 0:
                sub.pending.appendleft(data)


@dataclass(slots=True)
class FakePubsubBroker:
    """An isolated in-memory Pub/Sub double. Pass `broker.publisher` and
    `broker.subscriber` as the `publisher_factory=`/`subscriber_factory=`
    arguments to PubsubTaskQueue in place of the real SDK client classes."""

    _topics: set[str] = field(default_factory=set, init=False)
    _subscriptions: dict[str, _Subscription] = field(default_factory=dict, init=False)

    def publisher(self) -> _FakePublisherClient:
        return _FakePublisherClient(_broker=self)

    def subscriber(self) -> _FakeSubscriberClient:
        return _FakeSubscriberClient(_broker=self)
