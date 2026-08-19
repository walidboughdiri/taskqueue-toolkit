from __future__ import annotations

from collections.abc import AsyncGenerator, Iterator
from dataclasses import dataclass

import pytest

from taskqueue_toolkit.queue.dsn import UnsupportedDsnSchemeError
from taskqueue_toolkit.queue.factory import create_task_queue
from taskqueue_toolkit.queue.registry import (
    SchemeAlreadyRegisteredError,
    register_scheme,
    resolve_registered_scheme,
    unregister_scheme,
)
from taskqueue_toolkit.queue.task_queue import Decoder, Encoder, QueuedTask


@dataclass(slots=True)
class _FakeKafkaTaskQueue:
    dsn: str
    encode: Encoder[str]
    decode: Decoder[str]

    async def publish(self, task: str) -> None: ...

    async def consume(self) -> AsyncGenerator[QueuedTask[str]]:
        yield _FakeKafkaQueuedTask(task="fake")


@dataclass(slots=True)
class _FakeKafkaQueuedTask:
    task: str

    async def ack(self) -> None: ...
    async def nack(self, *, requeue: bool) -> None: ...


@pytest.fixture(autouse=True)
def _cleanup_kafka_scheme() -> Iterator[None]:
    yield
    unregister_scheme("kafka")


def test_resolve_registered_scheme_returns_none_for_unregistered_scheme() -> None:
    assert resolve_registered_scheme("kafka://localhost:9092/topic") is None


def test_register_scheme_makes_it_resolvable() -> None:
    def handler(dsn: str, encode: Encoder[str], decode: Decoder[str]) -> _FakeKafkaTaskQueue:
        return _FakeKafkaTaskQueue(dsn=dsn, encode=encode, decode=decode)

    register_scheme("kafka", handler)

    resolved = resolve_registered_scheme("kafka://localhost:9092/topic")
    assert resolved is handler


def test_register_scheme_twice_for_same_scheme_raises() -> None:
    def handler(dsn: str, encode: Encoder[str], decode: Decoder[str]) -> _FakeKafkaTaskQueue:
        return _FakeKafkaTaskQueue(dsn=dsn, encode=encode, decode=decode)

    register_scheme("kafka", handler)

    with pytest.raises(SchemeAlreadyRegisteredError):
        register_scheme("kafka", handler)


def test_create_task_queue_dispatches_to_a_registered_scheme() -> None:
    def handler(dsn: str, encode: Encoder[str], decode: Decoder[str]) -> _FakeKafkaTaskQueue:
        return _FakeKafkaTaskQueue(dsn=dsn, encode=encode, decode=decode)

    register_scheme("kafka", handler)

    queue = create_task_queue(
        "kafka://localhost:9092/topic",
        encode=lambda task: task.encode(),
        decode=lambda body: body.decode(),
    )

    assert isinstance(queue, _FakeKafkaTaskQueue)
    assert queue.dsn == "kafka://localhost:9092/topic"


def test_create_task_queue_still_raises_for_a_scheme_nobody_registered() -> None:
    with pytest.raises(UnsupportedDsnSchemeError):
        create_task_queue(
            "kafka://localhost:9092/topic",
            encode=lambda task: task.encode(),
            decode=lambda body: body.decode(),
        )
