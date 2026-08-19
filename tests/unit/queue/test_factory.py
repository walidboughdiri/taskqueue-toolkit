from __future__ import annotations

import pytest

from taskqueue_toolkit.queue.factory import MissingAwsAuthError, create_task_queue
from taskqueue_toolkit.queue.pubsub import PubsubTaskQueue
from taskqueue_toolkit.queue.rabbitmq import RabbitMqTaskQueue
from taskqueue_toolkit.queue.redis_streams import RedisStreamsTaskQueue
from taskqueue_toolkit.queue.sns import SnsTaskQueue
from taskqueue_toolkit.queue.sqs import SqsTaskQueue


def _encode(task: str) -> bytes:
    return task.encode()


def _decode(body: bytes) -> str:
    return body.decode()


@pytest.mark.parametrize(
    ("dsn", "expected_type"),
    [
        ("amqp://guest:guest@localhost:5672/?queue=t", RabbitMqTaskQueue),
        ("redis://localhost:6379/0?stream=t", RedisStreamsTaskQueue),
        ("sqs://eu-west-3/t?iam_role=true", SqsTaskQueue),
        ("sns://eu-west-3/t?iam_role=true", SnsTaskQueue),
        ("pubsub://project/t", PubsubTaskQueue),
    ],
)
def test_builds_the_adapter_matching_the_dsn_scheme(dsn: str, expected_type: type) -> None:
    queue = create_task_queue(dsn, encode=_encode, decode=_decode)

    assert isinstance(queue, expected_type)


def test_raises_when_aws_auth_missing_outside_development() -> None:
    with pytest.raises(MissingAwsAuthError):
        create_task_queue(
            "sqs://eu-west-3/t", encode=_encode, decode=_decode, environment="production"
        )


def test_allows_missing_aws_auth_in_development() -> None:
    queue = create_task_queue(
        "sqs://eu-west-3/t", encode=_encode, decode=_decode, environment="development"
    )

    assert isinstance(queue, SqsTaskQueue)
