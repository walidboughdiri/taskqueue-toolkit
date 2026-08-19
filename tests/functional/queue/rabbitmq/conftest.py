from __future__ import annotations

from typing import cast

import pytest

from taskqueue_toolkit.queue.dsn import RabbitMqDsn
from taskqueue_toolkit.queue.rabbitmq import Connect
from taskqueue_toolkit.testing import FakeRabbitMqBroker


@pytest.fixture
def dsn(request: pytest.FixtureRequest) -> RabbitMqDsn:
    queue_name = f"taskqueue-toolkit.test.{request.node.name}"
    return RabbitMqDsn(url="amqp://fake/", queue_name=queue_name)


@pytest.fixture
def connect() -> Connect:
    # FakeRabbitMqBroker only implements the slice of aio_pika's interface
    # RabbitMqTaskQueue actually calls — cast rather than replicate the
    # full SDK surface just to satisfy strict structural typing here.
    return cast(Connect, FakeRabbitMqBroker().connect)
