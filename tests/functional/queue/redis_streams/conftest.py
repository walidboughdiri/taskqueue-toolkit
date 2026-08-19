from __future__ import annotations

from typing import cast

import pytest

from taskqueue_toolkit.queue.dsn import RedisStreamsDsn
from taskqueue_toolkit.queue.redis_streams import ClientFactory
from taskqueue_toolkit.testing import FakeRedisStreamsBroker


@pytest.fixture
def dsn(request: pytest.FixtureRequest) -> RedisStreamsDsn:
    stream_name = f"taskqueue-toolkit-test-{request.node.name}"
    return RedisStreamsDsn(
        url="redis://fake/0",
        stream_name=stream_name,
        group="test-group",
        consumer="test-consumer",
    )


@pytest.fixture
def client_factory() -> ClientFactory:
    # FakeRedisStreamsBroker only implements the slice of redis.asyncio's
    # client interface RedisStreamsTaskQueue actually calls — cast rather
    # than replicate the full SDK surface just to satisfy strict
    # structural typing here.
    return cast(ClientFactory, FakeRedisStreamsBroker().client)
