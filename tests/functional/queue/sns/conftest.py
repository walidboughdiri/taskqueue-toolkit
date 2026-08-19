from __future__ import annotations

import hashlib
from typing import cast

import pytest

from taskqueue_toolkit.queue.dsn import SnsDsn
from taskqueue_toolkit.queue.sns import ClientFactory
from taskqueue_toolkit.testing import FakeSnsBroker


def _short_hash(name: str) -> str:
    return hashlib.sha1(name.encode()).hexdigest()[:12]


@pytest.fixture
def dsn(request: pytest.FixtureRequest) -> SnsDsn:
    topic_name = f"taskqueue-toolkit-test-{_short_hash(request.node.name)}-topic"
    return SnsDsn(
        region="eu-west-3",
        topic_name=topic_name,
        endpoint_url="http://fake",
        access_key_id="test",
        secret_access_key="test",
        use_iam_role=False,
    )


@pytest.fixture
def client_factory() -> ClientFactory:
    # FakeSnsBroker only implements the slice of the SNS/SQS client
    # interfaces SnsTaskQueue actually calls — cast rather than replicate
    # the full SDK surface just to satisfy strict structural typing here.
    return cast(ClientFactory, FakeSnsBroker().client)
