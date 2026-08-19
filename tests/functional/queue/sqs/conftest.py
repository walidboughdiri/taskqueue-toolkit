from __future__ import annotations

import re
from typing import cast

import pytest

from taskqueue_toolkit.queue.dsn import SqsDsn
from taskqueue_toolkit.queue.sqs import ClientFactory
from taskqueue_toolkit.testing import FakeSqsBroker


def _sanitize(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "-", name)


@pytest.fixture
def dsn(request: pytest.FixtureRequest) -> SqsDsn:
    queue_name = f"taskqueue-toolkit-test-{_sanitize(request.node.name)}"
    return SqsDsn(
        region="eu-west-3",
        queue_name=queue_name,
        endpoint_url="http://fake",
        access_key_id="test",
        secret_access_key="test",
        use_iam_role=False,
    )


@pytest.fixture
def client_factory() -> ClientFactory:
    # FakeSqsBroker only implements the slice of the SQS client interface
    # SqsTaskQueue actually calls — cast rather than replicate the full SDK
    # surface just to satisfy strict structural typing here.
    return cast(ClientFactory, FakeSqsBroker().client)
