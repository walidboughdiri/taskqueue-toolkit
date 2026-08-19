from __future__ import annotations

import pytest

from taskqueue_toolkit.queue.dsn import PubsubDsn
from taskqueue_toolkit.queue.pubsub import PublisherFactory, SubscriberFactory
from taskqueue_toolkit.testing import FakePubsubBroker

_PROJECT_ID = "taskqueue-toolkit-test"


@pytest.fixture
def dsn(request: pytest.FixtureRequest) -> PubsubDsn:
    topic_name = f"taskqueue-toolkit-test-{request.node.name}"
    return PubsubDsn(
        project_id=_PROJECT_ID,
        topic_name=topic_name,
        subscription_name=f"{topic_name}-subscriber",
        emulator_host="",
    )


@pytest.fixture
def _broker() -> FakePubsubBroker:
    return FakePubsubBroker()


@pytest.fixture
def publisher_factory(_broker: FakePubsubBroker) -> PublisherFactory:
    return _broker.publisher


@pytest.fixture
def subscriber_factory(_broker: FakePubsubBroker) -> SubscriberFactory:
    return _broker.subscriber
