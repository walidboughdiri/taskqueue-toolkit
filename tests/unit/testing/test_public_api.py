from __future__ import annotations

import pytest

import taskqueue_toolkit.testing as testing_pkg


def test_exports_all_five_fake_brokers() -> None:
    assert set(testing_pkg.__all__) == {
        "FakePubsubBroker",
        "FakeRabbitMqBroker",
        "FakeRedisStreamsBroker",
        "FakeSnsBroker",
        "FakeSqsBroker",
    }


@pytest.mark.parametrize("name", testing_pkg.__all__)
def test_each_export_is_reachable(name: str) -> None:
    assert hasattr(testing_pkg, name)
    assert callable(getattr(testing_pkg, name))


def test_unknown_attribute_raises_attribute_error() -> None:
    with pytest.raises(AttributeError):
        _ = testing_pkg.NotARealBroker


def test_two_broker_instances_are_isolated_from_each_other() -> None:
    from taskqueue_toolkit.testing import FakeRabbitMqBroker

    first = FakeRabbitMqBroker()
    second = FakeRabbitMqBroker()

    assert first._queues is not second._queues
