"""In-memory doubles of each broker adapter, for testing code that uses
this package without a real RabbitMQ/SQS/SNS/Redis/Pub-Sub.

Each Fake*Broker is a real, working implementation of the relevant slice of
its SDK — not a call-recording mock — so it exercises the same code paths
(FIFO ordering, ack/nack, consumer groups, fan-out, ...) a real broker
would. Create one instance per test; there is no global state to reset.

Importing a given Fake*Broker requires the same optional extra as the
adapter it doubles (e.g. FakeRabbitMqBroker needs no extra since it has no
SDK dependency of its own, but FakeRedisStreamsBroker needs
`pip install taskqueue-toolkit[redis-streams]`, FakeSqsBroker/FakeSnsBroker
need `[aws]`, FakePubsubBroker needs `[pubsub]`) — this module only imports
the specific fake actually accessed, via module-level __getattr__, so
`import taskqueue_toolkit.testing` itself never requires every extra to be
installed at once.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from taskqueue_toolkit.testing.pubsub import FakePubsubBroker
    from taskqueue_toolkit.testing.rabbitmq import FakeRabbitMqBroker
    from taskqueue_toolkit.testing.redis_streams import FakeRedisStreamsBroker
    from taskqueue_toolkit.testing.sns import FakeSnsBroker
    from taskqueue_toolkit.testing.sqs import FakeSqsBroker

__all__ = [
    "FakePubsubBroker",
    "FakeRabbitMqBroker",
    "FakeRedisStreamsBroker",
    "FakeSnsBroker",
    "FakeSqsBroker",
]

_EXPORTS = {
    "FakePubsubBroker": ("taskqueue_toolkit.testing.pubsub", "FakePubsubBroker"),
    "FakeRabbitMqBroker": ("taskqueue_toolkit.testing.rabbitmq", "FakeRabbitMqBroker"),
    "FakeRedisStreamsBroker": (
        "taskqueue_toolkit.testing.redis_streams",
        "FakeRedisStreamsBroker",
    ),
    "FakeSnsBroker": ("taskqueue_toolkit.testing.sns", "FakeSnsBroker"),
    "FakeSqsBroker": ("taskqueue_toolkit.testing.sqs", "FakeSqsBroker"),
}


def __getattr__(name: str) -> object:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    import importlib

    module = importlib.import_module(module_name)
    return getattr(module, attr_name)
