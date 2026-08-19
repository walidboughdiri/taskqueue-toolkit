from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

from taskqueue_toolkit.queue.task_queue import Decoder, Encoder, TaskQueue

# A handler takes the full DSN string plus this call's encode/decode pair
# and returns a ready-to-use TaskQueue — parsing and construction happen
# together so registering a broker is one call, not a parser plus a
# separate constructor to keep in sync.
TaskQueueHandler = Callable[[str, Encoder[Any], Decoder[Any]], TaskQueue[Any]]

_registry: dict[str, TaskQueueHandler] = {}


class SchemeAlreadyRegisteredError(Exception):
    def __init__(self, scheme: str) -> None:
        super().__init__(
            f"A handler is already registered for scheme {scheme!r}. "
            "Each scheme can only map to one broker handler."
        )


def register_scheme(scheme: str, handler: TaskQueueHandler) -> None:
    """Make create_task_queue() recognize a DSN scheme this package doesn't
    ship a built-in adapter for (e.g. "kafka://...").

    This is the extension point for brokers outside the five this package
    integrates directly (amqp/redis/sqs/sns/pubsub, handled internally and
    checked for exhaustiveness by mypy) — a consumer that needs a broker
    this package doesn't know about registers it once, at import/startup
    time, instead of forking the package.
    """
    if scheme in _registry:
        raise SchemeAlreadyRegisteredError(scheme)
    _registry[scheme] = handler


def unregister_scheme(scheme: str) -> None:
    """Mainly useful for tests that register a fake handler and want to
    clean up afterwards."""
    _registry.pop(scheme, None)


def resolve_registered_scheme(dsn: str) -> TaskQueueHandler | None:
    """Look up a handler registered via register_scheme() for this DSN's
    scheme, or None if nothing is registered for it (including every scheme
    this package already handles internally — those never reach the
    registry)."""
    scheme = urlsplit(dsn).scheme
    return _registry.get(scheme)
