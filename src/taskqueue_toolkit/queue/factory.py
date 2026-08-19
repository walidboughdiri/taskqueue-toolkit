from __future__ import annotations

from typing import assert_never
from urllib.parse import urlsplit

from taskqueue_toolkit.queue.dsn import (
    PubsubDsn,
    RabbitMqDsn,
    RedisStreamsDsn,
    SnsDsn,
    SqsDsn,
    UnsupportedDsnSchemeError,
    missing_production_aws_auth,
    parse_task_queue_dsn,
)
from taskqueue_toolkit.queue.pubsub import PubsubTaskQueue
from taskqueue_toolkit.queue.rabbitmq import RabbitMqTaskQueue
from taskqueue_toolkit.queue.redis_streams import RedisStreamsTaskQueue
from taskqueue_toolkit.queue.registry import resolve_registered_scheme
from taskqueue_toolkit.queue.sns import SnsTaskQueue
from taskqueue_toolkit.queue.sqs import SqsTaskQueue
from taskqueue_toolkit.queue.task_queue import Decoder, Encoder, TaskQueue

_BUILT_IN_SCHEMES = frozenset({"amqp", "amqps", "redis", "rediss", "sqs", "sns", "pubsub"})


class MissingAwsAuthError(Exception):
    def __init__(self, environment: str) -> None:
        super().__init__(
            f"environment={environment!r} but the task queue DSN has no way "
            "to authenticate against AWS: no endpoint_url (LocalStack), no "
            "access_key_id/secret_access_key, and no iam_role=true. Add "
            "credentials, or add '&iam_role=true' if auth is handled by an "
            "IAM role (ECS task role, EKS service account, EC2 instance "
            "profile) — refusing to build the queue with no AWS auth "
            "configured outside development."
        )


def create_task_queue[T](
    dsn: str,
    *,
    encode: Encoder[T],
    decode: Decoder[T],
    environment: str = "development",
) -> TaskQueue[T]:
    """Build the TaskQueue implementation described by a single DSN. The
    scheme picks the adapter — amqp/amqps -> RabbitMQ, redis/rediss -> Redis
    Streams, sqs/sns/pubsub -> themselves — and everything else the
    connection needs (queue/topic names, region, credentials, emulator
    target, ...) travels in the same string. This is the one place in this
    package allowed to know every concrete adapter exists; callers depend on
    TaskQueue[T] only, so switching broker is a DSN change, not a code
    change.

    encode/decode are required because this package has no opinion on how
    your task type T serializes — every adapter needs a bytes-in/bytes-out
    pair to actually move a T through its broker.

    `environment` gates one fail-fast check: outside development, an
    sqs/sns DSN with no LocalStack endpoint, no explicit keys, and no
    iam_role=true opt-in is almost certainly a forgotten credential, not a
    real deployment — refuse to build the adapter rather than let it fail
    confusingly on the first network call.

    A scheme this package doesn't ship a built-in adapter for is looked up
    in the registry (see registry.register_scheme()) before giving up with
    UnsupportedDsnSchemeError — that's how a broker outside this package's
    five (amqp/redis/sqs/sns/pubsub) gets plugged in.
    """
    scheme = urlsplit(dsn).scheme
    if scheme not in _BUILT_IN_SCHEMES:
        handler = resolve_registered_scheme(dsn)
        if handler is not None:
            return handler(dsn, encode, decode)
        raise UnsupportedDsnSchemeError(scheme)

    parsed = parse_task_queue_dsn(dsn)

    if environment != "development" and missing_production_aws_auth(parsed):
        raise MissingAwsAuthError(environment)

    if isinstance(parsed, RabbitMqDsn):
        return RabbitMqTaskQueue(dsn=parsed, encode=encode, decode=decode)
    if isinstance(parsed, RedisStreamsDsn):
        return RedisStreamsTaskQueue(dsn=parsed, encode=encode, decode=decode)
    if isinstance(parsed, SqsDsn):
        return SqsTaskQueue(dsn=parsed, encode=encode, decode=decode)
    if isinstance(parsed, SnsDsn):
        return SnsTaskQueue(dsn=parsed, encode=encode, decode=decode)
    if isinstance(parsed, PubsubDsn):
        return PubsubTaskQueue(dsn=parsed, encode=encode, decode=decode)

    assert_never(parsed)
