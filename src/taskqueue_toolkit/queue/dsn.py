from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit


class UnsupportedDsnSchemeError(Exception):
    def __init__(self, scheme: str) -> None:
        super().__init__(
            f"Unsupported task queue DSN scheme: {scheme!r} "
            "(expected one of amqp, redis, sqs, sns, pubsub)"
        )


def _query(raw_query: str) -> dict[str, str]:
    return {key: values[0] for key, values in parse_qs(raw_query).items()}


@dataclass(frozen=True, slots=True)
class RabbitMqDsn:
    url: str
    queue_name: str


@dataclass(frozen=True, slots=True)
class RedisStreamsDsn:
    url: str
    stream_name: str
    group: str
    consumer: str


@dataclass(frozen=True, slots=True)
class SqsDsn:
    region: str
    queue_name: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    use_iam_role: bool


@dataclass(frozen=True, slots=True)
class SnsDsn:
    region: str
    topic_name: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    use_iam_role: bool


@dataclass(frozen=True, slots=True)
class PubsubDsn:
    project_id: str
    topic_name: str
    subscription_name: str
    emulator_host: str


TaskQueueDsn = RabbitMqDsn | RedisStreamsDsn | SqsDsn | SnsDsn | PubsubDsn

_DEFAULT_QUEUE_NAME = "tasks"
_DEFAULT_STREAM_GROUP = "workers"
_DEFAULT_STREAM_CONSUMER = "worker-1"


def parse_task_queue_dsn(dsn: str) -> TaskQueueDsn:
    """Parse a single connection string into a broker-specific, typed config.

    The scheme picks the broker (and therefore which TaskQueue
    implementation to build) — everything else the connection needs travels
    in the same string: host/credentials in the URL itself, broker-specific
    extras (queue/topic/group names, region, emulator endpoint, ...) as
    query params. One string per deployment instead of a broker-name setting
    plus a parallel block of per-broker settings.

    Examples:
      amqp://guest:guest@host:5672/?queue=my.tasks
      redis://:password@host:6379/0?stream=my.tasks&group=workers&consumer=worker-1
      sqs://eu-west-3/my.tasks?endpoint_url=...&access_key_id=...&secret_access_key=...
      sns://eu-west-3/my-tasks?endpoint_url=...&access_key_id=...&secret_access_key=...
      pubsub://project-id/my-tasks?subscription=my-tasks-subscriber&emulator_host=localhost:8085
    """
    parts = urlsplit(dsn)
    query = _query(parts.query)

    if parts.scheme in ("amqp", "amqps"):
        return RabbitMqDsn(
            url=dsn.split("?", 1)[0],
            queue_name=query.get("queue", _DEFAULT_QUEUE_NAME),
        )

    if parts.scheme in ("redis", "rediss"):
        return RedisStreamsDsn(
            url=dsn.split("?", 1)[0],
            stream_name=query.get("stream", _DEFAULT_QUEUE_NAME),
            group=query.get("group", _DEFAULT_STREAM_GROUP),
            consumer=query.get("consumer", _DEFAULT_STREAM_CONSUMER),
        )

    if parts.scheme == "sqs":
        return SqsDsn(
            region=parts.hostname or "",
            queue_name=parts.path.lstrip("/"),
            endpoint_url=query.get("endpoint_url", ""),
            access_key_id=query.get("access_key_id", ""),
            secret_access_key=query.get("secret_access_key", ""),
            use_iam_role=query.get("iam_role", "").lower() == "true",
        )

    if parts.scheme == "sns":
        return SnsDsn(
            region=parts.hostname or "",
            topic_name=parts.path.lstrip("/"),
            endpoint_url=query.get("endpoint_url", ""),
            access_key_id=query.get("access_key_id", ""),
            secret_access_key=query.get("secret_access_key", ""),
            use_iam_role=query.get("iam_role", "").lower() == "true",
        )

    if parts.scheme == "pubsub":
        topic_name = parts.path.lstrip("/")
        return PubsubDsn(
            project_id=parts.hostname or "",
            topic_name=topic_name,
            subscription_name=query.get("subscription", f"{topic_name}-subscriber"),
            emulator_host=query.get("emulator_host", ""),
        )

    raise UnsupportedDsnSchemeError(parts.scheme)


def missing_production_aws_auth(dsn: TaskQueueDsn) -> bool:
    """True when an sqs/sns DSN has no way to authenticate against real AWS:
    no LocalStack endpoint, no explicit keys, and no iam_role=true opt-in.
    Not applicable (returns False) to every other DSN type. Split out from
    parsing so a caller's own production fail-fast check can call it without
    needing to know anything about AWS-specific DSN fields.
    """
    if not isinstance(dsn, SqsDsn | SnsDsn):
        return False
    return (
        not dsn.endpoint_url
        and not dsn.use_iam_role
        and (not dsn.access_key_id or not dsn.secret_access_key)
    )
