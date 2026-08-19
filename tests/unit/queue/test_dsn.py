from __future__ import annotations

import pytest

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


def test_parses_amqp_dsn_with_default_queue_name() -> None:
    parsed = parse_task_queue_dsn("amqp://guest:guest@localhost:5672/")

    assert parsed == RabbitMqDsn(url="amqp://guest:guest@localhost:5672/", queue_name="tasks")


def test_parses_amqp_dsn_with_explicit_queue_name() -> None:
    parsed = parse_task_queue_dsn("amqp://guest:guest@localhost:5672/?queue=my.tasks")

    assert parsed == RabbitMqDsn(url="amqp://guest:guest@localhost:5672/", queue_name="my.tasks")


def test_parses_amqps_scheme() -> None:
    parsed = parse_task_queue_dsn("amqps://user:pass@broker:5671/?queue=secure.tasks")

    assert isinstance(parsed, RabbitMqDsn)
    assert parsed.url == "amqps://user:pass@broker:5671/"


def test_parses_redis_dsn_with_defaults() -> None:
    parsed = parse_task_queue_dsn("redis://localhost:6379/0")

    assert parsed == RedisStreamsDsn(
        url="redis://localhost:6379/0",
        stream_name="tasks",
        group="workers",
        consumer="worker-1",
    )


def test_parses_redis_dsn_with_explicit_params() -> None:
    parsed = parse_task_queue_dsn(
        "redis://:password@host:6379/0?stream=my.tasks&group=g1&consumer=c1"
    )

    assert parsed == RedisStreamsDsn(
        url="redis://:password@host:6379/0",
        stream_name="my.tasks",
        group="g1",
        consumer="c1",
    )


def test_parses_rediss_scheme() -> None:
    parsed = parse_task_queue_dsn("rediss://host:6380/0?stream=s")

    assert isinstance(parsed, RedisStreamsDsn)
    assert parsed.url == "rediss://host:6380/0"


def test_parses_sqs_dsn_with_localstack_endpoint() -> None:
    parsed = parse_task_queue_dsn(
        "sqs://eu-west-3/my.tasks?endpoint_url=http://localhost:4566"
        "&access_key_id=test&secret_access_key=test"
    )

    assert parsed == SqsDsn(
        region="eu-west-3",
        queue_name="my.tasks",
        endpoint_url="http://localhost:4566",
        access_key_id="test",
        secret_access_key="test",
        use_iam_role=False,
    )


def test_parses_sqs_dsn_with_iam_role_opt_in() -> None:
    parsed = parse_task_queue_dsn("sqs://eu-west-3/my.tasks?iam_role=true")

    assert isinstance(parsed, SqsDsn)
    assert parsed.use_iam_role is True
    assert parsed.endpoint_url == ""


def test_parses_sns_dsn() -> None:
    parsed = parse_task_queue_dsn(
        "sns://eu-west-3/my-tasks?endpoint_url=http://localhost:4566"
        "&access_key_id=test&secret_access_key=test"
    )

    assert parsed == SnsDsn(
        region="eu-west-3",
        topic_name="my-tasks",
        endpoint_url="http://localhost:4566",
        access_key_id="test",
        secret_access_key="test",
        use_iam_role=False,
    )


def test_parses_pubsub_dsn_with_default_subscription_name() -> None:
    parsed = parse_task_queue_dsn("pubsub://my-project/my-tasks?emulator_host=localhost:8085")

    assert parsed == PubsubDsn(
        project_id="my-project",
        topic_name="my-tasks",
        subscription_name="my-tasks-subscriber",
        emulator_host="localhost:8085",
    )


def test_parses_pubsub_dsn_with_explicit_subscription_name() -> None:
    parsed = parse_task_queue_dsn("pubsub://my-project/my-tasks?subscription=custom-sub")

    assert isinstance(parsed, PubsubDsn)
    assert parsed.subscription_name == "custom-sub"


def test_raises_on_unsupported_scheme() -> None:
    with pytest.raises(UnsupportedDsnSchemeError):
        parse_task_queue_dsn("kafka://localhost:9092/my.tasks")


@pytest.mark.parametrize(
    "dsn",
    [
        SqsDsn(
            region="eu-west-3",
            queue_name="q",
            endpoint_url="",
            access_key_id="",
            secret_access_key="",
            use_iam_role=False,
        ),
        SnsDsn(
            region="eu-west-3",
            topic_name="t",
            endpoint_url="",
            access_key_id="",
            secret_access_key="",
            use_iam_role=False,
        ),
    ],
)
def test_missing_production_aws_auth_true_when_no_credentials(
    dsn: SqsDsn | SnsDsn,
) -> None:
    assert missing_production_aws_auth(dsn) is True


def test_missing_production_aws_auth_false_with_localstack_endpoint() -> None:
    dsn = SqsDsn(
        region="eu-west-3",
        queue_name="q",
        endpoint_url="http://localhost:4566",
        access_key_id="",
        secret_access_key="",
        use_iam_role=False,
    )

    assert missing_production_aws_auth(dsn) is False


def test_missing_production_aws_auth_false_with_iam_role() -> None:
    dsn = SnsDsn(
        region="eu-west-3",
        topic_name="t",
        endpoint_url="",
        access_key_id="",
        secret_access_key="",
        use_iam_role=True,
    )

    assert missing_production_aws_auth(dsn) is False


def test_missing_production_aws_auth_false_with_explicit_credentials() -> None:
    dsn = SqsDsn(
        region="eu-west-3",
        queue_name="q",
        endpoint_url="",
        access_key_id="AKIA...",
        secret_access_key="secret",
        use_iam_role=False,
    )

    assert missing_production_aws_auth(dsn) is False


def test_missing_production_aws_auth_not_applicable_to_non_aws_dsn() -> None:
    dsn = RabbitMqDsn(url="amqp://localhost/", queue_name="q")

    assert missing_production_aws_auth(dsn) is False
