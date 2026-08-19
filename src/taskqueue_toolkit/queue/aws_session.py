from __future__ import annotations

from taskqueue_toolkit.queue.dsn import SnsDsn, SqsDsn


def aws_session_kwargs(dsn: SqsDsn | SnsDsn) -> dict[str, str]:
    """boto3-style client() kwargs shared by every AWS-backed TaskQueue
    implementation (SQS, SNS). Only includes keys that were actually set, so
    real AWS auth (IAM role, env vars picked up by botocore itself) still
    works when the DSN leaves them out — only LocalStack testing needs them.
    """
    kwargs = {"region_name": dsn.region}
    if dsn.endpoint_url:
        kwargs["endpoint_url"] = dsn.endpoint_url
    if dsn.access_key_id:
        kwargs["aws_access_key_id"] = dsn.access_key_id
    if dsn.secret_access_key:
        kwargs["aws_secret_access_key"] = dsn.secret_access_key
    return kwargs
