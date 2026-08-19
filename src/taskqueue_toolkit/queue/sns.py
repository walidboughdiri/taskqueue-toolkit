from __future__ import annotations

import base64
import json
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field

import aioboto3
from types_aiobotocore_sns.client import SNSClient
from types_aiobotocore_sqs.client import SQSClient

from taskqueue_toolkit.queue.aws_session import aws_session_kwargs
from taskqueue_toolkit.queue.dsn import SnsDsn
from taskqueue_toolkit.queue.task_queue import Decoder, Encoder

logger = logging.getLogger(__name__)

_LONG_POLL_WAIT_SECONDS = 10
_SUBSCRIBER_QUEUE_SUFFIX = "-subscriber"

ClientFactory = Callable[[SnsDsn], AbstractAsyncContextManager[tuple[SNSClient, SQSClient]]]


@asynccontextmanager
async def _default_client_factory(dsn: SnsDsn) -> AsyncIterator[tuple[SNSClient, SQSClient]]:
    session = aioboto3.Session()
    async with (
        session.client("sns", **aws_session_kwargs(dsn)) as sns,
        session.client("sqs", **aws_session_kwargs(dsn)) as sqs,
    ):
        yield sns, sqs


def _sqs_policy_allowing_sns_publish(queue_arn: str, topic_arn: str) -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "sns.amazonaws.com"},
                    "Action": "sqs:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                }
            ],
        }
    )


def _unwrap_sns_envelope(body: str) -> str:
    """SNS-to-SQS delivery wraps the published message in a JSON envelope
    (Type, MessageId, TopicArn, Message, Timestamp, Signature, ...) — the
    payload we actually published is in the "Message" field."""
    return str(json.loads(body)["Message"])


@dataclass(slots=True)
class SnsQueuedTask[T]:
    _client: SQSClient
    _queue_url: str
    _receipt_handle: str
    task: T

    async def ack(self) -> None:
        await self._client.delete_message(
            QueueUrl=self._queue_url, ReceiptHandle=self._receipt_handle
        )

    async def nack(self, *, requeue: bool) -> None:
        if requeue:
            await self._client.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=self._receipt_handle,
                VisibilityTimeout=0,
            )
        else:
            await self._client.delete_message(
                QueueUrl=self._queue_url, ReceiptHandle=self._receipt_handle
            )


@dataclass(slots=True)
class SnsTaskQueue[T]:
    """SNS is fan-out only — there's no receive/consume on a topic itself.

    consume() needs a queue to actually poll, so this adapter provisions one
    dedicated SQS queue subscribed to the topic (both create_topic and
    subscribe are idempotent in the AWS API, so calling this on every
    publish()/consume() is safe — no separate "already set up" state to
    track). publish() only ever talks to SNS; consume() only ever talks to
    the subscriber queue.
    """

    dsn: SnsDsn
    encode: Encoder[T]
    decode: Decoder[T]
    # Overridable for tests (inject fake clients) or a shared/pooled session
    # the caller already manages; defaults to a fresh aioboto3 session per
    # call, matching the real SDK's usual usage.
    client_factory: ClientFactory = field(default=_default_client_factory)

    async def _get_or_create_topic_arn(self, sns: SNSClient) -> str:
        response = await sns.create_topic(Name=self.dsn.topic_name)
        return response["TopicArn"]

    async def _get_or_create_subscriber_queue(
        self, sqs: SQSClient, sns: SNSClient, topic_arn: str
    ) -> str:
        queue_name = self.dsn.topic_name + _SUBSCRIBER_QUEUE_SUFFIX
        try:
            queue_url = (await sqs.get_queue_url(QueueName=queue_name))["QueueUrl"]
        except sqs.exceptions.QueueDoesNotExist:
            queue_url = (await sqs.create_queue(QueueName=queue_name))["QueueUrl"]

        queue_arn = (
            await sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        )["Attributes"]["QueueArn"]

        await sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={"Policy": _sqs_policy_allowing_sns_publish(queue_arn, topic_arn)},
        )
        await sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        return queue_url

    async def publish(self, task: T) -> None:
        async with self.client_factory(self.dsn) as (sns, sqs):
            topic_arn = await self._get_or_create_topic_arn(sns)
            # SNS fan-out only delivers to subscribers that already exist at
            # publish time — a message published before the subscriber queue
            # is provisioned is silently dropped, unlike RabbitMQ/SQS/Redis
            # Streams where publishing ahead of any consumer still keeps the
            # message. Ensuring the subscription here, not just in consume(),
            # is what makes the first publish after a cold deploy not lose
            # its message.
            await self._get_or_create_subscriber_queue(sqs, sns, topic_arn)
            message = base64.b64encode(self.encode(task)).decode()
            await sns.publish(TopicArn=topic_arn, Message=message)
            logger.info("task published", extra={"topic": self.dsn.topic_name})

    async def consume(self) -> AsyncIterator[SnsQueuedTask[T]]:
        async with self.client_factory(self.dsn) as (sns, sqs):
            topic_arn = await self._get_or_create_topic_arn(sns)
            queue_url = await self._get_or_create_subscriber_queue(sqs, sns, topic_arn)

            while True:
                response = await sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=_LONG_POLL_WAIT_SECONDS,
                )
                for message in response.get("Messages", []):
                    envelope = _unwrap_sns_envelope(message["Body"])
                    yield SnsQueuedTask(
                        _client=sqs,
                        _queue_url=queue_url,
                        _receipt_handle=message["ReceiptHandle"],
                        task=self.decode(base64.b64decode(envelope)),
                    )
