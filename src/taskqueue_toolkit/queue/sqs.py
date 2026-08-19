from __future__ import annotations

import base64
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import aioboto3

if TYPE_CHECKING:
    from types_aiobotocore_sqs.client import SQSClient

from taskqueue_toolkit.queue.aws_session import aws_session_kwargs
from taskqueue_toolkit.queue.dsn import SqsDsn
from taskqueue_toolkit.queue.task_queue import Decoder, Encoder

logger = logging.getLogger(__name__)

_LONG_POLL_WAIT_SECONDS = 10

ClientFactory = Callable[[SqsDsn], AbstractAsyncContextManager["SQSClient"]]


def _default_client_factory(dsn: SqsDsn) -> AbstractAsyncContextManager[SQSClient]:
    return cast(
        "AbstractAsyncContextManager[SQSClient]",
        aioboto3.Session().client("sqs", **aws_session_kwargs(dsn)),
    )


@dataclass(slots=True)
class SqsQueuedTask[T]:
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
            # Make the message immediately visible again instead of waiting
            # out its VisibilityTimeout — SQS has no direct "nack" RPC, this
            # is the idiomatic way to force redelivery on demand.
            await self._client.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=self._receipt_handle,
                VisibilityTimeout=0,
            )
        else:
            # SQS has no per-call dead-letter action; permanently dropping a
            # task means removing it the same way a successful ack does. A
            # queue-level DLQ (RedrivePolicy) is a deployment concern, not
            # something this adapter configures per message.
            await self._client.delete_message(
                QueueUrl=self._queue_url, ReceiptHandle=self._receipt_handle
            )


@dataclass(slots=True)
class SqsTaskQueue[T]:
    dsn: SqsDsn
    encode: Encoder[T]
    decode: Decoder[T]
    # Overridable for tests (inject a fake client) or a shared/pooled
    # session the caller already manages; defaults to a fresh aioboto3
    # session per call, matching the real SDK's usual usage.
    client_factory: ClientFactory = field(default=_default_client_factory)

    async def _get_queue_url(self, client: SQSClient) -> str:
        try:
            response = await client.get_queue_url(QueueName=self.dsn.queue_name)
        except client.exceptions.QueueDoesNotExist:
            response = await client.create_queue(QueueName=self.dsn.queue_name)
        return response["QueueUrl"]

    async def publish(self, task: T) -> None:
        async with self.client_factory(self.dsn) as client:
            queue_url = await self._get_queue_url(client)
            body = base64.b64encode(self.encode(task)).decode()
            await client.send_message(QueueUrl=queue_url, MessageBody=body)
            logger.info("task published", extra={"queue": self.dsn.queue_name})

    async def consume(self) -> AsyncIterator[SqsQueuedTask[T]]:
        async with self.client_factory(self.dsn) as client:
            queue_url = await self._get_queue_url(client)

            while True:
                response = await client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=_LONG_POLL_WAIT_SECONDS,
                )
                for message in response.get("Messages", []):
                    yield SqsQueuedTask(
                        _client=client,
                        _queue_url=queue_url,
                        _receipt_handle=message["ReceiptHandle"],
                        task=self.decode(base64.b64decode(message["Body"])),
                    )
