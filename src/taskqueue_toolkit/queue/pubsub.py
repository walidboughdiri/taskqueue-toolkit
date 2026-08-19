from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import dataclass, field

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

from taskqueue_toolkit.queue.dsn import PubsubDsn
from taskqueue_toolkit.queue.task_queue import Decoder, Encoder

logger = logging.getLogger(__name__)

_PULL_TIMEOUT_SECONDS = 10.0

PublisherFactory = Callable[[], "pubsub_v1.PublisherClient"]
SubscriberFactory = Callable[[], "pubsub_v1.SubscriberClient"]


@dataclass(slots=True)
class PubsubQueuedTask[T]:
    _subscriber: pubsub_v1.SubscriberClient
    _subscription_path: str
    _ack_id: str
    task: T

    async def ack(self) -> None:
        await asyncio.to_thread(
            self._subscriber.acknowledge,
            subscription=self._subscription_path,
            ack_ids=[self._ack_id],
        )

    async def nack(self, *, requeue: bool) -> None:
        if requeue:
            # No dedicated "nack" RPC — resetting the ack deadline to 0
            # makes the message immediately re-deliverable, same idea as
            # SQS's change_message_visibility(VisibilityTimeout=0).
            await asyncio.to_thread(
                self._subscriber.modify_ack_deadline,
                subscription=self._subscription_path,
                ack_ids=[self._ack_id],
                ack_deadline_seconds=0,
            )
        else:
            await asyncio.to_thread(
                self._subscriber.acknowledge,
                subscription=self._subscription_path,
                ack_ids=[self._ack_id],
            )


@dataclass(slots=True)
class PubsubTaskQueue[T]:
    """Pub/Sub is fan-out only, like SNS — no receive on a topic directly.

    consume() needs a subscription to pull from, so this adapter provisions
    one dedicated subscription per topic (create_topic and create_subscription
    both raise AlreadyExists — not silently no-op like AWS's idempotent
    create_topic/subscribe — so that's caught explicitly rather than relied
    on to be a no-op).

    The official client (google-cloud-pubsub) is synchronous gRPC, not
    asyncio-native — every call here runs through asyncio.to_thread rather
    than reimplementing an async gRPC stack for a single adapter.
    """

    dsn: PubsubDsn
    encode: Encoder[T]
    decode: Decoder[T]
    # Overridable for tests (inject fake clients) or a shared client the
    # caller already manages; each defaults to the real SDK client.
    publisher_factory: PublisherFactory = field(default=pubsub_v1.PublisherClient)
    subscriber_factory: SubscriberFactory = field(default=pubsub_v1.SubscriberClient)
    _publisher_client: pubsub_v1.PublisherClient | None = field(default=None, init=False)
    _subscriber_client: pubsub_v1.SubscriberClient | None = field(default=None, init=False)

    def _apply_emulator_host(self) -> None:
        # The official client only knows to target an emulator via this env
        # var (it swaps in an insecure channel + anonymous credentials as a
        # unit) — there's no supported constructor kwarg that does the same
        # thing as reliably. Setting it here, from the DSN, right before the
        # first client is built keeps the emulator target fully described by
        # the DSN instead of requiring a second, separate env var.
        if self.dsn.emulator_host:
            os.environ["PUBSUB_EMULATOR_HOST"] = self.dsn.emulator_host

    @property
    def _publisher(self) -> pubsub_v1.PublisherClient:
        # Built lazily, not at __init__ time (e.g. via default_factory) —
        # constructing the client eagerly authenticates against GCP
        # immediately, which fails outside an environment with real or
        # emulated credentials even if this instance is never actually used.
        if self._publisher_client is None:
            self._apply_emulator_host()
            self._publisher_client = self.publisher_factory()
        return self._publisher_client

    @property
    def _subscriber(self) -> pubsub_v1.SubscriberClient:
        if self._subscriber_client is None:
            self._apply_emulator_host()
            self._subscriber_client = self.subscriber_factory()
        return self._subscriber_client

    def _topic_path(self) -> str:
        return str(self._publisher.topic_path(self.dsn.project_id, self.dsn.topic_name))

    def _subscription_path(self) -> str:
        return str(
            self._subscriber.subscription_path(self.dsn.project_id, self.dsn.subscription_name)
        )

    async def _ensure_topic(self) -> str:
        topic_path = self._topic_path()
        with suppress(AlreadyExists):
            await asyncio.to_thread(self._publisher.create_topic, name=topic_path)
        return topic_path

    async def _ensure_subscription(self, topic_path: str) -> str:
        subscription_path = self._subscription_path()
        with suppress(AlreadyExists):
            await asyncio.to_thread(
                self._subscriber.create_subscription, name=subscription_path, topic=topic_path
            )
        return subscription_path

    async def publish(self, task: T) -> None:
        topic_path = await self._ensure_topic()
        # Pub/Sub fan-out only delivers to subscriptions that already exist
        # at publish time — a message published before the subscription is
        # provisioned is silently dropped, same trap as SNS. Ensuring it
        # here, not just in consume(), keeps the first publish after a cold
        # deploy from being lost.
        await self._ensure_subscription(topic_path)

        future = self._publisher.publish(topic_path, self.encode(task))
        await asyncio.to_thread(future.result, timeout=10)
        logger.info("task published", extra={"topic": self.dsn.topic_name})

    async def consume(self) -> AsyncIterator[PubsubQueuedTask[T]]:
        topic_path = await self._ensure_topic()
        subscription_path = await self._ensure_subscription(topic_path)

        while True:
            response = await asyncio.to_thread(
                self._subscriber.pull,
                subscription=subscription_path,
                max_messages=1,
                timeout=_PULL_TIMEOUT_SECONDS,
            )
            for received in response.received_messages:
                yield PubsubQueuedTask(
                    _subscriber=self._subscriber,
                    _subscription_path=subscription_path,
                    _ack_id=received.ack_id,
                    task=self.decode(received.message.data),
                )
