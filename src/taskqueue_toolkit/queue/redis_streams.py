from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import cast

import redis.asyncio as redis
from redis.exceptions import ResponseError

from taskqueue_toolkit.queue.dsn import RedisStreamsDsn
from taskqueue_toolkit.queue.task_queue import Decoder, Encoder

logger = logging.getLogger(__name__)

_LONG_POLL_BLOCK_MS = 10_000
_FIELD = "payload"

ClientFactory = Callable[[RedisStreamsDsn], "redis.Redis"]


def _default_client_factory(dsn: RedisStreamsDsn) -> redis.Redis:
    return redis.Redis.from_url(dsn.url)


@dataclass(slots=True)
class RedisStreamsQueuedTask[T]:
    _redis: redis.Redis
    _stream: str
    _group: str
    _message_id: str
    task: T

    async def ack(self) -> None:
        await self._redis.xack(self._stream, self._group, self._message_id)

    async def nack(self, *, requeue: bool) -> None:
        if requeue:
            # Leaving the message unacknowledged (in the group's Pending
            # Entries List) is enough — consume()'s next pass re-reads
            # pending entries (id="0") before new ones, so it's redelivered
            # without needing an explicit "give it back" call the way SQS's
            # change_message_visibility does.
            return
        # No per-message dead-letter action here either — same reasoning as
        # SQS: permanently dropping a task means removing it from the PEL,
        # same as a successful ack.
        await self._redis.xack(self._stream, self._group, self._message_id)


@dataclass(slots=True)
class RedisStreamsTaskQueue[T]:
    dsn: RedisStreamsDsn
    encode: Encoder[T]
    decode: Decoder[T]
    # Overridable for tests (inject a fake client) or a shared/pooled
    # connection the caller already manages; defaults to a fresh connection
    # from the DSN's URL per call, matching the real SDK's usual usage.
    client_factory: ClientFactory = field(default=_default_client_factory)

    def _client(self) -> redis.Redis:
        return self.client_factory(self.dsn)

    async def _ensure_group(self, client: redis.Redis) -> None:
        try:
            await client.xgroup_create(
                self.dsn.stream_name, self.dsn.group, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def publish(self, task: T) -> None:
        client = self._client()
        try:
            await client.xadd(self.dsn.stream_name, {_FIELD: self.encode(task)})
            logger.info("task published", extra={"stream": self.dsn.stream_name})
        finally:
            await client.aclose()

    async def consume(self) -> AsyncIterator[RedisStreamsQueuedTask[T]]:
        client = self._client()
        try:
            await self._ensure_group(client)
            stream = self.dsn.stream_name
            group = self.dsn.group
            consumer = self.dsn.consumer

            while True:
                # Re-deliver our own still-pending entries first (id="0") —
                # covers a crash between XREADGROUP and XACK on a previous
                # run — then fall through to new entries (id=">").
                for read_id in ("0", ">"):
                    # redis-py's XReadGroupResponse type is a broad union
                    # (it also covers the decode_responses=True str shape);
                    # without decode_responses the actual shape is always
                    # this nested list of (stream_name, entries) in bytes.
                    raw_response = await client.xreadgroup(
                        group,
                        consumer,
                        {stream: read_id},
                        count=1,
                        block=_LONG_POLL_BLOCK_MS if read_id == ">" else None,
                    )
                    response = cast(
                        "list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]",
                        raw_response,
                    )
                    for _stream_name, messages in response:
                        for message_id, fields in messages:
                            yield RedisStreamsQueuedTask(
                                _redis=client,
                                _stream=stream,
                                _group=group,
                                _message_id=message_id.decode(),
                                task=self.decode(fields[_FIELD.encode()]),
                            )
        finally:
            await client.aclose()
