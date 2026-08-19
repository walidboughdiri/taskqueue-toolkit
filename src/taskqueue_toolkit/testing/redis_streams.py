"""An in-memory double of the slice of redis.asyncio that
RedisStreamsTaskQueue calls, for testing code that uses this package
without a real Redis — with real consumer-group semantics (a pending
entries list per group, "0" re-reads a consumer's own unacked entries
before ">" reads new ones), since that ordering is what nack(requeue=True)
relies on.

Usage:
    broker = FakeRedisStreamsBroker()
    queue = RedisStreamsTaskQueue(
        dsn=dsn, encode=encode, decode=decode, client_factory=broker.client
    )

Each FakeRedisStreamsBroker instance owns its own isolated state — create a
new one per test rather than sharing one, and there is nothing to reset
between tests.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from redis.exceptions import ResponseError

__all__ = ["FakeRedisStreamsBroker"]


@dataclass(slots=True)
class _Group:
    # Entries delivered to a consumer but not yet XACKed, in delivery order
    # (a real broker's Pending Entries List) — nack(requeue=True) leaves an
    # entry here; xreadgroup's "0" branch redelivers from here first.
    pending_entries: list[tuple[bytes, dict[bytes, bytes]]] = field(default_factory=list)


@dataclass(slots=True)
class _Stream:
    entries: list[tuple[bytes, dict[bytes, bytes]]] = field(default_factory=list)
    next_id: int = 1
    groups: dict[bytes, _Group] = field(default_factory=dict)
    # Position in `entries` each group has already delivered up to, for the
    # ">" (new entries) branch of xreadgroup.
    cursor: dict[bytes, int] = field(default_factory=dict)


@dataclass(slots=True)
class _FakeRedisClient:
    _broker: FakeRedisStreamsBroker

    async def ping(self) -> bool:
        return True

    async def xgroup_create(
        self, name: str, group: str, *, id: str = "0", mkstream: bool = False
    ) -> None:
        stream = self._broker._stream_for(name.encode())
        group_key = group.encode()
        if group_key in stream.groups:
            raise ResponseError("BUSYGROUP Consumer Group name already exists")
        stream.groups[group_key] = _Group()
        stream.cursor[group_key] = 0

    async def xadd(self, name: str, fields: dict[str, bytes]) -> bytes:
        stream = self._broker._stream_for(name.encode())
        entry_id = f"{stream.next_id}-0".encode()
        stream.next_id += 1
        encoded_fields = {key.encode(): value for key, value in fields.items()}
        stream.entries.append((entry_id, encoded_fields))
        return entry_id

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]:
        ((name, read_id),) = streams.items()
        stream = self._broker._stream_for(name.encode())
        group_key = group.encode()
        grp = stream.groups[group_key]

        if read_id == "0":
            # Re-deliver this consumer's own still-pending entries — this
            # fake doesn't track per-consumer PELs separately, only
            # per-group, matching this package's single-consumer-per-group
            # usage pattern.
            delivered = grp.pending_entries[: count or len(grp.pending_entries)]
            return [(name.encode(), delivered)]

        # ">" branch: deliver new entries the group hasn't seen yet.
        cursor = stream.cursor[group_key]
        new_entries = stream.entries[cursor : cursor + (count or 1)]
        if new_entries:
            stream.cursor[group_key] = cursor + len(new_entries)
            grp.pending_entries.extend(new_entries)
            return [(name.encode(), new_entries)]

        # Nothing new — a real broker blocks up to `block` ms; the fake
        # yields control once so callers polling in a loop don't spin the
        # event loop hot, then returns empty like a timed-out BLOCK would.
        await asyncio.sleep(0)
        return [(name.encode(), [])]

    async def xack(self, name: str, group: str, *entry_ids: bytes) -> int:
        stream = self._broker._stream_for(name.encode())
        grp = stream.groups.get(group.encode())
        if grp is None:
            return 0
        ids = set(entry_ids)
        before = len(grp.pending_entries)
        grp.pending_entries = [e for e in grp.pending_entries if e[0] not in ids]
        return before - len(grp.pending_entries)

    async def delete(self, *names: str) -> int:
        count = 0
        for name in names:
            if self._broker._streams.pop(name.encode(), None) is not None:
                count += 1
        return count

    async def aclose(self) -> None:
        return None


@dataclass(slots=True)
class FakeRedisStreamsBroker:
    """An isolated in-memory Redis Streams double. Pass `broker.client` as
    the `client_factory=` argument to RedisStreamsTaskQueue in place of the
    default redis.Redis.from_url-based factory."""

    _streams: dict[bytes, _Stream] = field(default_factory=dict, init=False)

    def _stream_for(self, name: bytes) -> _Stream:
        return self._streams.setdefault(name, _Stream())

    def client(self, dsn: object) -> _FakeRedisClient:
        return _FakeRedisClient(_broker=self)
