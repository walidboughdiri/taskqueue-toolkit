from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, LargeBinary, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# Values for OutboxRow.status. Not a DB enum on purpose — SQLite (handy for
# tests) and Postgres would need separate enum handling, and a plain indexed
# string is enough for the states polled by a relay.
#
# pending -> claimed -> published            (happy path)
# pending -> claimed -> pending               (publish failed, retry)
# pending -> claimed -> failed                (publish failed too many times)
#
# "claimed" exists so a relay can release its SELECT ... FOR UPDATE SKIP
# LOCKED lock immediately after marking rows claimed, rather than holding a
# transaction open across a network call to the broker — the lock's only
# job is to make the claim itself atomic across concurrent relays.
STATUS_PENDING = "pending"
STATUS_CLAIMED = "claimed"
STATUS_PUBLISHED = "published"
STATUS_FAILED = "failed"


class OutboxRow(Base):
    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    # Free-form, caller-assigned identifier (e.g. an aggregate/entity id) —
    # not interpreted by this package, only indexed so a caller can look up
    # "what's pending for this project/order/tenant" without decoding every
    # payload. Optional: a caller with no natural correlation key just
    # leaves it unset.
    correlation_id: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    payload: Mapped[bytes] = mapped_column(LargeBinary)
    status: Mapped[str] = mapped_column(String(20), default=STATUS_PENDING, index=True)
    attempts: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
