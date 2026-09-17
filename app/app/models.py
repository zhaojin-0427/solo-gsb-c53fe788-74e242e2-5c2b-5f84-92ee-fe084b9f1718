"""SQLAlchemy models.

Table overview:
  events             - ingested events, idempotent on (source, event_id)
  subscriptions      - callback subscriptions for a source stream; also hold the worker lease
  deliveries         - one row per (subscription, event) unit of work; replays create new rows
  delivery_attempts  - immutable per-attempt history for each delivery
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Event(Base):
    __tablename__ = "events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    source: Mapped[str] = mapped_column(String(128), index=True)
    event_id: Mapped[str] = mapped_column(String(256))
    type: Mapped[str] = mapped_column(String(128), default="event")
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (
        UniqueConstraint("source", "event_id", name="uq_events_source_event_id"),
    )


class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    source: Mapped[str] = mapped_column(String(128), index=True)
    # null / empty list means "all event types of this source"
    event_types: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    target_url: Mapped[str] = mapped_column(Text)
    secret: Mapped[str] = mapped_column(String(256))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    # Database lease: only the worker named by lease_owner may process this
    # subscription's queue, and only until lease_expires_at.
    lease_owner: Mapped[str | None] = mapped_column(String(256), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Delivery(Base):
    __tablename__ = "deliveries"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    # Identifies a delivery chain. The original delivery starts a chain; a
    # replay starts a NEW chain (old rows are preserved untouched).
    chain_id: Mapped[uuid.UUID] = mapped_column(Uuid, index=True)
    subscription_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("subscriptions.id"), index=True
    )
    event_id: Mapped[uuid.UUID] = mapped_column(Uuid, ForeignKey("events.id"), index=True)

    # Global monotonic sequence; per-subscription ordering is "ORDER BY seq".
    seq: Mapped[int] = mapped_column(BigInteger, Identity(), nullable=False)

    # pending -> success | dead
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Set on deliveries created by replaying a dead letter.
    replayed_from: Mapped[uuid.UUID | None] = mapped_column(
        Uuid, ForeignKey("deliveries.id"), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    __table_args__ = (
        Index("ix_deliveries_sub_status_seq", "subscription_id", "status", "seq"),
        Index("ix_deliveries_status_next_attempt", "status", "next_attempt_at"),
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    delivery_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("deliveries.id"), index=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[float] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
