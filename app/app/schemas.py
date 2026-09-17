"""Pydantic request/response schemas for the HTTP API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ---------- events ----------

class EventCreate(BaseModel):
    source: str = Field(min_length=1, max_length=128)
    event_id: str = Field(min_length=1, max_length=256)
    type: str = Field(default="event", max_length=128)
    payload: dict = Field(default_factory=dict)


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    source: str
    event_id: str
    type: str
    payload: dict
    created_at: datetime
    # True when this (source, event_id) was already ingested before.
    duplicate: bool = False
    # Number of deliveries created for this submission (0 for duplicates).
    deliveries_created: int = 0


# ---------- subscriptions ----------

class SubscriptionCreate(BaseModel):
    source: str = Field(min_length=1, max_length=128)
    target_url: str = Field(min_length=1)
    # Optional; a random secret is generated when omitted.
    secret: str | None = Field(default=None, min_length=8, max_length=256)
    # Optional allow-list of event types; null/empty = all types of the source.
    event_types: list[str] | None = None

    @field_validator("target_url")
    @classmethod
    def _check_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("target_url must start with http:// or https://")
        return v


class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    source: str
    target_url: str
    event_types: list[str] | None
    is_active: bool
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    created_at: datetime
    secret_hint: str = ""


class SubscriptionCreatedOut(SubscriptionOut):
    # The full secret is returned only once, at creation time.
    secret: str


# ---------- deliveries ----------

DeliveryStatus = Literal["pending", "success", "dead"]


class DeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    chain_id: UUID
    subscription_id: UUID
    event_id: UUID
    seq: int
    status: DeliveryStatus
    attempts: int
    next_attempt_at: datetime
    last_status_code: int | None
    last_error: str | None
    replayed_from: UUID | None
    created_at: datetime
    updated_at: datetime


class AttemptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    delivery_id: UUID
    attempt_number: int
    status_code: int | None
    error: str | None
    duration_ms: float
    created_at: datetime


class DeliveryDetail(DeliveryOut):
    # Full attempt history of this delivery.
    history: list[AttemptOut] = []
    # Ids of deliveries created by replaying this one.
    replays: list[UUID] = []
