"""HTTP API: event ingestion, subscriptions, delivery history, dead-letter replay."""

from __future__ import annotations

import logging
import secrets
import uuid
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Response
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from . import __version__, db
from .config import settings
from .models import Delivery, DeliveryAttempt, Event, Subscription, utcnow
from .schemas import (
    AttemptOut,
    DeliveryDetail,
    DeliveryOut,
    EventCreate,
    EventOut,
    SubscriptionCreate,
    SubscriptionCreatedOut,
    SubscriptionOut,
)

log = logging.getLogger("webhook.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if db.SessionFactory is None:
        db.configure(settings.database_url)
    await db.create_schema()
    yield


app = FastAPI(title="Webhook Delivery Service", version=__version__, lifespan=lifespan)


# ------------------------------------------------------------------- helpers

def _event_out(event: Event, duplicate: bool, deliveries_created: int) -> EventOut:
    return EventOut(
        id=event.id,
        source=event.source,
        event_id=event.event_id,
        type=event.type,
        payload=event.payload,
        created_at=event.created_at,
        duplicate=duplicate,
        deliveries_created=deliveries_created,
    )


def _subscription_out(sub: Subscription) -> SubscriptionOut:
    return SubscriptionOut(
        id=sub.id,
        source=sub.source,
        target_url=sub.target_url,
        event_types=sub.event_types,
        is_active=sub.is_active,
        lease_owner=sub.lease_owner,
        lease_expires_at=sub.lease_expires_at,
        created_at=sub.created_at,
        secret_hint=f"****{sub.secret[-4:]}",
    )


# --------------------------------------------------------------------- meta

@app.get("/")
async def root():
    return {
        "service": "webhook-delivery",
        "version": __version__,
        "docs": "/docs",
        "endpoints": ["/events", "/subscriptions", "/deliveries", "/dead-letters", "/healthz"],
    }


@app.get("/healthz")
async def healthz():
    async with db.session() as s:
        await s.execute(text("SELECT 1"))
    return {"status": "ok"}


# ------------------------------------------------------------------- events

@app.post("/events", response_model=EventOut, status_code=201)
async def create_event(body: EventCreate, response: Response):
    """Ingest an event. Idempotent on (source, event_id): a duplicate
    submission returns HTTP 200 with the original event and creates no
    new deliveries."""
    async with db.session() as s:
        stmt = (
            pg_insert(Event)
            .values(source=body.source, event_id=body.event_id, type=body.type, payload=body.payload)
            .on_conflict_do_nothing(index_elements=["source", "event_id"])
            .returning(Event.id)
        )
        new_id = (await s.execute(stmt)).scalar_one_or_none()

        if new_id is None:
            event = await s.scalar(
                select(Event).where(Event.source == body.source, Event.event_id == body.event_id)
            )
            await s.commit()
            response.status_code = 200
            return _event_out(event, duplicate=True, deliveries_created=0)

        event = await s.get(Event, new_id)
        subscriptions = (
            await s.scalars(
                select(Subscription).where(
                    Subscription.source == event.source,
                    Subscription.is_active.is_(True),
                )
            )
        ).all()
        now = utcnow()
        created = 0
        for sub in subscriptions:
            if sub.event_types and event.type not in sub.event_types:
                continue
            s.add(
                Delivery(
                    chain_id=uuid.uuid4(),
                    subscription_id=sub.id,
                    event_id=event.id,
                    status="pending",
                    attempts=0,
                    next_attempt_at=now,
                )
            )
            created += 1
        await s.commit()
        return _event_out(event, duplicate=False, deliveries_created=created)


@app.get("/events", response_model=list[EventOut])
async def list_events(
    source: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    async with db.session() as s:
        stmt = select(Event).order_by(Event.created_at.desc()).limit(limit).offset(offset)
        if source:
            stmt = stmt.where(Event.source == source)
        events = (await s.scalars(stmt)).all()
        return [_event_out(e, duplicate=False, deliveries_created=0) for e in events]


# ------------------------------------------------------------- subscriptions

@app.post("/subscriptions", response_model=SubscriptionCreatedOut, status_code=201)
async def create_subscription(body: SubscriptionCreate):
    async with db.session() as s:
        sub = Subscription(
            source=body.source,
            target_url=body.target_url,
            secret=body.secret or secrets.token_hex(32),
            event_types=body.event_types or None,
        )
        s.add(sub)
        await s.commit()
        out = SubscriptionCreatedOut(**_subscription_out(sub).model_dump(), secret=sub.secret)
        return out


@app.get("/subscriptions", response_model=list[SubscriptionOut])
async def list_subscriptions(source: str | None = None):
    async with db.session() as s:
        stmt = select(Subscription).order_by(Subscription.created_at)
        if source:
            stmt = stmt.where(Subscription.source == source)
        subs = (await s.scalars(stmt)).all()
        return [_subscription_out(sub) for sub in subs]


@app.get("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
async def get_subscription(subscription_id: UUID):
    async with db.session() as s:
        sub = await s.get(Subscription, subscription_id)
        if sub is None:
            raise HTTPException(404, "subscription not found")
        return _subscription_out(sub)


@app.delete("/subscriptions/{subscription_id}")
async def delete_subscription(subscription_id: UUID):
    """Soft-delete: stops new deliveries, keeps history. Any held lease is
    cleared so workers stop processing its queue."""
    async with db.session() as s:
        sub = await s.get(Subscription, subscription_id)
        if sub is None:
            raise HTTPException(404, "subscription not found")
        sub.is_active = False
        sub.lease_owner = None
        sub.lease_expires_at = None
        await s.commit()
        return {"deleted": True, "id": str(subscription_id)}


# --------------------------------------------------------------- deliveries

_DELIVERY_COLUMNS_ORDER = Delivery.seq.desc()


async def _query_deliveries(
    status: str | None,
    subscription_id: UUID | None,
    event_id: UUID | None,
    chain_id: UUID | None,
    limit: int,
    offset: int,
) -> list[Delivery]:
    async with db.session() as s:
        stmt = select(Delivery).order_by(_DELIVERY_COLUMNS_ORDER).limit(limit).offset(offset)
        if status:
            stmt = stmt.where(Delivery.status == status)
        if subscription_id:
            stmt = stmt.where(Delivery.subscription_id == subscription_id)
        if event_id:
            stmt = stmt.where(Delivery.event_id == event_id)
        if chain_id:
            stmt = stmt.where(Delivery.chain_id == chain_id)
        return list((await s.scalars(stmt)).all())


@app.get("/deliveries", response_model=list[DeliveryOut])
async def list_deliveries(
    subscription_id: UUID | None = None,
    event_id: UUID | None = None,
    chain_id: UUID | None = None,
    status: str | None = Query(None, pattern="^(pending|success|dead)$"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Delivery history, most recent first."""
    return await _query_deliveries(status, subscription_id, event_id, chain_id, limit, offset)


@app.get("/deliveries/{delivery_id}", response_model=DeliveryDetail)
async def get_delivery(delivery_id: UUID):
    async with db.session() as s:
        delivery = await s.get(Delivery, delivery_id)
        if delivery is None:
            raise HTTPException(404, "delivery not found")
        attempts = (
            await s.scalars(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.delivery_id == delivery_id)
                .order_by(DeliveryAttempt.attempt_number)
            )
        ).all()
        replays = (
            await s.scalars(
                select(Delivery.id).where(Delivery.replayed_from == delivery_id)
            )
        ).all()
        detail = DeliveryDetail.model_validate(delivery)
        detail.history = [AttemptOut.model_validate(a) for a in attempts]
        detail.replays = list(replays)
        return detail


# -------------------------------------------------------------- dead letters

@app.get("/dead-letters", response_model=list[DeliveryOut])
async def list_dead_letters(
    subscription_id: UUID | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return await _query_deliveries("dead", subscription_id, None, None, limit, offset)


@app.post("/dead-letters/{delivery_id}/replay", response_model=DeliveryOut, status_code=201)
async def replay_dead_letter(delivery_id: UUID):
    """Replay a dead-lettered delivery.

    Creates a NEW delivery (new chain, fresh attempt counter) appended to the
    tail of the subscription queue. The original dead-letter record is left
    untouched so the full history remains auditable.
    """
    async with db.session() as s:
        original = await s.get(Delivery, delivery_id, with_for_update=True)
        if original is None:
            raise HTTPException(404, "delivery not found")
        if original.status != "dead":
            raise HTTPException(409, f"only dead deliveries can be replayed (status={original.status})")
        replay = Delivery(
            chain_id=uuid.uuid4(),
            subscription_id=original.subscription_id,
            event_id=original.event_id,
            status="pending",
            attempts=0,
            next_attempt_at=utcnow(),
            replayed_from=original.id,
        )
        s.add(replay)
        await s.flush()
        await s.refresh(replay)
        await s.commit()
        return DeliveryOut.model_validate(replay)
