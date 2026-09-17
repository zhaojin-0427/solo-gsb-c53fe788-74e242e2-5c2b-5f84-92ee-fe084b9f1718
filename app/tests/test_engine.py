"""Engine tests: ordering, retries, dead letters, leases, replay.

These run against a real PostgreSQL and use an in-process mock receiver
(httpx.MockTransport) that verifies the HMAC signature of every request.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import timedelta

import httpx
from sqlalchemy import func, select, update

from app import db
from app.config import Settings
from app.engine import DeliveryEngine
from app.models import Delivery, DeliveryAttempt, Event, Subscription, utcnow
from app.security import verify_signature_header

from .conftest import wait_for

SECRET = "engine-test-secret"


def make_settings(**overrides) -> Settings:
    params = {
        "database_url": os.environ["DATABASE_URL"],
        "max_attempts": 6,
        "backoff_base_seconds": 0.05,
        "backoff_jitter_ratio": 0.0,
        "lease_ttl_seconds": 30.0,
        "poll_interval_seconds": 0.02,
        "http_timeout_seconds": 5.0,
        "worker_id": "worker-1",
    }
    params.update(overrides)
    return Settings(**params)


class MockReceiver:
    """In-process webhook receiver; behavior(delivery_id, attempt_no) -> HTTP status."""

    def __init__(self, behavior=None):
        self.behavior = behavior or (lambda delivery_id, attempt_no: 200)
        self.requests: list[dict] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = request.content
        signature = request.headers.get("X-Webhook-Signature")
        timestamp = request.headers.get("X-Webhook-Timestamp")
        assert timestamp is not None, "missing timestamp header"
        assert verify_signature_header(SECRET, signature, body, tolerance_seconds=300), (
            "invalid HMAC signature"
        )
        payload = json.loads(body)
        delivery_id = request.headers.get("X-Webhook-Delivery-Id")
        attempt_no = sum(1 for r in self.requests if r["delivery_id"] == delivery_id) + 1
        self.requests.append(
            {
                "delivery_id": delivery_id,
                "event_id": payload["event_id"],
                "attempt_no": attempt_no,
                "body": payload,
            }
        )
        status = self.behavior(delivery_id, attempt_no)
        return httpx.Response(status, json={"status": status})

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


async def make_subscription(source: str = "shop", event_types: list | None = None) -> Subscription:
    async with db.session() as s:
        sub = Subscription(
            source=source,
            target_url="http://receiver.test/callback",
            secret=SECRET,
            event_types=event_types,
        )
        s.add(sub)
        await s.commit()
        return sub


async def make_event(source: str = "shop", event_id: str = "e1", type_: str = "order.created") -> Event:
    async with db.session() as s:
        event = Event(source=source, event_id=event_id, type=type_, payload={"id": event_id})
        s.add(event)
        await s.commit()
        return event


async def make_delivery(sub: Subscription, event: Event) -> Delivery:
    async with db.session() as s:
        delivery = Delivery(
            chain_id=uuid.uuid4(),
            subscription_id=sub.id,
            event_id=event.id,
            status="pending",
            attempts=0,
            next_attempt_at=utcnow(),
        )
        s.add(delivery)
        await s.commit()
        await s.refresh(delivery)
        return delivery


async def get_delivery(delivery_id) -> Delivery:
    async with db.session() as s:
        return await s.get(Delivery, delivery_id)


async def run_engine_until(engine: DeliveryEngine, condition, timeout: float = 15.0):
    task = asyncio.create_task(engine.run_forever())
    try:
        await wait_for(condition, timeout=timeout)
    finally:
        engine.stop()
        await task


# --------------------------------------------------------------------- tests

async def test_successful_delivery_with_valid_signature():
    sub = await make_subscription()
    event = await make_event()
    delivery = await make_delivery(sub, event)
    receiver = MockReceiver()
    engine = DeliveryEngine(make_settings(), http_client=receiver.client())

    async def done():
        d = await get_delivery(delivery.id)
        return d.status == "success"

    await run_engine_until(engine, done)

    d = await get_delivery(delivery.id)
    assert d.attempts == 1
    assert d.last_status_code == 200
    assert d.last_error is None
    assert len(receiver.requests) == 1
    req = receiver.requests[0]
    assert req["body"]["event_id"] == "e1"
    assert req["body"]["payload"] == {"id": "e1"}
    assert req["body"]["delivery_id"] == str(delivery.id)

    async with db.session() as s:
        attempts = (
            await s.scalars(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == delivery.id))
        ).all()
    assert len(attempts) == 1
    assert attempts[0].status_code == 200
    assert attempts[0].attempt_number == 1


async def test_retry_with_backoff_then_success():
    sub = await make_subscription()
    event = await make_event()
    delivery = await make_delivery(sub, event)
    receiver = MockReceiver(behavior=lambda _id, n: 500 if n <= 2 else 200)
    engine = DeliveryEngine(make_settings(), http_client=receiver.client())

    async def done():
        d = await get_delivery(delivery.id)
        return d.status == "success"

    await run_engine_until(engine, done)

    d = await get_delivery(delivery.id)
    assert d.attempts == 3
    assert [r["attempt_no"] for r in receiver.requests] == [1, 2, 3]

    async with db.session() as s:
        attempts = (
            await s.scalars(
                select(DeliveryAttempt)
                .where(DeliveryAttempt.delivery_id == delivery.id)
                .order_by(DeliveryAttempt.attempt_number)
            )
        ).all()
    assert [a.status_code for a in attempts] == [500, 500, 200]
    # Backoff: attempts are spaced by increasing delays (base 0.05 -> 0.05, 0.10).
    gaps = [
        (attempts[i + 1].created_at - attempts[i].created_at).total_seconds()
        for i in range(len(attempts) - 1)
    ]
    assert gaps[1] > gaps[0]


async def test_dead_letter_after_six_attempts():
    sub = await make_subscription()
    event = await make_event()
    delivery = await make_delivery(sub, event)
    receiver = MockReceiver(behavior=lambda _id, _n: 500)
    engine = DeliveryEngine(make_settings(), http_client=receiver.client())

    async def done():
        d = await get_delivery(delivery.id)
        return d.status == "dead"

    await run_engine_until(engine, done)

    d = await get_delivery(delivery.id)
    assert d.attempts == 6  # exactly max_attempts, then dead
    assert len(receiver.requests) == 6

    async with db.session() as s:
        count = await s.scalar(
            select(func.count(DeliveryAttempt.id)).where(DeliveryAttempt.delivery_id == delivery.id)
        )
    assert count == 6


async def test_strict_per_subscription_ordering_with_head_of_line_blocking():
    """e1 always fails; e2/e3 must not be attempted until e1 is dead."""
    sub = await make_subscription()
    events = [await make_event(event_id=f"e{i}") for i in (1, 2, 3)]
    deliveries = [await make_delivery(sub, ev) for ev in events]

    def behavior(delivery_id, _n):
        # fail every attempt of the first event's delivery
        if delivery_id == str(deliveries[0].id):
            return 500
        return 200

    receiver = MockReceiver(behavior=behavior)
    engine = DeliveryEngine(make_settings(), http_client=receiver.client())

    async def done():
        async with db.session() as s:
            rows = (await s.scalars(select(Delivery).order_by(Delivery.seq))).all()
            return [r.status for r in rows] == ["dead", "success", "success"]

    await run_engine_until(engine, done)

    sent = [r["event_id"] for r in receiver.requests]
    assert sent == ["e1"] * 6 + ["e2", "e3"]


async def test_lease_blocks_other_workers_until_expiry():
    sub = await make_subscription()
    event = await make_event()
    await make_delivery(sub, event)

    # Another worker holds a valid lease -> we cannot claim.
    async with db.session() as s:
        await s.execute(
            update(Subscription)
            .where(Subscription.id == sub.id)
            .values(lease_owner="other-worker", lease_expires_at=utcnow() + timedelta(minutes=5))
        )
        await s.commit()
    engine = DeliveryEngine(make_settings(worker_id="worker-2"), http_client=MockReceiver().client())
    assert await engine.claim_one() is None

    # Lease expires -> takeover succeeds and the event gets delivered.
    async with db.session() as s:
        await s.execute(
            update(Subscription)
            .where(Subscription.id == sub.id)
            .values(lease_expires_at=utcnow() - timedelta(seconds=1))
        )
        await s.commit()
    assert await engine.claim_one() == sub.id
    await engine.release(sub.id)


async def test_crashed_worker_lease_takeover_delivers_event():
    sub = await make_subscription()
    event = await make_event()
    delivery = await make_delivery(sub, event)

    # Simulate a crashed worker: lease owned but long expired.
    async with db.session() as s:
        await s.execute(
            update(Subscription)
            .where(Subscription.id == sub.id)
            .values(lease_owner="crashed-worker", lease_expires_at=utcnow() - timedelta(seconds=5))
        )
        await s.commit()

    receiver = MockReceiver()
    engine = DeliveryEngine(make_settings(worker_id="worker-2"), http_client=receiver.client())

    async def done():
        d = await get_delivery(delivery.id)
        return d.status == "success"

    await run_engine_until(engine, done)
    assert len(receiver.requests) == 1


async def test_fencing_when_lease_lost_mid_processing():
    """If the lease is stolen, the engine stops processing that subscription."""
    sub = await make_subscription()
    events = [await make_event(event_id=f"e{i}") for i in (1, 2)]
    await asyncio.gather(*[make_delivery(sub, ev) for ev in events])

    receiver = MockReceiver()
    engine = DeliveryEngine(make_settings(), http_client=receiver.client())
    assert await engine.claim_one() == sub.id

    # Steal the lease as another worker.
    async with db.session() as s:
        await s.execute(
            update(Subscription)
            .where(Subscription.id == sub.id)
            .values(lease_owner="thief", lease_expires_at=utcnow() + timedelta(minutes=5))
        )
        await s.commit()

    await engine.process_subscription(sub.id)
    assert receiver.requests == []  # fencing check stopped it before sending


async def test_replay_creates_new_chain_and_preserves_history(client):
    sub = await make_subscription()
    event = await make_event()
    delivery = await make_delivery(sub, event)
    receiver = MockReceiver(behavior=lambda _id, _n: 500)
    engine = DeliveryEngine(make_settings(), http_client=receiver.client())

    async def dead():
        d = await get_delivery(delivery.id)
        return d.status == "dead"

    await run_engine_until(engine, dead)
    assert len(receiver.requests) == 6

    # Replay via the API: new delivery, new chain, old record untouched.
    resp = await client.post(f"/dead-letters/{delivery.id}/replay")
    assert resp.status_code == 201, resp.text
    replay = resp.json()
    assert replay["status"] == "pending"
    assert replay["attempts"] == 0
    assert replay["replayed_from"] == str(delivery.id)
    old_delivery = await get_delivery(delivery.id)
    assert replay["chain_id"] != str(old_delivery.chain_id)  # new chain
    assert replay["chain_id"] != replay["replayed_from"]

    old = (await client.get(f"/deliveries/{delivery.id}")).json()
    assert old["status"] == "dead"
    assert old["attempts"] == 6
    assert len(old["history"]) == 6
    assert old["replays"] == [replay["id"]]

    # Now let the replay succeed; the old dead letter stays dead.
    receiver.behavior = lambda _id, _n: 200
    engine2 = DeliveryEngine(make_settings(), http_client=receiver.client())

    async def replay_done():
        d = await get_delivery(replay["id"])
        return d.status == "success"

    await run_engine_until(engine2, replay_done)
    old_after = await get_delivery(delivery.id)
    assert old_after.status == "dead"

    detail = (await client.get(f"/deliveries/{replay['id']}")).json()
    assert len(detail["history"]) == 1
    assert detail["history"][0]["status_code"] == 200


async def test_inactive_subscription_is_not_processed():
    sub = await make_subscription()
    event = await make_event()
    delivery = await make_delivery(sub, event)
    async with db.session() as s:
        await s.execute(
            update(Subscription).where(Subscription.id == sub.id).values(is_active=False)
        )
        await s.commit()

    engine = DeliveryEngine(make_settings(), http_client=MockReceiver().client())
    assert await engine.claim_one() is None
    d = await get_delivery(delivery.id)
    assert d.status == "pending"
