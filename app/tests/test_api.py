"""API tests: ingestion idempotency, subscriptions, history, dead-letter replay."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select

from app import db
from app.models import Delivery


async def _make_subscription(client, **overrides) -> dict:
    body = {
        "source": "billing",
        "target_url": "http://receiver:9000/callback",
        "secret": "test-secret-123",
    }
    body.update(overrides)
    resp = await client.post("/subscriptions", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _post_event(client, **overrides):
    body = {"source": "billing", "event_id": "evt-1", "type": "invoice.paid", "payload": {"n": 1}}
    body.update(overrides)
    return await client.post("/events", json=body)


async def test_event_ingestion_creates_deliveries(client):
    await _make_subscription(client)
    resp = await _post_event(client)
    assert resp.status_code == 201
    data = resp.json()
    assert data["duplicate"] is False
    assert data["deliveries_created"] == 1
    assert data["source"] == "billing"
    assert data["event_id"] == "evt-1"


async def test_event_idempotency_returns_original_and_no_new_deliveries(client):
    await _make_subscription(client)
    first = await _post_event(client)
    assert first.status_code == 201

    # Same (source, event_id) -> HTTP 200, original event, no new deliveries.
    second = await _post_event(client, payload={"n": 999})
    assert second.status_code == 200
    assert second.json()["duplicate"] is True
    assert second.json()["deliveries_created"] == 0
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["payload"] == {"n": 1}  # original payload preserved

    async with db.session() as s:
        count = await s.scalar(select(func.count(Delivery.id)))
    assert count == 1

    # A different event_id under the same source is a new event.
    third = await _post_event(client, event_id="evt-2")
    assert third.status_code == 201
    assert third.json()["deliveries_created"] == 1


async def test_event_without_subscribers_creates_no_deliveries(client):
    resp = await _post_event(client, source="lonely")
    assert resp.status_code == 201
    assert resp.json()["deliveries_created"] == 0


async def test_event_type_filtering(client):
    await _make_subscription(client, event_types=["invoice.paid"])
    matched = await _post_event(client, event_id="e1", type="invoice.paid")
    assert matched.json()["deliveries_created"] == 1
    unmatched = await _post_event(client, event_id="e2", type="customer.created")
    assert unmatched.json()["deliveries_created"] == 0


async def test_subscription_crud_and_secret_handling(client):
    created = await _make_subscription(client)
    assert created["secret"] == "test-secret-123"
    assert created["is_active"] is True

    # Secret is generated when omitted and only shown at creation time.
    generated = await _make_subscription(client, source="x", secret=None)
    assert len(generated["secret"]) == 64

    listed = (await client.get("/subscriptions")).json()
    assert len(listed) == 2
    assert all("secret" not in s for s in listed)
    assert listed[0]["secret_hint"].startswith("****")

    got = (await client.get(f"/subscriptions/{created['id']}")).json()
    assert got["target_url"] == "http://receiver:9000/callback"

    # Soft delete: inactive subscriptions receive no new deliveries.
    resp = await client.delete(f"/subscriptions/{created['id']}")
    assert resp.status_code == 200
    assert (await client.get(f"/subscriptions/{created['id']}")).json()["is_active"] is False
    ev = await _post_event(client, event_id="evt-after-delete")
    assert ev.json()["deliveries_created"] == 0

    missing = await client.get(f"/subscriptions/{uuid.uuid4()}")
    assert missing.status_code == 404


async def test_delivery_history_endpoints(client):
    sub = await _make_subscription(client)
    ev = await _post_event(client)

    deliveries = (await client.get("/deliveries")).json()
    assert len(deliveries) == 1
    d = deliveries[0]
    assert d["status"] == "pending"
    assert d["subscription_id"] == sub["id"]
    assert d["event_id"] == ev.json()["id"]
    assert d["attempts"] == 0

    by_sub = (await client.get("/deliveries", params={"subscription_id": sub["id"]})).json()
    assert len(by_sub) == 1
    by_status = (await client.get("/deliveries", params={"status": "success"})).json()
    assert by_status == []

    detail = (await client.get(f"/deliveries/{d['id']}")).json()
    assert detail["history"] == []
    assert detail["replays"] == []

    assert (await client.get(f"/deliveries/{uuid.uuid4()}")).status_code == 404


async def test_replay_requires_dead_status(client):
    await _make_subscription(client)
    await _post_event(client)
    delivery = (await client.get("/deliveries")).json()[0]
    resp = await client.post(f"/dead-letters/{delivery['id']}/replay")
    assert resp.status_code == 409
    assert (await client.post(f"/dead-letters/{uuid.uuid4()}/replay")).status_code == 404


async def test_healthz(client):
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
