"""Receiver app tests: signature verification and failure simulation."""

from __future__ import annotations

import time

import httpx
import pytest

from app.receiver import RECEIVED, SECRET, app as receiver_app
from app.security import make_signature_header


@pytest.fixture
async def client():
    RECEIVED.clear()
    transport = httpx.ASGITransport(app=receiver_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://receiver.test") as c:
        yield c


def signed_headers(body: bytes, secret: str = SECRET, timestamp: int | None = None) -> dict:
    ts, sig = make_signature_header(secret, body, timestamp)
    return {
        "X-Webhook-Timestamp": ts,
        "X-Webhook-Signature": sig,
        "X-Webhook-Delivery-Id": "d-1",
        "X-Webhook-Event-Type": "test",
    }


async def test_valid_signature_accepted(client):
    body = b'{"hello":"world"}'
    resp = await client.post("/callback", content=body, headers=signed_headers(body))
    assert resp.status_code == 200
    received = (await client.get("/received")).json()
    assert len(received) == 1
    assert received[0]["body"] == '{"hello":"world"}'


async def test_invalid_signature_rejected(client):
    body = b"{}"
    headers = signed_headers(body, secret="wrong-secret")
    resp = await client.post("/callback", content=body, headers=headers)
    assert resp.status_code == 401

    resp = await client.post("/callback", content=body)  # no signature at all
    assert resp.status_code == 401


async def test_expired_timestamp_rejected(client):
    body = b"{}"
    old_ts = int(time.time()) - 3600
    resp = await client.post("/callback", content=body, headers=signed_headers(body, timestamp=old_ts))
    assert resp.status_code == 401


async def test_fail_behavior(client):
    body = b"{}"
    resp = await client.post("/callback/fail", content=body, headers=signed_headers(body))
    assert resp.status_code == 500


async def test_flaky_behavior_fails_then_succeeds(client):
    body = b"{}"
    headers = signed_headers(body)
    codes = [await client.post("/callback/flaky", content=body, headers=headers) for _ in range(3)]
    assert [r.status_code for r in codes] == [500, 500, 200]
