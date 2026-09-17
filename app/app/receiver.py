"""Local callback receiver for development and demos.

Verifies the timestamped HMAC signature, records received webhooks in memory
and can simulate failures:

  POST /callback            behave per RECEIVER_BEHAVIOR (default: ok)
  POST /callback/ok         always 200
  POST /callback/fail       always 500 (drives deliveries to the DLQ)
  POST /callback/flaky      fail RECEIVER_FLAKY_FAILS times per delivery, then 200

The X-Receiver-Behavior request header overrides the behavior for one call.
"""

from __future__ import annotations

import os
from collections import deque

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .security import verify_signature_header

SECRET = os.environ.get("RECEIVER_SECRET", "whsec_dev_receiver_secret")
TOLERANCE = int(os.environ.get("RECEIVER_TOLERANCE_SECONDS", "300"))
DEFAULT_BEHAVIOR = os.environ.get("RECEIVER_BEHAVIOR", "ok")
FLAKY_FAILS = int(os.environ.get("RECEIVER_FLAKY_FAILS", "2"))

app = FastAPI(title="Webhook Test Receiver")

RECEIVED: deque[dict] = deque(maxlen=1000)
_FLAKY_COUNTS: dict[str, int] = {}


@app.post("/callback")
@app.post("/callback/{behavior}")
async def callback(request: Request, behavior: str | None = None):
    body = await request.body()
    behavior = behavior or request.headers.get("X-Receiver-Behavior") or DEFAULT_BEHAVIOR

    signature = request.headers.get("X-Webhook-Signature")
    if not verify_signature_header(SECRET, signature, body, TOLERANCE):
        return JSONResponse({"error": "invalid signature"}, status_code=401)

    delivery_id = request.headers.get("X-Webhook-Delivery-Id", "")

    if behavior == "fail":
        return JSONResponse({"error": "simulated failure"}, status_code=500)
    if behavior == "flaky":
        count = _FLAKY_COUNTS.get(delivery_id, 0) + 1
        _FLAKY_COUNTS[delivery_id] = count
        if count <= FLAKY_FAILS:
            return JSONResponse({"error": f"simulated flaky failure #{count}"}, status_code=500)

    RECEIVED.appendleft(
        {
            "delivery_id": delivery_id,
            "event_type": request.headers.get("X-Webhook-Event-Type"),
            "timestamp": request.headers.get("X-Webhook-Timestamp"),
            "body": body.decode("utf-8", "replace"),
        }
    )
    return {"received": True, "delivery_id": delivery_id}


@app.get("/received")
async def list_received():
    return list(RECEIVED)


@app.delete("/received")
async def clear_received():
    RECEIVED.clear()
    _FLAKY_COUNTS.clear()
    return {"cleared": True}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
