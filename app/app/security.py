"""Timestamped HMAC-SHA256 request signing (Stripe-style).

The worker sends, for each delivery attempt:

  X-Webhook-Timestamp: <unix seconds>
  X-Webhook-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "{t}.{body}")>

Receivers recompute v1 over the *raw* body and reject requests whose
timestamp is older than the tolerance window (replay protection).
"""

from __future__ import annotations

import hashlib
import hmac
import time


def compute_signature(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode("utf-8"), digestmod=hashlib.sha256)
    mac.update(timestamp.encode("utf-8"))
    mac.update(b".")
    mac.update(body)
    return mac.hexdigest()


def make_signature_header(secret: str, body: bytes, timestamp: int | None = None) -> tuple[str, str]:
    """Return (timestamp, signature-header-value) for an outgoing request."""
    ts = str(timestamp if timestamp is not None else int(time.time()))
    return ts, f"t={ts},v1={compute_signature(secret, ts, body)}"


def verify_signature_header(
    secret: str,
    header: str | None,
    body: bytes,
    tolerance_seconds: int = 300,
    now: float | None = None,
) -> bool:
    if not header:
        return False
    parts: dict[str, str] = {}
    for item in header.split(","):
        if "=" in item:
            key, value = item.split("=", 1)
            parts[key.strip()] = value.strip()
    ts, v1 = parts.get("t"), parts.get("v1")
    if not ts or not v1:
        return False
    try:
        ts_int = int(ts)
    except ValueError:
        return False
    now = time.time() if now is None else now
    if abs(now - ts_int) > tolerance_seconds:
        return False
    expected = compute_signature(secret, ts, body)
    return hmac.compare_digest(expected, v1)
