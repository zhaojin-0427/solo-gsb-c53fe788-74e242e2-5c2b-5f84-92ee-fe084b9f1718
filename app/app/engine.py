"""Delivery engine: lease-based, strictly ordered, at-least-once delivery.

Concurrency & ordering model
----------------------------
* The unit of work is a *subscription queue*. A worker claims a subscription
  by atomically stamping (lease_owner, lease_expires_at) on its row; the
  UPDATE ... WHERE lease-expired guard makes the claim race-safe across
  processes. A lease that is not renewed (crashed worker) expires and can be
  taken over by any other worker.
* While holding the lease, the worker processes ONLY the head of the queue:
  the pending delivery with the lowest `seq`. Event N+1 is never sent while
  event N is still pending (awaiting retry) - it becomes eligible only when
  N succeeds or exhausts its attempts and turns `dead`.
* Failed attempts are rescheduled with exponential backoff
  (base * 2**(attempt-1), capped, plus jitter). After `max_attempts`
  failures the delivery is moved to the dead letter queue.
* Every state write re-checks lease ownership (fencing), so a worker that
  silently lost its lease stops instead of corrupting the queue.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta

import httpx
from sqlalchemy import or_, select, update

from . import db
from .config import Settings
from .models import Delivery, DeliveryAttempt, Event, Subscription, utcnow
from .security import make_signature_header

log = logging.getLogger("webhook.engine")


@dataclass
class _Outcome:
    ok: bool
    status_code: int | None
    error: str | None
    duration_ms: float


@dataclass
class _Head:
    """A due head-of-queue delivery plus everything needed to attempt it."""

    delivery_id: uuid.UUID
    target_url: str
    secret: str
    body: dict


class DeliveryEngine:
    def __init__(self, settings: Settings, http_client: httpx.AsyncClient | None = None):
        self.settings = settings
        self.worker_id = settings.worker_id
        self._client = http_client
        self._owns_client = http_client is None
        self._stop = asyncio.Event()

    # ------------------------------------------------------------------ loop

    async def run_forever(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.settings.http_timeout_seconds)
            self._owns_client = True
        log.info(
            "worker %s started (lease_ttl=%ss, max_attempts=%d)",
            self.worker_id,
            self.settings.lease_ttl_seconds,
            self.settings.max_attempts,
        )
        try:
            while not self._stop.is_set():
                subscription_id = await self.claim_one()
                if subscription_id is None:
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(), timeout=self.settings.poll_interval_seconds
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue
                try:
                    await self.process_subscription(subscription_id)
                except Exception:
                    log.exception("error while processing subscription %s", subscription_id)
                finally:
                    await self.release(subscription_id)
        finally:
            if self._owns_client and self._client is not None:
                await self._client.aclose()
            self._client = None
        log.info("worker %s stopped", self.worker_id)

    def stop(self) -> None:
        self._stop.set()

    # ----------------------------------------------------------------- lease

    async def claim_one(self) -> uuid.UUID | None:
        """Atomically claim one subscription that has due work. None if idle."""
        now = utcnow()
        expires = now + timedelta(seconds=self.settings.lease_ttl_seconds)
        lease_free = or_(
            Subscription.lease_expires_at.is_(None),
            Subscription.lease_expires_at <= now,
        )
        due_work = (
            select(Delivery.id)
            .where(Delivery.subscription_id == Subscription.id)
            .where(Delivery.status == "pending")
            .where(Delivery.next_attempt_at <= now)
            .exists()
        )
        async with db.session() as s:
            candidates = (
                await s.execute(
                    select(Subscription.id)
                    .where(Subscription.is_active.is_(True))
                    .where(lease_free)
                    .where(due_work)
                    .order_by(Subscription.id)
                    .limit(5)
                )
            ).scalars().all()
            for sid in candidates:
                # Atomic guard: only one racing worker can flip the lease.
                res = await s.execute(
                    update(Subscription)
                    .where(Subscription.id == sid)
                    .where(lease_free)
                    .values(lease_owner=self.worker_id, lease_expires_at=expires)
                )
                if res.rowcount == 1:
                    await s.commit()
                    log.debug("worker %s claimed subscription %s", self.worker_id, sid)
                    return sid
            await s.rollback()
            return None

    async def renew_lease(self, subscription_id: uuid.UUID) -> bool:
        """Extend our lease. Returns False if we no longer own it (fencing)."""
        expires = utcnow() + timedelta(seconds=self.settings.lease_ttl_seconds)
        async with db.session() as s:
            res = await s.execute(
                update(Subscription)
                .where(Subscription.id == subscription_id)
                .where(Subscription.lease_owner == self.worker_id)
                .values(lease_expires_at=expires)
            )
            await s.commit()
            return res.rowcount == 1

    async def release(self, subscription_id: uuid.UUID) -> None:
        """Best-effort release; only clears the lease if we still own it."""
        try:
            async with db.session() as s:
                await s.execute(
                    update(Subscription)
                    .where(Subscription.id == subscription_id)
                    .where(Subscription.lease_owner == self.worker_id)
                    .values(lease_owner=None, lease_expires_at=None)
                )
                await s.commit()
        except Exception:
            log.warning("failed to release lease on %s", subscription_id, exc_info=True)

    # -------------------------------------------------------------- processing

    async def process_subscription(self, subscription_id: uuid.UUID) -> None:
        """Drain the subscription queue head-by-head while we hold the lease."""
        while not self._stop.is_set():
            if not await self.renew_lease(subscription_id):
                log.warning("worker %s lost lease on %s", self.worker_id, subscription_id)
                return
            head = await self._get_head(subscription_id)
            if head is None:
                return  # queue empty, or head not yet due (backoff) - come back later
            outcome = await self._attempt(head)
            if not await self._record_outcome(subscription_id, head.delivery_id, outcome):
                log.warning(
                    "worker %s lost lease while recording %s", self.worker_id, head.delivery_id
                )
                return

    async def _get_head(self, subscription_id: uuid.UUID) -> _Head | None:
        now = utcnow()
        async with db.session() as s:
            delivery = await s.scalar(
                select(Delivery)
                .where(Delivery.subscription_id == subscription_id)
                .where(Delivery.status == "pending")
                .order_by(Delivery.seq)
                .limit(1)
            )
            if delivery is None or delivery.next_attempt_at > now:
                return None
            sub = await s.get(Subscription, subscription_id)
            if sub is None or not sub.is_active:
                return None
            event = await s.get(Event, delivery.event_id)
            if event is None:
                return None
            body = {
                "id": str(event.id),
                "source": event.source,
                "event_id": event.event_id,
                "type": event.type,
                "payload": event.payload,
                "delivery_id": str(delivery.id),
                "chain_id": str(delivery.chain_id),
                "created_at": event.created_at.isoformat(),
            }
            return _Head(
                delivery_id=delivery.id,
                target_url=sub.target_url,
                secret=sub.secret,
                body=body,
            )

    async def _attempt(self, head: _Head) -> _Outcome:
        body = json.dumps(head.body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        timestamp, signature = make_signature_header(head.secret, body)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "webhook-delivery/1.0",
            "X-Webhook-Delivery-Id": str(head.delivery_id),
            "X-Webhook-Event-Type": str(head.body["type"]),
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": signature,
        }
        start = time.monotonic()
        try:
            resp = await self._client.post(head.target_url, content=body, headers=headers)
            duration_ms = (time.monotonic() - start) * 1000
            ok = 200 <= resp.status_code < 300
            return _Outcome(
                ok=ok,
                status_code=resp.status_code,
                error=None if ok else f"HTTP {resp.status_code}: {resp.text[:300]}",
                duration_ms=duration_ms,
            )
        except httpx.HTTPError as exc:
            duration_ms = (time.monotonic() - start) * 1000
            return _Outcome(
                ok=False,
                status_code=None,
                error=f"{type(exc).__name__}: {exc}"[:500],
                duration_ms=duration_ms,
            )

    async def _record_outcome(
        self, subscription_id: uuid.UUID, delivery_id: uuid.UUID, outcome: _Outcome
    ) -> bool:
        """Persist the attempt result. Returns False if the lease was lost."""
        now = utcnow()
        async with db.session() as s:
            owner = await s.scalar(
                select(Subscription.lease_owner).where(Subscription.id == subscription_id)
            )
            if owner != self.worker_id:
                await s.rollback()
                return False
            delivery = await s.get(Delivery, delivery_id)
            if delivery is None or delivery.status != "pending":
                await s.rollback()
                return True  # nothing to record; keep going
            delivery.attempts += 1
            delivery.last_status_code = outcome.status_code
            delivery.last_error = outcome.error
            delivery.updated_at = now
            s.add(
                DeliveryAttempt(
                    delivery_id=delivery.id,
                    attempt_number=delivery.attempts,
                    status_code=outcome.status_code,
                    error=outcome.error,
                    duration_ms=outcome.duration_ms,
                )
            )
            if outcome.ok:
                delivery.status = "success"
            elif delivery.attempts >= self.settings.max_attempts:
                delivery.status = "dead"
                log.info(
                    "delivery %s dead-lettered after %d attempts",
                    delivery.id,
                    delivery.attempts,
                )
            else:
                delay = self.backoff_delay(delivery.attempts)
                delivery.next_attempt_at = now + timedelta(seconds=delay)
                log.debug(
                    "delivery %s attempt %d failed, retry in %.2fs",
                    delivery.id,
                    delivery.attempts,
                    delay,
                )
            await s.commit()
            return True

    def backoff_delay(self, attempts: int) -> float:
        """Exponential backoff: base * 2**(attempts-1), capped, plus jitter."""
        delay = self.settings.backoff_base_seconds * (2 ** (attempts - 1))
        delay = min(delay, self.settings.backoff_max_seconds)
        jitter = delay * self.settings.backoff_jitter_ratio * random.random()
        return delay + jitter
