"""Test fixtures: a real PostgreSQL (via pgserver) + fresh schema per session.

Set WEBHOOK_TEST_DATABASE_URL to reuse an existing PostgreSQL instead of
spinning up the embedded one.
"""

from __future__ import annotations

import asyncio
import os

# --- start PostgreSQL and point DATABASE_URL at it BEFORE importing the app ---
if "WEBHOOK_TEST_DATABASE_URL" in os.environ:
    os.environ["DATABASE_URL"] = os.environ["WEBHOOK_TEST_DATABASE_URL"]
else:
    import asyncpg
    import pgserver

    _PG_DIR = "/tmp/webhook_svc_test_pg"
    _server = pgserver.get_server(_PG_DIR)

    async def _ensure_db() -> None:
        conn = await asyncpg.connect(user="postgres", database="postgres", host=_PG_DIR)
        try:
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = 'webhook_test'"
            )
            if not exists:
                await conn.execute("CREATE DATABASE webhook_test")
        finally:
            await conn.close()

    asyncio.run(_ensure_db())
    os.environ["DATABASE_URL"] = f"postgresql+asyncpg://postgres@/webhook_test?host={_PG_DIR}"

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text

from app import db
from app.main import app as api_app


@pytest_asyncio.fixture(scope="session", autouse=True)
async def _schema():
    db.configure(os.environ["DATABASE_URL"])
    await db.create_schema()
    yield
    await db.dispose()


@pytest_asyncio.fixture(autouse=True)
async def _clean_tables():
    async with db.engine.begin() as conn:
        await conn.execute(
            text("TRUNCATE delivery_attempts, deliveries, subscriptions, events RESTART IDENTITY CASCADE")
        )
    yield


@pytest_asyncio.fixture
async def client():
    transport = httpx.ASGITransport(app=api_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://api.test") as c:
        yield c


async def wait_for(condition, timeout: float = 10.0, interval: float = 0.02):
    """Poll `condition` (async callable) until truthy; fail on timeout."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if await condition():
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(interval)
