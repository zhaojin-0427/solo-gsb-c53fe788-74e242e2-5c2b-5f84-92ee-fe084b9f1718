"""Async database engine / session management.

`configure()` is called once at process startup (API lifespan, worker main,
or test fixtures) before any session is used.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from .models import Base

engine: AsyncEngine | None = None
SessionFactory: async_sessionmaker[AsyncSession] | None = None

# Advisory lock serializes concurrent `create_all` when api + workers boot together.
_SCHEMA_LOCK_KEY = 726_559_384


def configure(database_url: str, **engine_kwargs) -> None:
    global engine, SessionFactory
    kwargs = {"pool_size": 10, "max_overflow": 10}
    kwargs.update(engine_kwargs)
    engine = create_async_engine(database_url, **kwargs)
    SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


def session() -> AsyncSession:
    if SessionFactory is None:
        raise RuntimeError("db.configure() must be called before using sessions")
    return SessionFactory()


async def create_schema() -> None:
    if engine is None:
        raise RuntimeError("db.configure() must be called before create_schema()")
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _SCHEMA_LOCK_KEY})
        try:
            await conn.run_sync(Base.metadata.create_all)
        finally:
            await conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _SCHEMA_LOCK_KEY})


async def dispose() -> None:
    global engine, SessionFactory
    if engine is not None:
        await engine.dispose()
    engine = None
    SessionFactory = None
