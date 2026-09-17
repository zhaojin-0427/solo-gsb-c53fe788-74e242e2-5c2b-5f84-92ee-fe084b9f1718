"""Service configuration, driven by environment variables (see .env.example)."""

from __future__ import annotations

import socket

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # PostgreSQL DSN, e.g. postgresql+asyncpg://webhook:webhook@db:5432/webhook
    database_url: str = "postgresql+asyncpg://webhook:webhook@localhost:5432/webhook"

    # Delivery / retry policy
    max_attempts: int = 6                 # attempts before a delivery goes to the dead letter queue
    backoff_base_seconds: float = 1.0     # delay = base * 2**(attempt-1), capped + jitter
    backoff_max_seconds: float = 300.0
    backoff_jitter_ratio: float = 0.1
    http_timeout_seconds: float = 10.0

    # Worker lease
    lease_ttl_seconds: float = 30.0       # lease expires -> another worker may take over
    poll_interval_seconds: float = 0.5    # idle poll interval when no work is claimable
    worker_id: str = socket.gethostname()

    # Signature verification tolerance (used by the receiver)
    signature_tolerance_seconds: int = 300


settings = Settings()
