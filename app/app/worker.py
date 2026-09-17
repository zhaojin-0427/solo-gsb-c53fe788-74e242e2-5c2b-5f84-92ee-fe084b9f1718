"""Worker process entrypoint: python -m app.worker"""

from __future__ import annotations

import asyncio
import logging
import signal

from . import db
from .config import settings
from .engine import DeliveryEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("webhook.worker")


async def amain() -> None:
    db.configure(settings.database_url)
    await db.create_schema()

    engine = DeliveryEngine(settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, engine.stop)

    await engine.run_forever()
    await db.dispose()


if __name__ == "__main__":
    asyncio.run(amain())
