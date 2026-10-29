"""Standalone local server runner with live background worker and outbox publisher.

Allows running the FastAPI HTTP API on http://127.0.0.1:8000 alongside an
in-memory AMQP broker and background worker without requiring Docker.

Usage:
    python scripts/run_server.py
"""

import asyncio
import logging

import uvicorn
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from taskqueue.api.app import app
from taskqueue.api.dependencies import set_broker_instance
from taskqueue.broker.memory import InMemoryBroker
from taskqueue.database import Base
from taskqueue.outbox.publisher import OutboxPublisher
from taskqueue.outbox.reaper import LeaseReaper
from taskqueue.services.worker_service import Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("taskqueue.server")


async def main() -> None:
    db_file = "taskqueue_local.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_file}",
        echo=False,
        connect_args={"timeout": 30.0},
    )
    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA journal_mode=WAL;"))
        await conn.execute(text("PRAGMA synchronous=NORMAL;"))
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    broker = InMemoryBroker()
    await broker.connect()
    set_broker_instance(broker)

    publisher = OutboxPublisher(session_factory=session_factory, broker=broker, poll_interval=0.5)
    worker = Worker(broker=broker, session_factory=session_factory, worker_id="local-worker-1", concurrency=5)
    reaper = LeaseReaper(session_factory=session_factory, reaper_interval=5.0)

    await publisher.start()
    await worker.start()
    await reaper.start()

    logger.info("=" * 70)
    logger.info(" LOCAL DISTRIBUTED TASK QUEUE SERVER ACTIVE")
    logger.info(" Interactive Swagger Docs: http://127.0.0.1:8000/docs")
    logger.info(" Prometheus Metrics:       http://127.0.0.1:8000/metrics")
    logger.info(" Health Readiness:         http://127.0.0.1:8000/health/ready")
    logger.info("=" * 70)

    config = uvicorn.Config(app, host="127.0.0.1", port=8000, log_level="info")
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        logger.info("Shutting down background services...")
        await worker.stop()
        await publisher.stop()
        await reaper.stop()
        await broker.disconnect()
        await engine.dispose()
        logger.info("Local server stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
