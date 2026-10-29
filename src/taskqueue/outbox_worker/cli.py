"""Outbox Publisher and Lease Reaper Daemon CLI.

Runs the transactional outbox publisher and abandoned task lease reaper concurrently.
Usage:
    python -m taskqueue.outbox_worker.cli
"""

import asyncio
import logging
import signal
import sys

from taskqueue.broker.rabbitmq import RabbitMQBroker
from taskqueue.config import settings
from taskqueue.database import async_session_factory, close_db
from taskqueue.logging import setup_logging
from taskqueue.outbox.publisher import OutboxPublisher
from taskqueue.outbox.reaper import LeaseReaper

logger = logging.getLogger("taskqueue.outbox_worker")


async def run_outbox_daemon() -> None:
    """Run publisher and reaper in the same or separate background process."""
    setup_logging(level=settings.log_level)
    logger.info("Initializing Outbox Publisher and Lease Reaper...")

    broker = RabbitMQBroker()
    await broker.connect()

    publisher = OutboxPublisher(
        session_factory=async_session_factory,
        broker=broker,
        batch_size=settings.outbox_batch_size,
        poll_interval=settings.outbox_poll_interval_seconds,
    )

    reaper = LeaseReaper(
        session_factory=async_session_factory,
        reaper_interval=settings.reaper_interval_seconds,
        batch_size=settings.reaper_batch_size,
    )

    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Termination signal received. Shutting down daemon...")
        stop_event.set()

    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)
    else:
        signal.signal(signal.SIGINT, lambda s, f: stop_event.set())
        signal.signal(signal.SIGTERM, lambda s, f: stop_event.set())

    await publisher.start()
    await reaper.start()
    logger.info("Outbox publisher and Lease reaper started successfully.")

    try:
        await stop_event.wait()
    finally:
        await publisher.stop()
        await reaper.stop()
        await broker.disconnect()
        await close_db()
        logger.info("Daemon cleanly stopped.")


def main() -> None:
    """CLI script entrypoint."""
    try:
        asyncio.run(run_outbox_daemon())
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()

