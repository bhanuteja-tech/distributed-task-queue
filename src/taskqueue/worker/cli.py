"""Worker Process CLI entrypoint.

Runs an independent worker listening to RabbitMQ and processing tasks.
Usage:
    python -m taskqueue.worker.cli
"""

import asyncio
import logging
import signal
import sys

from taskqueue.broker.rabbitmq import RabbitMQBroker
from taskqueue.config import settings
from taskqueue.database import async_session_factory, close_db
from taskqueue.logging import setup_logging
from taskqueue.services.worker_service import Worker

logger = logging.getLogger("taskqueue.worker")


async def run_worker() -> None:
    """Initialize dependencies and run worker event loop."""
    setup_logging(level=settings.log_level)
    logger.info("Initializing worker process...")

    broker = RabbitMQBroker()
    await broker.connect()

    worker = Worker(
        broker=broker,
        session_factory=async_session_factory,
        concurrency=settings.worker_concurrency,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Received termination signal. Shutting down worker...")
        stop_event.set()

    # Register OS signals for graceful termination
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler)
    else:
        # Windows signal support
        signal.signal(signal.SIGINT, lambda s, f: stop_event.set())
        signal.signal(signal.SIGTERM, lambda s, f: stop_event.set())

    await worker.start()
    logger.info("Worker is actively running and waiting for tasks.")

    try:
        await stop_event.wait()
    finally:
        await worker.stop()
        await broker.disconnect()
        await close_db()
        logger.info("Worker process terminated cleanly.")


def main() -> None:
    """CLI script entrypoint."""
    try:
        asyncio.run(run_worker())
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()

