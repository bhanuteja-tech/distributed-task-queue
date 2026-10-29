"""FastAPI Application factory and lifecycle configuration."""

import logging
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from taskqueue.api.dependencies import get_broker
from taskqueue.api.routes import health_router, metrics_router, tasks_router
from taskqueue.cache.redis import cache
from taskqueue.config import settings
from taskqueue.database import close_db
from taskqueue.logging import correlation_request_id, setup_logging

logger = logging.getLogger(__name__)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Assigns or propagates an X-Request-ID across the HTTP request lifecycle."""

    async def dispatch(self, request: Request, call_next) -> Response:
        req_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        token = correlation_request_id.set(req_id)
        try:
            response: Response = await call_next(request)
            response.headers["X-Request-ID"] = req_id
            return response
        finally:
            correlation_request_id.reset(token)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager for startup and graceful shutdown."""
    setup_logging(level=settings.log_level)
    logger.info("Initializing %s in %s environment...", settings.app_name, settings.environment)

    # Connect cache if enabled
    if settings.redis_enabled:
        await cache.connect()

    yield

    # Clean shutdown
    logger.info("Shutting down %s...", settings.app_name)
    if settings.redis_enabled:
        await cache.disconnect()

    try:
        broker = get_broker()
        await broker.disconnect()
    except Exception as e:
        logger.warning("Error disconnecting broker: %s", e)

    await close_db()
    logger.info("Application shutdown complete.")


def create_app() -> FastAPI:
    """Build and configure the FastAPI application instance."""
    app = FastAPI(
        title="Distributed Task Queue API",
        description=(
            "A fault-tolerant distributed task queue demonstrating asynchronous execution, "
            "transactional outbox publishing, worker leases, idempotent processing, and observability."
        ),
        version="0.1.0",
        lifespan=lifespan,
    )

    # Middleware
    app.add_middleware(CorrelationIdMiddleware)

    # Routes
    app.include_router(tasks_router)
    app.include_router(health_router)
    app.include_router(metrics_router)

    return app


# Default ASGI app instance for uvicorn
app = create_app()
