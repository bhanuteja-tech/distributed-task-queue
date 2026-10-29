"""Health check endpoints for liveness and readiness probes."""

import time

from fastapi import APIRouter, Response, status
from sqlalchemy import text

from taskqueue.api.dependencies import Broker, DbSession
from taskqueue.cache.redis import cache
from taskqueue.config import settings
from taskqueue.models.base import utc_now
from taskqueue.schemas.health import DependencyHealth, HealthResponse

router = APIRouter(prefix="/health", tags=["Health"])


@router.get(
    "/live",
    summary="Liveness probe",
    description="Returns 200 OK if the FastAPI process is alive and responding.",
)
async def liveness_probe() -> dict[str, str]:
    return {"status": "ok", "service": settings.app_name}


@router.get(
    "/ready",
    response_model=HealthResponse,
    summary="Readiness probe",
    description="Probes connectivity to PostgreSQL, RabbitMQ, and Redis (if enabled).",
)
async def readiness_probe(
    session: DbSession,
    broker: Broker,
    response: Response,
) -> HealthResponse:
    deps: dict[str, DependencyHealth] = {}
    is_healthy = True

    # 1. Probe Database
    t0 = time.perf_counter()
    try:
        await session.execute(text("SELECT 1"))
        latency = (time.perf_counter() - t0) * 1000
        deps["database"] = DependencyHealth(status="ok", latency_ms=round(latency, 2))
    except Exception as e:
        latency = (time.perf_counter() - t0) * 1000
        deps["database"] = DependencyHealth(status="down", latency_ms=round(latency, 2), error=str(e))
        is_healthy = False

    # 2. Probe Message Broker
    t0 = time.perf_counter()
    try:
        broker_ok = await broker.check_health()
        latency = (time.perf_counter() - t0) * 1000
        if broker_ok:
            deps["message_broker"] = DependencyHealth(status="ok", latency_ms=round(latency, 2))
        else:
            deps["message_broker"] = DependencyHealth(status="down", latency_ms=round(latency, 2), error="Broker reports disconnected")
            is_healthy = False
    except Exception as e:
        latency = (time.perf_counter() - t0) * 1000
        deps["message_broker"] = DependencyHealth(status="down", latency_ms=round(latency, 2), error=str(e))
        is_healthy = False

    # 3. Probe Redis (if enabled)
    if settings.redis_enabled:
        t0 = time.perf_counter()
        try:
            redis_ok = await cache.check_health()
            latency = (time.perf_counter() - t0) * 1000
            deps["cache"] = DependencyHealth(
                status="ok" if redis_ok else "down",
                latency_ms=round(latency, 2),
            )
            # Notice: Redis failure is degraded, not fatal, because DB is authoritative!
        except Exception as e:
            latency = (time.perf_counter() - t0) * 1000
            deps["cache"] = DependencyHealth(status="down", latency_ms=round(latency, 2), error=str(e))

    overall_status = "ok" if is_healthy else "degraded"
    if not is_healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status=overall_status,
        timestamp=utc_now(),
        dependencies=deps,
        details={
            "environment": settings.environment,
            "version": "0.1.0",
        },
    )
