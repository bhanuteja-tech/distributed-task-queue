"""Health check schemas for liveness and readiness probes."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DependencyHealth(BaseModel):
    """Status of an individual backend dependency."""

    status: str = Field(..., description="ok, degraded, or down")
    latency_ms: float | None = Field(None, description="Check duration in milliseconds")
    error: str | None = Field(None, description="Error description if unhealthy")


class HealthResponse(BaseModel):
    """Aggregate health check response."""

    status: str = Field(..., description="ok, degraded, or down")
    timestamp: datetime = Field(..., description="UTC time of check")
    dependencies: dict[str, DependencyHealth] = Field(
        default_factory=dict,
        description="Health status breakdown of external dependencies",
    )
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Auxiliary process information",
    )
