"""API routes."""

from taskqueue.api.routes.health import router as health_router
from taskqueue.api.routes.metrics import router as metrics_router
from taskqueue.api.routes.tasks import router as tasks_router

__all__ = ["health_router", "metrics_router", "tasks_router"]

