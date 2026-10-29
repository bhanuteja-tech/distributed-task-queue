"""Metrics exposition route for Prometheus scraping."""

from fastapi import APIRouter, Response

from taskqueue.metrics import export_metrics

router = APIRouter(tags=["Metrics"])


@router.get(
    "/metrics",
    summary="Application metrics",
    description="Exposes Prometheus metrics for task latency, throughput, retries, and errors.",
)
async def metrics_endpoint() -> Response:
    data, content_type = export_metrics()
    return Response(content=data, media_type=content_type)

