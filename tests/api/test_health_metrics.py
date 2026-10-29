"""API tests for health probes and metrics."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_liveness_probe(client: AsyncClient) -> None:
    response = await client.get("/health/live")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_readiness_probe(client: AsyncClient) -> None:
    response = await client.get("/health/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "database" in data["dependencies"]
    assert data["dependencies"]["database"]["status"] == "ok"


@pytest.mark.asyncio
async def test_metrics_endpoint(client: AsyncClient) -> None:
    # Trigger a task submission to increment a metric
    await client.post(
        "/tasks",
        json={"task_type": "report_generation", "payload": {"report_type": "metric_test"}},
    )

    response = await client.get("/metrics")
    assert response.status_code == 200
    body = response.text
    assert "taskqueue_tasks_submitted_total" in body

