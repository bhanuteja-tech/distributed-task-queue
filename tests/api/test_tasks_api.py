"""API functional tests for task endpoints."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_submit_task_success(client: AsyncClient) -> None:
    payload = {
        "task_type": "report_generation",
        "payload": {"report_type": "monthly_summary", "simulated_delay_seconds": 0.0},
        "priority": "HIGH",
    }
    response = await client.post("/tasks", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert "id" in data
    assert data["task_type"] == "report_generation"
    assert data["status"] == "PENDING"
    assert data["priority"] == "HIGH"
    assert data["attempt_count"] == 0


@pytest.mark.asyncio
async def test_submit_task_idempotency_behavior(client: AsyncClient) -> None:
    key = "unique-order-key-555"
    payload = {
        "task_type": "report_generation",
        "payload": {"report_type": "annual"},
        "idempotency_key": key,
    }

    # 1. First submission creates the task
    res1 = await client.post("/tasks", json=payload)
    assert res1.status_code == 201
    task1 = res1.json()

    # 2. Second submission with IDENTICAL key & payload returns existing task
    res2 = await client.post("/tasks", json=payload)
    assert res2.status_code == 200
    task2 = res2.json()
    assert task1["id"] == task2["id"]

    # 3. Third submission with SAME key but DIFFERENT payload returns 409 Conflict
    conflict_payload = {
        "task_type": "report_generation",
        "payload": {"report_type": "different_summary"},
        "idempotency_key": key,
    }
    res3 = await client.post("/tasks", json=conflict_payload)
    assert res3.status_code == 409
    assert "Idempotency conflict" in res3.json()["detail"]


@pytest.mark.asyncio
async def test_submit_unknown_task_type(client: AsyncClient) -> None:
    payload = {
        "task_type": "invalid_unknown_type",
        "payload": {},
    }
    response = await client.post("/tasks", json=payload)
    assert response.status_code == 400
    assert "Unknown task_type" in response.json()["detail"]


@pytest.mark.asyncio
async def test_get_task_by_id(client: AsyncClient) -> None:
    # Submit first
    post_res = await client.post(
        "/tasks",
        json={"task_type": "word_count_aggregation", "payload": {"text": "hello world"}},
    )
    task_id = post_res.json()["id"]

    # Query
    get_res = await client.get(f"/tasks/{task_id}")
    assert get_res.status_code == 200
    assert get_res.json()["id"] == task_id


@pytest.mark.asyncio
async def test_get_non_existent_task(client: AsyncClient) -> None:
    response = await client.get("/tasks/non-existent-uuid-999")
    assert response.status_code == 404
    assert "Task not found" in response.json()["detail"]


@pytest.mark.asyncio
async def test_list_tasks_pagination(client: AsyncClient) -> None:
    for i in range(5):
        await client.post(
            "/tasks",
            json={"task_type": "word_count_aggregation", "payload": {"text": f"item {i}"}},
        )

    response = await client.get("/tasks?limit=3&offset=0")
    assert response.status_code == 200
    data = response.json()
    assert data["limit"] == 3
    assert data["offset"] == 0
    assert len(data["items"]) == 3
    assert data["total"] >= 5
    assert data["has_next"] is True


@pytest.mark.asyncio
async def test_retry_non_failed_task_rejected(client: AsyncClient) -> None:
    post_res = await client.post(
        "/tasks",
        json={"task_type": "report_generation", "payload": {"report_type": "test"}},
    )
    task_id = post_res.json()["id"]

    # Attempting to retry a PENDING task should fail
    retry_res = await client.post(f"/tasks/{task_id}/retry")
    assert retry_res.status_code == 400
    assert "Cannot retry task" in retry_res.json()["detail"]

