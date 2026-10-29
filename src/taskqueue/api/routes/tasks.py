"""API routes for task submission, querying, pagination, and manual retries."""

import logging
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Response, status

from taskqueue.api.dependencies import DbSession, Service
from taskqueue.exceptions import (
    IdempotencyConflictError,
    PermanentTaskError,
    StateTransitionError,
    TaskNotFoundError,
)
from taskqueue.models.task import TaskStatus
from taskqueue.schemas.task import (
    TaskCreate,
    TaskListResponse,
    TaskResponse,
    TaskRetryRequest,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/tasks", tags=["Tasks"])


@router.post(
    "",
    response_model=TaskResponse,
    summary="Submit a new asynchronous task",
    description="Accepts a task payload, records it durably in PostgreSQL with an outbox event, and returns immediately.",
    responses={
        201: {"description": "Task created successfully"},
        200: {"description": "Idempotent duplicate submission returned existing task"},
        400: {"description": "Invalid payload or unknown task type"},
        409: {"description": "Idempotency key conflict with mismatched payload"},
    },
)
async def submit_task(
    task_in: TaskCreate,
    session: DbSession,
    service: Service,
    response: Response,
) -> TaskResponse:
    try:
        task, is_duplicate = await service.create_task(session, task_in)
        if is_duplicate:
            response.status_code = status.HTTP_200_OK
        else:
            response.status_code = status.HTTP_201_CREATED
        return TaskResponse.model_validate(task)
    except PermanentTaskError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except IdempotencyConflictError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e


@router.get(
    "/{task_id}",
    response_model=TaskResponse,
    summary="Retrieve task status and result",
    description="Fetches current lifecycle status, execution attempts, and result or error details.",
    responses={
        200: {"description": "Task details"},
        404: {"description": "Task not found"},
    },
)
async def get_task_by_id(
    task_id: str,
    session: DbSession,
    service: Service,
) -> TaskResponse:
    try:
        task = await service.get_task(session, task_id)
        return TaskResponse.model_validate(task)
    except TaskNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e


@router.get(
    "",
    response_model=TaskListResponse,
    summary="List tasks with pagination and status filtering",
    description="Returns a paginated list of tasks ordered by newest first.",
)
async def list_tasks(
    session: DbSession,
    service: Service,
    status_filter: Annotated[
        TaskStatus | None,
        Query(alias="status", description="Filter tasks by lifecycle status"),
    ] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=100, description="Number of tasks per page"),
    ] = 20,
    offset: Annotated[
        int,
        Query(ge=0, description="Pagination offset"),
    ] = 0,
) -> TaskListResponse:
    tasks, total = await service.list_tasks(
        session=session,
        status=status_filter,
        limit=limit,
        offset=offset,
    )
    items = [TaskResponse.model_validate(t) for t in tasks]
    return TaskListResponse(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
        has_next=(offset + limit) < total,
    )


@router.post(
    "/{task_id}/retry",
    response_model=TaskResponse,
    summary="Manually retry an eligible failed task",
    description="Transitions an eligible FAILED or DEAD_LETTER task back to PENDING and publishes an outbox event.",
    responses={
        200: {"description": "Task queued for retry"},
        400: {"description": "Task is not in an eligible failed state"},
        404: {"description": "Task not found"},
    },
)
async def retry_task(
    task_id: str,
    session: DbSession,
    service: Service,
    retry_req: TaskRetryRequest | None = None,
) -> TaskResponse:
    try:
        reason = retry_req.reason if retry_req else None
        task = await service.retry_failed_task(session, task_id, reason=reason)
        return TaskResponse.model_validate(task)
    except TaskNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e
    except StateTransitionError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot retry task: {e}",
        ) from e

