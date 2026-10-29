"""Unit tests for Task state machine and lifecycle transitions."""

import pytest

from taskqueue.exceptions import StateTransitionError
from taskqueue.models.task import Task, TaskPriority, TaskStatus


def test_initial_state_is_pending() -> None:
    task = Task(
        task_type="report_generation",
        payload={"report_type": "summary"},
        priority=TaskPriority.DEFAULT,
    )
    assert task.status == TaskStatus.PENDING
    assert task.started_at is None
    assert task.completed_at is None
    assert task.version == 1


def test_valid_happy_path_transitions() -> None:
    task = Task(
        task_type="report_generation",
        payload={},
        status=TaskStatus.PENDING,
    )

    # 1. PENDING -> RUNNING
    task.transition_to(TaskStatus.RUNNING)
    assert task.status == TaskStatus.RUNNING
    assert task.started_at is not None
    assert task.completed_at is None
    assert task.version == 2

    # 2. RUNNING -> SUCCEEDED
    task.transition_to(TaskStatus.SUCCEEDED)
    assert task.status == TaskStatus.SUCCEEDED
    assert task.completed_at is not None
    assert task.version == 3


def test_valid_retry_and_dead_letter_transitions() -> None:
    task = Task(
        task_type="report_generation",
        payload={},
        status=TaskStatus.PENDING,
    )

    task.transition_to(TaskStatus.RUNNING)
    assert task.status == TaskStatus.RUNNING

    # RUNNING -> RETRYING
    task.transition_to(TaskStatus.RETRYING)
    assert task.status == TaskStatus.RETRYING

    # RETRYING -> RUNNING
    task.transition_to(TaskStatus.RUNNING)
    assert task.status == TaskStatus.RUNNING

    # RUNNING -> DEAD_LETTER
    task.transition_to(TaskStatus.DEAD_LETTER)
    assert task.status == TaskStatus.DEAD_LETTER
    assert task.completed_at is not None


def test_invalid_transitions_raise_error() -> None:
    task = Task(
        task_type="report_generation",
        payload={},
        status=TaskStatus.PENDING,
    )

    # Cannot skip straight from PENDING to SUCCEEDED
    with pytest.raises(StateTransitionError) as exc_info:
        task.transition_to(TaskStatus.SUCCEEDED)
    assert exc_info.value.current_state == "PENDING"
    assert exc_info.value.target_state == "SUCCEEDED"

    # Move to SUCCEEDED
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.SUCCEEDED)

    # SUCCEEDED is terminal: cannot transition out
    with pytest.raises(StateTransitionError) as exc_info:
        task.transition_to(TaskStatus.RUNNING)
    assert exc_info.value.current_state == "SUCCEEDED"

    with pytest.raises(StateTransitionError) as exc_info:
        task.transition_to(TaskStatus.FAILED)
    assert exc_info.value.current_state == "SUCCEEDED"


def test_manual_retry_from_failed() -> None:
    task = Task(
        task_type="report_generation",
        payload={},
        status=TaskStatus.PENDING,
    )
    task.transition_to(TaskStatus.RUNNING)
    task.transition_to(TaskStatus.FAILED)
    assert task.status == TaskStatus.FAILED

    # Allowed to transition back to PENDING via operator retry
    task.transition_to(TaskStatus.PENDING)
    assert task.status == TaskStatus.PENDING

