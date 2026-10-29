"""Domain exceptions for the Distributed Task Queue system.

Distinguishes between retryable (transient) failures and permanent failures.
Also provides domain exceptions for state transitions, concurrency conflicts,
and lease violations.
"""


class TaskQueueError(Exception):
    """Base exception for all task queue errors."""

    def __init__(self, message: str, task_id: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.task_id = task_id


class StateTransitionError(TaskQueueError):
    """Raised when an illegal task state transition is requested."""

    def __init__(self, current_state: str, target_state: str, task_id: str | None = None) -> None:
        message = f"Invalid state transition from {current_state} to {target_state}"
        super().__init__(message, task_id=task_id)
        self.current_state = current_state
        self.target_state = target_state


class TaskNotFoundError(TaskQueueError):
    """Raised when a requested task does not exist in the database."""

    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task not found: {task_id}", task_id=task_id)


class IdempotencyConflictError(TaskQueueError):
    """Raised when a task with the same idempotency key exists with differing payload."""

    def __init__(self, key: str, task_id: str | None = None) -> None:
        super().__init__(
            f"Idempotency conflict: key '{key}' already registered with different parameters",
            task_id=task_id,
        )
        self.key = key


class ConcurrencyConflictError(TaskQueueError):
    """Raised when optimistic concurrency control detects a conflicting state update."""

    def __init__(self, task_id: str, current_version: int) -> None:
        super().__init__(
            f"Concurrency conflict on task {task_id} (version {current_version})",
            task_id=task_id,
        )
        self.current_version = current_version


class LeaseExpiredError(TaskQueueError):
    """Raised when a worker attempts to update or finish a task whose lease has expired."""

    def __init__(self, task_id: str, worker_id: str) -> None:
        super().__init__(
            f"Task lease expired for worker {worker_id} on task {task_id}",
            task_id=task_id,
        )
        self.worker_id = worker_id


# Handler execution exception hierarchy:
class TaskExecutionError(TaskQueueError):
    """Base exception for errors raised during task handler execution."""

    pass


class RetryableTaskError(TaskExecutionError):
    """Transient error indicating the task handler encountered a recoverable issue.

    Examples: temporary network timeout, database lock contention, rate limit exceeded.
    The task queue will schedule this task for retry with exponential backoff.
    """

    def __init__(
        self,
        message: str,
        task_id: str | None = None,
        retry_delay_seconds: float | None = None,
    ) -> None:
        super().__init__(message, task_id=task_id)
        self.retry_delay_seconds = retry_delay_seconds


class PermanentTaskError(TaskExecutionError):
    """Fatal error indicating the task handler cannot succeed on retry.

    Examples: malformed payload, missing business prerequisite, validation failure.
    The task queue will transition this task directly to FAILED or DEAD_LETTER without retrying.
    """

    pass


class TaskTimeoutError(RetryableTaskError):
    """Raised when handler execution exceeds the configured timeout."""

    pass

