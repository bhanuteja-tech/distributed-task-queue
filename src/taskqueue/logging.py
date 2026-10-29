"""Structured logging system with contextual correlation IDs.

Provides JSON-formatted logs with contextual task_id, request_id, and worker_id
using Python's contextvars for async-safe correlation across coroutines.
"""

import contextvars
import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

# Context variables for correlation across async execution boundaries
correlation_task_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_task_id", default=None
)
correlation_request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_request_id", default=None
)
correlation_worker_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_worker_id", default=None
)


class JSONFormatter(logging.Formatter):
    """Custom formatter rendering log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include contextual correlation fields if available
        task_id = correlation_task_id.get()
        if task_id:
            log_obj["task_id"] = task_id

        request_id = correlation_request_id.get()
        if request_id:
            log_obj["request_id"] = request_id

        worker_id = correlation_worker_id.get()
        if worker_id:
            log_obj["worker_id"] = worker_id

        # Merge extra attributes if provided
        for key, value in record.__dict__.items():
            if key not in (
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "message",
            ):
                log_obj[key] = value

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj, default=str)


def setup_logging(level: str = "INFO", json_format: bool = True) -> None:
    """Configure root logger with either JSON or console formatting."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())

    # Remove existing handlers to avoid duplicates
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    if json_format:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
        )

    root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger instance."""
    return logging.getLogger(name)
