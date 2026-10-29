"""SQLAlchemy declarative base and common model utilities."""

from datetime import UTC, datetime

from sqlalchemy.orm import DeclarativeBase


def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(UTC)


def ensure_utc(dt: datetime | None) -> datetime | None:
    """Ensure a datetime object is timezone-aware UTC.

    Handles SQLite's storage of timezone-naive datetimes by attaching UTC timezone.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""

    pass
