"""Unit tests for retry delay and exponential backoff calculations."""

from taskqueue.services.task_service import calculate_retry_delay


def test_exponential_backoff_without_jitter() -> None:
    # attempt 1: base delay 2.0 * (2 ** 0) = 2.0
    d1 = calculate_retry_delay(
        attempt=1,
        initial_delay=2.0,
        max_delay=60.0,
        backoff_base=2.0,
        jitter=False,
    )
    assert d1 == 2.0

    # attempt 2: 2.0 * (2 ** 1) = 4.0
    d2 = calculate_retry_delay(
        attempt=2,
        initial_delay=2.0,
        max_delay=60.0,
        backoff_base=2.0,
        jitter=False,
    )
    assert d2 == 4.0

    # attempt 3: 2.0 * (2 ** 2) = 8.0
    d3 = calculate_retry_delay(
        attempt=3,
        initial_delay=2.0,
        max_delay=60.0,
        backoff_base=2.0,
        jitter=False,
    )
    assert d3 == 8.0


def test_max_delay_cap() -> None:
    # attempt 10 would be 2.0 * 512 = 1024 without cap
    delay = calculate_retry_delay(
        attempt=10,
        initial_delay=2.0,
        max_delay=30.0,
        backoff_base=2.0,
        jitter=False,
    )
    assert delay == 30.0


def test_jitter_stays_within_bounds() -> None:
    base_delay = 10.0
    for _ in range(50):
        delay = calculate_retry_delay(
            attempt=1,
            initial_delay=base_delay,
            max_delay=60.0,
            backoff_base=2.0,
            jitter=True,
        )
        # Jitter applies uniform(0.8, 1.2)
        assert 8.0 <= delay <= 12.0

