"""Unit tests for task handler registry and individual task handlers."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue.exceptions import PermanentTaskError, RetryableTaskError
from taskqueue.handlers.base import HandlerRegistry
from taskqueue.handlers.ledger import LedgerBatchHandler
from taskqueue.handlers.report import ReportGenerationHandler
from taskqueue.handlers.word_count import WordCountHandler


@pytest.mark.asyncio
async def test_report_generation_handler_success(db_session: AsyncSession) -> None:
    handler = ReportGenerationHandler()
    result = await handler.execute(
        task_id="test-task-1",
        payload={"report_type": "quarterly_sales", "date_range": "2026-Q1", "simulated_delay_seconds": 0.0},
        session=db_session,
    )
    assert result["report_type"] == "quarterly_sales"
    assert result["total_records_processed"] > 0
    assert "summary_metrics" in result
    assert "total_revenue" in result["summary_metrics"]


@pytest.mark.asyncio
async def test_report_generation_handler_missing_param(db_session: AsyncSession) -> None:
    handler = ReportGenerationHandler()
    with pytest.raises(PermanentTaskError) as exc_info:
        await handler.execute(
            task_id="test-task-2",
            payload={},
            session=db_session,
        )
    assert "report_type" in str(exc_info.value)


@pytest.mark.asyncio
async def test_report_generation_handler_simulated_errors(db_session: AsyncSession) -> None:
    handler = ReportGenerationHandler()
    with pytest.raises(RetryableTaskError):
        await handler.execute(
            task_id="test-task-3",
            payload={"report_type": "sales", "simulate_error": "transient", "simulated_delay_seconds": 0.0},
            session=db_session,
        )

    with pytest.raises(PermanentTaskError):
        await handler.execute(
            task_id="test-task-4",
            payload={"report_type": "sales", "simulate_error": "permanent", "simulated_delay_seconds": 0.0},
            session=db_session,
        )


@pytest.mark.asyncio
async def test_ledger_batch_handler_success(db_session: AsyncSession) -> None:
    handler = LedgerBatchHandler()
    payload = {
        "account_id": "acc-test-100",
        "transactions": [
            {"transaction_id": "tx-1", "amount": 150.0, "operation": "CREDIT"},
            {"transaction_id": "tx-2", "amount": 50.0, "operation": "DEBIT"},
        ],
    }
    result = await handler.execute(task_id="task-ledger-1", payload=payload, session=db_session)
    assert result["account_id"] == "acc-test-100"
    assert result["final_balance"] == 100.0
    assert result["applied_count"] == 2
    assert result["skipped_duplicate_count"] == 0


@pytest.mark.asyncio
async def test_word_count_handler_success(db_session: AsyncSession) -> None:
    handler = WordCountHandler()
    payload = {"text": "distributed task queue distributed systems architecture distributed"}
    result = await handler.execute(task_id="task-wc-1", payload=payload, session=db_session)
    assert result["total_words"] == 7
    assert result["top_words"]["distributed"] == 3


def test_registry_unrecognized_type() -> None:
    reg = HandlerRegistry()
    reg.register(ReportGenerationHandler())
    assert "report_generation" in reg.list_supported_types()

    with pytest.raises(PermanentTaskError) as exc_info:
        reg.get("non_existent_handler")
    assert "non_existent_handler" in str(exc_info.value)
