"""Simulated Report Generation Task Handler.

Generates structured analytical reports from input parameters.
Demonstrates validation, structured output, and deterministic error simulation.
"""

import asyncio
import hashlib
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue.exceptions import PermanentTaskError, RetryableTaskError
from taskqueue.handlers.base import BaseTaskHandler


class ReportGenerationHandler(BaseTaskHandler):
    """Generates synthetic analytical reports based on date range and parameters."""

    task_type = "report_generation"

    async def execute(
        self,
        task_id: str,
        payload: dict[str, Any],
        session: AsyncSession,
    ) -> dict[str, Any]:
        report_type = payload.get("report_type")
        if not report_type:
            raise PermanentTaskError("Missing required parameter: 'report_type'")

        date_range = payload.get("date_range", "last_30_days")
        include_charts = payload.get("include_charts", False)
        simulate_error = payload.get("simulate_error")

        # Deterministic failure injection support for tests
        if simulate_error == "transient":
            raise RetryableTaskError(
                "Transient database connection timeout while querying report data"
            )
        elif simulate_error == "permanent":
            raise PermanentTaskError(
                "Malformed report schema requested or invalid permissions"
            )

        # Simulated computation / processing time
        simulated_delay = payload.get("simulated_delay_seconds", 0.05)
        if simulated_delay > 0:
            await asyncio.sleep(min(simulated_delay, 2.0))

        # Deterministic generation based on hash of input
        seed_string = f"{report_type}:{date_range}:{task_id}"
        hash_val = int(hashlib.sha256(seed_string.encode()).hexdigest()[:8], 16)
        total_records = 1000 + (hash_val % 9000)
        total_amount = round(total_records * 42.75, 2)

        return {
            "report_id": f"REP-{task_id[:8].upper()}",
            "report_type": report_type,
            "date_range": date_range,
            "total_records_processed": total_records,
            "summary_metrics": {
                "total_revenue": total_amount,
                "average_order_value": round(total_amount / total_records, 2),
                "conversion_rate_pct": round(2.5 + (hash_val % 40) / 10.0, 2),
            },
            "include_charts": include_charts,
            "generated_at": datetime.now(UTC).isoformat(),
        }
