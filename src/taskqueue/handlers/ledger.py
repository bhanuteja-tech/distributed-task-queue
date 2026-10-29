"""Ledger Batch Processing Task Handler.

Demonstrates business-level idempotency and transactional side effects.
Even if RabbitMQ delivers the task multiple times, the transaction_id uniqueness
guarantees that accounts are not credited or debited more than once.
"""

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from taskqueue.exceptions import PermanentTaskError, RetryableTaskError
from taskqueue.handlers.base import BaseTaskHandler
from taskqueue.models.ledger import Account, LedgerEntry


class LedgerBatchHandler(BaseTaskHandler):
    """Processes financial batch transactions idempotently."""

    task_type = "ledger_batch_processor"

    async def execute(
        self,
        task_id: str,
        payload: dict[str, Any],
        session: AsyncSession,
    ) -> dict[str, Any]:
        account_id = payload.get("account_id")
        if not account_id:
            raise PermanentTaskError("Missing required parameter: 'account_id'")

        transactions = payload.get("transactions", [])
        if not isinstance(transactions, list) or not transactions:
            raise PermanentTaskError("Payload 'transactions' must be a non-empty list")

        # Deterministic failure injection
        simulate_error = payload.get("simulate_error")
        if simulate_error == "transient":
            raise RetryableTaskError("Database deadlock simulation on ledger batch")
        elif simulate_error == "permanent":
            raise PermanentTaskError("Invalid account currency or compliance rejection")

        # Ensure account exists or initialize it
        account = await session.get(Account, account_id)
        if not account:
            account = Account(id=account_id, balance=0.0)
            session.add(account)
            await session.flush()

        applied_count = 0
        skipped_duplicate_count = 0
        balance_delta = 0.0

        for item in transactions:
            tx_id = item.get("transaction_id")
            amount = item.get("amount")
            operation = item.get("operation", "CREDIT").upper()

            if not tx_id or amount is None:
                raise PermanentTaskError(f"Malformed transaction in batch: {item}")

            try:
                amount_val = float(amount)
            except (ValueError, TypeError) as err:
                raise PermanentTaskError(f"Invalid amount value '{amount}'") from err

            # Business Idempotency Check:
            # Check if this transaction_id has already been journaled
            existing_tx = await session.scalar(
                select(LedgerEntry).where(LedgerEntry.transaction_id == tx_id)
            )

            if existing_tx:
                # Already processed! Skip to prevent double counting
                skipped_duplicate_count += 1
                continue

            # Record ledger journal entry
            entry = LedgerEntry(
                account_id=account_id,
                transaction_id=tx_id,
                amount=amount_val,
                operation=operation,
            )
            session.add(entry)

            if operation == "CREDIT":
                account.balance += amount_val
                balance_delta += amount_val
            elif operation == "DEBIT":
                account.balance -= amount_val
                balance_delta -= amount_val
            else:
                raise PermanentTaskError(f"Unsupported operation '{operation}'")

            applied_count += 1

        await session.flush()

        return {
            "account_id": account_id,
            "starting_balance": round(account.balance - balance_delta, 2),
            "final_balance": round(account.balance, 2),
            "total_transactions": len(transactions),
            "applied_count": applied_count,
            "skipped_duplicate_count": skipped_duplicate_count,
            "idempotent_safe": True,
        }

