# 04 - Idempotency and Duplicate Delivery

## 1. What Problem This Module Solves
In distributed systems, the network is fundamentally unreliable. Retries at the HTTP layer, broker acknowledgments dropped due to network partitions, and worker crashes cause the same message to be submitted or delivered more than once.

Without idempotency, repeated execution causes critical business bugs: double charges on credit cards, duplicate inventory deductions, or corrupt ledger balances.

## 2. Three Distinct Guarantees (Often Confused in Interviews)

| Guarantee Level | Where It Operates | Mechanism in This Architecture | What It Solves |
|---|---|---|---|
| **1. Submission Idempotency** | HTTP API Layer | Database Unique Constraint on `idempotency_key` | Prevents a client from creating duplicate tasks when retrying an HTTP request after a client-side timeout. |
| **2. Broker Duplicate Delivery Tolerance** | Consumer / Worker | Status check (`task.status == SUCCEEDED`) before processing | Prevents RabbitMQ at-least-once redeliveries from re-running an already finished task. |
| **3. Business Effect Idempotency** | Handler Business Logic | Journal table with unique `transaction_id` (`LedgerEntry`) | Prevents repeated operations within the handler from double-crediting an account balance even if the handler runs multiple times. |

## 3. Demonstrating Business Effect Idempotency: The Ledger Handler
In [src/taskqueue/handlers/ledger.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/src/taskqueue/handlers/ledger.py), we implement an account ledger batch processor:
1. The payload specifies an `account_id` and an array of financial transactions:
   ```json
   {
     "account_id": "acc-100",
     "transactions": [
       {"transaction_id": "tx-999", "amount": 250.0, "operation": "CREDIT"}
     ]
   }
   ```
2. When the handler runs:
   - For every transaction, it checks if `transaction_id` already exists in `ledger_entries`.
   - If `transaction_id` exists: it **skips** applying the credit and increments `skipped_duplicate_count`.
   - If not found: it inserts `LedgerEntry(transaction_id="tx-999", ...)` and increments `account.balance += 250.0`.
3. If the worker crashes right after committing the database transaction but before sending `msg.ack()` to RabbitMQ:
   - RabbitMQ will re-deliver the message to another worker.
   - The second worker receives the message and executes the handler.
   - The handler inspects `ledger_entries`, finds `"tx-999"` already recorded, skips the balance increment, and reports `skipped_duplicate_count: 1`.
   - **Result**: The final account balance is exactly `$250.00`, never `$500.00`!

## 4. Why We Do Not Claim Universal Exactly-Once Processing
True "exactly-once execution" across disparate external systems (e.g. sending an email, charging a third-party Stripe API, writing to a database) is mathematically impossible without distributed consensus and idempotent APIs on the receiver. 

Our system guarantees:
- **At-least-once message delivery** via RabbitMQ persistent messages and manual ACKs.
- **Effectively exactly-once business outcome** via deduplication tokens, state guards, and database transactions.

## 5. Relevant Tests
- [tests/failure/test_idempotency_duplicate_delivery.py](file:///c:/Users/bhanu/Music/Complete%20Desktop/Distributed_sysstem_project/tests/failure/test_idempotency_duplicate_delivery.py): Deliberately delivers the same message twice and verifies the account balance is never double-credited.

