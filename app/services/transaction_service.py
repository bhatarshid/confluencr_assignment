"""Service layer encapsulating transaction ingestion, idempotency, and status retrieval."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Tuple

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.logging_config import get_logger
from app.models.transaction import ProcessingJob, Transaction
from app.schemas.transaction import TransactionWebhookRequest

logger = get_logger("transaction_service")


class ConflictingTransactionError(Exception):
    """Raised when an incoming webhook provides a repeated transaction_id with conflicting payload data."""
    pass


class TransactionService:
    """Handles persistence, idempotency verification, and querying of transactions."""

    def ingest_transaction(
        self,
        db: Session,
        payload: TransactionWebhookRequest,
    ) -> Tuple[Transaction, bool]:
        """Ingest a transaction webhook.

        Uses the database unique constraint as the final authority for idempotency.
        Handles concurrent insertions safely via savepoints.

        Returns:
            Tuple[Transaction, bool]: (transaction, is_duplicate)

        Raises:
            ConflictingTransactionError: If transaction_id exists with differing details.
        """
        now = datetime.now(timezone.utc)
        new_txn = Transaction(
            transaction_id=payload.transaction_id,
            source_account=payload.source_account,
            destination_account=payload.destination_account,
            amount=payload.amount,
            currency=payload.currency,
            status="PROCESSING",
            created_at=now,
            processed_at=None,
        )

        try:
            db.add(new_txn)
            db.flush()

            # Insert initial processing job record for Phase 3 background workers
            job = ProcessingJob(
                transaction_id=new_txn.id,
                status="PENDING",
                attempts=0,
                available_at=now,
                created_at=now,
            )
            db.add(job)
            db.commit()
            db.refresh(new_txn)
            logger.info("Successfully ingested new transaction: %s", payload.transaction_id)
            return new_txn, False

        except IntegrityError as exc:
            db.rollback()
            logger.info(
                "Duplicate transaction_id detected for '%s'; verifying payload idempotency.",
                payload.transaction_id,
            )

            # Query the existing transaction from DB
            existing_txn = db.execute(
                select(Transaction).where(Transaction.transaction_id == payload.transaction_id)
            ).scalar_one_or_none()

            if existing_txn is None:
                # If constraint violation was on another field or unexpected error, re-raise
                logger.error("IntegrityError occurred but record could not be retrieved: %s", exc)
                raise

            # Verify that payload fields match the existing record
            # Compare amounts using Decimal equality
            amounts_match = Decimal(str(existing_txn.amount)) == Decimal(str(payload.amount))
            is_identical = (
                existing_txn.source_account == payload.source_account
                and existing_txn.destination_account == payload.destination_account
                and amounts_match
                and existing_txn.currency == payload.currency
            )

            if not is_identical:
                logger.warning(
                    "Conflicting duplicate payload received for transaction '%s'. Rejecting.",
                    payload.transaction_id,
                )
                raise ConflictingTransactionError(
                    f"Transaction ID '{payload.transaction_id}' already exists with conflicting transaction details."
                )

            logger.info("Identical duplicate transaction acknowledged: %s", payload.transaction_id)
            return existing_txn, True

    def get_transaction_by_id(self, db: Session, transaction_id: str) -> Optional[Transaction]:
        """Retrieve a transaction by its external transaction_id."""
        stmt = select(Transaction).where(Transaction.transaction_id == transaction_id)
        return db.execute(stmt).scalar_one_or_none()


def get_transaction_service() -> TransactionService:
    """Dependency provider for TransactionService."""
    return TransactionService()
