"""Worker service encapsulating durable job claiming, execution, retries, and stale-job recovery."""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import time
from typing import List, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.database import session_scope
from app.logging_config import get_logger
from app.models.transaction import ProcessingJob, Transaction

logger = get_logger("worker_service")


def sanitize_error_message(err: Exception | str, max_length: int = 500) -> str:
    """Sanitize error messages to prevent credential leaks and keep them concise."""
    raw = str(err)
    # Mask potential sensitive words
    for word in ("password", "secret", "token", "key"):
        if word in raw.lower():
            raw = f"{err.__class__.__name__}: [sensitive error details sanitized]"
            break
    if len(raw) > max_length:
        raw = raw[:max_length] + "..."
    return raw


class WorkerService:
    """Encapsulates job queue interactions using PostgreSQL row-level locking."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or get_settings()

    def claim_jobs(self, db: Session, batch_size: Optional[int] = None) -> List[int]:
        """Atomically claim eligible PENDING jobs with row-level locking.

        Releases row lock and transaction upon return so slow work does not hold DB locks.

        Returns:
            List[int]: List of claimed processing job IDs.
        """
        now = datetime.now(timezone.utc)
        limit = batch_size or self.settings.WORKER_BATCH_SIZE

        stmt = select(ProcessingJob).where(
            ProcessingJob.status == "PENDING",
            or_(
                ProcessingJob.available_at <= now,
                ProcessingJob.available_at.is_(None),
            ),
        ).order_by(ProcessingJob.created_at.asc()).limit(limit)

        # On PostgreSQL, use FOR UPDATE SKIP LOCKED to guarantee safe concurrent claiming across workers
        if db.bind and db.bind.dialect.name == "postgresql":
            stmt = stmt.with_for_update(skip_locked=True)

        jobs = db.execute(stmt).scalars().all()
        if not jobs:
            return []

        claimed_ids = []
        for job in jobs:
            job.status = "PROCESSING"
            job.locked_at = now
            job.attempts += 1
            claimed_ids.append(job.id)

        db.commit()
        logger.info("Claimed %d job(s): %s", len(claimed_ids), claimed_ids)
        return claimed_ids

    def complete_job(self, db: Session, job_id: int) -> bool:
        """Atomically complete the background job and transition transaction to PROCESSED.

        Both updates occur in a single atomic database transaction.
        """
        now = datetime.now(timezone.utc)
        job = db.execute(
            select(ProcessingJob).where(ProcessingJob.id == job_id)
        ).scalar_one_or_none()

        if job is None:
            logger.warning("Attempted to complete nonexistent job ID %d", job_id)
            return False

        if job.status == "COMPLETED":
            logger.info("Job ID %d is already completed (idempotent call)", job_id)
            return True

        txn = db.execute(
            select(Transaction).where(Transaction.id == job.transaction_id)
        ).scalar_one_or_none()

        if txn is None:
            logger.error("Job ID %d has no corresponding transaction ID %d", job_id, job.transaction_id)
            return False

        # Atomic state updates
        job.status = "COMPLETED"
        job.locked_at = None
        job.last_error = None

        txn.status = "PROCESSED"
        txn.processed_at = now

        db.commit()
        logger.info(
            "Job ID %d successfully completed. Transaction '%s' marked PROCESSED at %s",
            job.id,
            txn.transaction_id,
            now.isoformat(),
        )
        return True

    def fail_or_retry_job(self, db: Session, job_id: int, error: Exception | str) -> None:
        """Schedule a retry with exponential backoff or mark FAILED if max attempts exhausted."""
        now = datetime.now(timezone.utc)
        job = db.execute(
            select(ProcessingJob).where(ProcessingJob.id == job_id)
        ).scalar_one_or_none()

        if job is None:
            logger.warning("Attempted to fail/retry nonexistent job ID %d", job_id)
            return

        sanitized_err = sanitize_error_message(error)

        if job.attempts < self.settings.WORKER_MAX_ATTEMPTS:
            backoff_secs = self.settings.WORKER_RETRY_BASE_SECONDS * (2 ** (job.attempts - 1))
            job.status = "PENDING"
            job.available_at = now + timedelta(seconds=backoff_secs)
            job.locked_at = None
            job.last_error = sanitized_err
            logger.warning(
                "Job ID %d failed attempt %d/%d. Scheduled retry in %.1fs: %s",
                job.id,
                job.attempts,
                self.settings.WORKER_MAX_ATTEMPTS,
                backoff_secs,
                sanitized_err,
            )
        else:
            job.status = "FAILED"
            job.locked_at = None
            job.last_error = f"Max attempts ({self.settings.WORKER_MAX_ATTEMPTS}) reached: {sanitized_err}"
            logger.error(
                "Job ID %d reached maximum attempts (%d) and is marked FAILED: %s",
                job.id,
                self.settings.WORKER_MAX_ATTEMPTS,
                sanitized_err,
            )

        db.commit()

    def recover_stale_jobs(self, db: Session) -> int:
        """Reclaim in-progress jobs whose locks have expired due to worker crash or freeze."""
        now = datetime.now(timezone.utc)
        stale_threshold = now - timedelta(seconds=self.settings.WORKER_STALE_LOCK_SECONDS)

        stmt = select(ProcessingJob).where(
            ProcessingJob.status == "PROCESSING",
            ProcessingJob.locked_at < stale_threshold,
        )

        if db.bind and db.bind.dialect.name == "postgresql":
            stmt = stmt.with_for_update(skip_locked=True)

        stale_jobs = db.execute(stmt).scalars().all()
        if not stale_jobs:
            return 0

        recovered_count = 0
        for job in stale_jobs:
            if job.attempts >= self.settings.WORKER_MAX_ATTEMPTS:
                job.status = "FAILED"
                job.locked_at = None
                job.last_error = f"Stale lock expired; max attempts ({self.settings.WORKER_MAX_ATTEMPTS}) reached"
            else:
                job.status = "PENDING"
                job.available_at = now
                job.locked_at = None
                job.last_error = "Recovered from stale lock"
            recovered_count += 1

        db.commit()
        logger.warning("Recovered %d stale job(s) locked prior to %s", recovered_count, stale_threshold.isoformat())
        return recovered_count

    async def execute_job(
        self,
        job_id: int,
        delay_seconds: Optional[float] = None,
    ) -> bool:
        """Execute simulated external processing delay and commit completion atomically."""
        delay = (
            delay_seconds
            if delay_seconds is not None
            else self.settings.WORKER_PROCESSING_DELAY_SECONDS
        )

        logger.info("Starting processing for job ID %d (simulated external delay: %.1fs)", job_id, delay)
        start_time = time.perf_counter()

        try:
            # Cancellation-aware sleep simulating external payment gateway processing
            if delay > 0:
                await asyncio.sleep(delay)

            # Atomic completion in database
            with session_scope() as db:
                success = self.complete_job(db, job_id)

            duration = round(time.perf_counter() - start_time, 2)
            logger.info("Job ID %d completed in %.2fs", job_id, duration)
            return success

        except asyncio.CancelledError:
            logger.warning("Job ID %d cancelled during execution sleep.", job_id)
            with session_scope() as db:
                self.fail_or_retry_job(db, job_id, "Execution cancelled")
            raise

        except Exception as exc:
            logger.exception("Error executing job ID %d: %s", job_id, exc)
            with session_scope() as db:
                self.fail_or_retry_job(db, job_id, exc)
            return False


def get_worker_service() -> WorkerService:
    """Dependency provider for WorkerService."""
    return WorkerService()
