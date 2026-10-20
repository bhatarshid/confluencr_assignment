"""Automated unit and integration tests for Phase 3 background worker, retries, and transitions."""

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models.transaction import ProcessingJob, Transaction
from app.schemas.transaction import TransactionWebhookRequest
from app.services.transaction_service import TransactionService
from app.services.worker_service import WorkerService
from app.worker import TransactionWorker


@pytest.fixture
def worker_service() -> WorkerService:
    """WorkerService with fast test settings."""
    test_settings = Settings(
        DATABASE_URL=None,
        WORKER_POLL_INTERVAL_SECONDS=0.01,
        WORKER_PROCESSING_DELAY_SECONDS=0.05,
        WORKER_MAX_ATTEMPTS=3,
        WORKER_RETRY_BASE_SECONDS=1.0,
        WORKER_STALE_LOCK_SECONDS=10.0,
        WORKER_BATCH_SIZE=5,
    )
    return WorkerService(settings=test_settings)


def create_test_transaction_and_job(
    db: Session,
    txn_id: str = "txn_worker_test_01",
    job_status: str = "PENDING",
    available_at: datetime | None = None,
    locked_at: datetime | None = None,
    attempts: int = 0,
) -> tuple[Transaction, ProcessingJob]:
    """Helper to seed a transaction and associated processing job."""
    now = datetime.now(timezone.utc)
    txn = Transaction(
        transaction_id=txn_id,
        source_account="acc_src",
        destination_account="acc_dest",
        amount=Decimal("500"),
        currency="INR",
        status="PROCESSING",
        created_at=now,
        processed_at=None,
    )
    db.add(txn)
    db.flush()

    job = ProcessingJob(
        transaction_id=txn.id,
        status=job_status,
        attempts=attempts,
        available_at=available_at or now,
        locked_at=locked_at,
        created_at=now,
    )
    db.add(job)
    db.commit()
    db.refresh(txn)
    db.refresh(job)
    return txn, job


def test_claim_pending_job_success(db_session: Session, worker_service: WorkerService):
    """Test 1: Eligible PENDING job is claimed, status becomes PROCESSING, and attempts increments."""
    txn, job = create_test_transaction_and_job(db_session, "txn_claim_01")

    claimed_ids = worker_service.claim_jobs(db_session)
    assert job.id in claimed_ids

    db_session.refresh(job)
    assert job.status == "PROCESSING"
    assert job.locked_at is not None
    assert job.attempts == 1


def test_future_available_at_job_not_claimed_early(db_session: Session, worker_service: WorkerService):
    """Test 2: Job with future available_at is not claimed before its schedule."""
    future_time = datetime.now(timezone.utc) + timedelta(minutes=10)
    txn, job = create_test_transaction_and_job(db_session, "txn_future_01", available_at=future_time)

    claimed_ids = worker_service.claim_jobs(db_session)
    assert job.id not in claimed_ids

    db_session.refresh(job)
    assert job.status == "PENDING"
    assert job.locked_at is None
    assert job.attempts == 0


def test_already_processing_job_not_claimed(db_session: Session, worker_service: WorkerService):
    """Test 3: Job in PROCESSING state is skipped by subsequent claim calls."""
    now = datetime.now(timezone.utc)
    txn, job = create_test_transaction_and_job(db_session, "txn_proc_01", job_status="PROCESSING", locked_at=now)

    claimed_ids = worker_service.claim_jobs(db_session)
    assert job.id not in claimed_ids


def test_completed_job_not_claimed(db_session: Session, worker_service: WorkerService):
    """Test 4: Job in COMPLETED state is never claimed."""
    txn, job = create_test_transaction_and_job(db_session, "txn_done_01", job_status="COMPLETED")

    claimed_ids = worker_service.claim_jobs(db_session)
    assert job.id not in claimed_ids


def test_job_execution_transitions_transaction_to_processed(db_session: Session, worker_service: WorkerService):
    """Test 5-9: Successful job execution updates transaction to PROCESSED and job to COMPLETED."""
    txn, job = create_test_transaction_and_job(db_session, "txn_exec_01")

    # Claim job
    claimed = worker_service.claim_jobs(db_session)
    assert job.id in claimed

    # Execute with test delay of 0.05s
    start_time = datetime.now(timezone.utc)
    with patch("app.services.worker_service.session_scope") as mock_scope:
        from contextlib import contextmanager

        @contextmanager
        def scope():
            yield db_session

        mock_scope.side_effect = scope

        success = asyncio.run(worker_service.execute_job(job.id, delay_seconds=0.05))
        assert success is True

    db_session.refresh(job)
    db_session.refresh(txn)

    # Verify job completion
    assert job.status == "COMPLETED"
    assert job.locked_at is None
    assert job.last_error is None

    # Verify transaction completion
    assert txn.status == "PROCESSED"
    assert txn.processed_at is not None
    processed_at = txn.processed_at.replace(tzinfo=timezone.utc) if txn.processed_at.tzinfo is None else txn.processed_at
    assert processed_at >= start_time


def test_retryable_failure_schedules_backoff(db_session: Session, worker_service: WorkerService):
    """Test 10-12: Transient failure resets job to PENDING with exponential backoff."""
    txn, job = create_test_transaction_and_job(db_session, "txn_retry_01", attempts=1)

    now = datetime.now(timezone.utc)
    worker_service.fail_or_retry_job(db_session, job.id, "Payment gateway timeout")

    db_session.refresh(job)
    assert job.status == "PENDING"
    assert job.locked_at is None
    assert "timeout" in job.last_error
    # Attempt 1 backoff = 1.0 * (2^0) = 1.0s
    available_at = job.available_at.replace(tzinfo=timezone.utc) if job.available_at.tzinfo is None else job.available_at
    assert available_at > now


def test_exhausted_attempts_marks_job_failed_and_preserves_transaction(db_session: Session, worker_service: WorkerService):
    """Test 13-14: Max attempts reached marks job FAILED and leaves transaction in PROCESSING."""
    txn, job = create_test_transaction_and_job(db_session, "txn_fail_01", attempts=3)

    worker_service.fail_or_retry_job(db_session, job.id, "Terminal provider error")

    db_session.refresh(job)
    db_session.refresh(txn)

    assert job.status == "FAILED"
    assert job.locked_at is None
    assert "Max attempts (3) reached" in job.last_error

    # Transaction must NOT be marked PROCESSED
    assert txn.status == "PROCESSING"
    assert txn.processed_at is None


def test_stale_job_recovery(db_session: Session, worker_service: WorkerService):
    """Test 15-16: Stale locked job is recovered, while recent lock is preserved."""
    now = datetime.now(timezone.utc)

    # 1. Stale job (locked 20 seconds ago, threshold is 10 seconds)
    stale_time = now - timedelta(seconds=20)
    txn_stale, job_stale = create_test_transaction_and_job(
        db_session, "txn_stale_01", job_status="PROCESSING", locked_at=stale_time, attempts=1
    )

    # 2. Recent active job (locked 2 seconds ago)
    recent_time = now - timedelta(seconds=2)
    txn_active, job_active = create_test_transaction_and_job(
        db_session, "txn_active_01", job_status="PROCESSING", locked_at=recent_time, attempts=1
    )

    recovered_count = worker_service.recover_stale_jobs(db_session)
    assert recovered_count == 1

    db_session.refresh(job_stale)
    db_session.refresh(job_active)

    # Stale job recovered back to PENDING
    assert job_stale.status == "PENDING"
    assert job_stale.locked_at is None
    assert "Recovered from stale lock" in job_stale.last_error

    # Active job untouched
    assert job_active.status == "PROCESSING"
    assert job_active.locked_at is not None


def test_duplicate_webhook_does_not_requeue_completed_job(db_session: Session):
    """Test 18-19: Ingesting an identical webhook when a job is already COMPLETED does not reset it to PENDING."""
    txn_service = TransactionService()
    payload = TransactionWebhookRequest(
        transaction_id="txn_requeue_test_01",
        source_account="acc_src",
        destination_account="acc_dest",
        amount=Decimal("1500"),
        currency="INR",
    )

    # Ingest initially
    txn, is_dup = txn_service.ingest_transaction(db_session, payload)
    assert not is_dup

    # Simulate job completed
    job = db_session.execute(
        select(ProcessingJob).where(ProcessingJob.transaction_id == txn.id)
    ).scalar_one()
    job.status = "COMPLETED"
    txn.status = "PROCESSED"
    db_session.commit()

    # Redeliver identical webhook
    txn2, is_dup2 = txn_service.ingest_transaction(db_session, payload)
    assert is_dup2 is True

    # Verify job remained COMPLETED and was not re-queued
    db_session.refresh(job)
    assert job.status == "COMPLETED"
    assert txn2.status == "PROCESSED"


def test_worker_loop_single_iteration(db_session: Session, worker_service: WorkerService):
    """Test worker loop processes a job within a controlled single iteration."""
    txn, job = create_test_transaction_and_job(db_session, "txn_worker_iter_01")

    worker = TransactionWorker(settings=worker_service.settings)
    worker.service = worker_service

    from contextlib import contextmanager

    @contextmanager
    def scope():
        yield db_session

    with patch("app.services.worker_service.session_scope", side_effect=scope), \
         patch("app.worker.session_scope", side_effect=scope):
        asyncio.run(worker.run(max_iterations=1))

    db_session.refresh(job)
    db_session.refresh(txn)

    assert job.status == "COMPLETED"
    assert txn.status == "PROCESSED"
    assert txn.processed_at is not None


def test_e2e_webhook_ingestion_and_worker_completion(
    client,
    db_session: Session,
    worker_service: WorkerService,
):
    """End-to-end test: Webhook accepted -> PROCESSING -> Worker runs -> PROCESSED."""
    txn_id = "txn_e2e_full_lifecycle_01"
    payload = {
        "transaction_id": txn_id,
        "source_account": "acc_client_1",
        "destination_account": "acc_vendor_2",
        "amount": 2500,
        "currency": "INR",
    }

    # 1. Ingest via API
    post_resp = client.post("/v1/webhooks/transactions", json=payload)
    assert post_resp.status_code == 202

    # 2. Verify immediate status is PROCESSING with null processed_at
    get_resp_before = client.get(f"/v1/transactions/{txn_id}")
    assert get_resp_before.status_code == 200
    assert get_resp_before.json()["status"] == "PROCESSING"
    assert get_resp_before.json()["processed_at"] is None

    # 3. Run worker
    worker = TransactionWorker(settings=worker_service.settings)
    worker.service = worker_service

    from contextlib import contextmanager

    @contextmanager
    def scope():
        yield db_session

    with patch("app.services.worker_service.session_scope", side_effect=scope), \
         patch("app.worker.session_scope", side_effect=scope):
        asyncio.run(worker.run(max_iterations=1))

    # 4. Verify status transitioned to PROCESSED with populated processed_at
    get_resp_after = client.get(f"/v1/transactions/{txn_id}")
    assert get_resp_after.status_code == 200
    after_data = get_resp_after.json()
    assert after_data["status"] == "PROCESSED"
    assert after_data["processed_at"] is not None
    assert after_data["processed_at"].endswith("Z")
