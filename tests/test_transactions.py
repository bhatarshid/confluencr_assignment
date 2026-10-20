"""Tests for transaction webhook ingestion, status retrieval, idempotency, and performance."""

import concurrent.futures
from decimal import Decimal
import time
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.transaction import ProcessingJob, Transaction

SAMPLE_WEBHOOK_PAYLOAD = {
    "transaction_id": "txn_abc123def456",
    "source_account": "acc_user_789",
    "destination_account": "acc_merchant_456",
    "amount": 1500,
    "currency": "INR",
}


def test_valid_webhook_returns_202_and_persists_once(client: TestClient, db_session: Session):
    """Test webhook accepts valid payload, returns 202, and persists transaction with status PROCESSING."""
    response = client.post("/v1/webhooks/transactions", json=SAMPLE_WEBHOOK_PAYLOAD)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "ACCEPTED"
    assert data["transaction_id"] == SAMPLE_WEBHOOK_PAYLOAD["transaction_id"]
    assert "asynchronous processing" in data["message"]

    # Verify database persistence
    saved_txn = db_session.execute(
        select(Transaction).where(Transaction.transaction_id == SAMPLE_WEBHOOK_PAYLOAD["transaction_id"])
    ).scalar_one_or_none()

    assert saved_txn is not None
    assert saved_txn.status == "PROCESSING"
    assert saved_txn.source_account == "acc_user_789"
    assert saved_txn.destination_account == "acc_merchant_456"
    assert saved_txn.amount == Decimal("1500")
    assert saved_txn.currency == "INR"
    assert saved_txn.created_at is not None
    assert saved_txn.processed_at is None

    # Verify corresponding processing job created for Phase 3
    job = db_session.execute(
        select(ProcessingJob).where(ProcessingJob.transaction_id == saved_txn.id)
    ).scalar_one_or_none()
    assert job is not None
    assert job.status == "PENDING"


def test_missing_or_invalid_fields_return_422(client: TestClient):
    """Test validation errors for missing or empty fields."""
    # Missing transaction_id
    payload = {
        "source_account": "acc_1",
        "destination_account": "acc_2",
        "amount": 100,
        "currency": "INR",
    }
    assert client.post("/v1/webhooks/transactions", json=payload).status_code == 422

    # Empty string transaction_id
    payload["transaction_id"] = ""
    assert client.post("/v1/webhooks/transactions", json=payload).status_code == 422


def test_invalid_amount_rejected(client: TestClient):
    """Test non-positive or non-finite amounts return HTTP 422."""
    payload = dict(SAMPLE_WEBHOOK_PAYLOAD)

    # Zero amount
    payload["amount"] = 0
    assert client.post("/v1/webhooks/transactions", json=payload).status_code == 422

    # Negative amount
    payload["amount"] = -50
    assert client.post("/v1/webhooks/transactions", json=payload).status_code == 422

    # String non-numeric amount
    payload["amount"] = "invalid"
    assert client.post("/v1/webhooks/transactions", json=payload).status_code == 422


def test_invalid_currency_rejected(client: TestClient):
    """Test currency validation rejects non-3-letter or invalid currencies."""
    payload = dict(SAMPLE_WEBHOOK_PAYLOAD)

    # 2-letter
    payload["currency"] = "IN"
    assert client.post("/v1/webhooks/transactions", json=payload).status_code == 422

    # 4-letter
    payload["currency"] = "INRA"
    assert client.post("/v1/webhooks/transactions", json=payload).status_code == 422

    # Numeric currency code
    payload["currency"] = "356"
    assert client.post("/v1/webhooks/transactions", json=payload).status_code == 422


def test_repeated_identical_webhook_returns_202_and_no_duplicate(client: TestClient, db_session: Session):
    """Test idempotency: identical duplicate webhook returns 202 without creating duplicate record."""
    resp1 = client.post("/v1/webhooks/transactions", json=SAMPLE_WEBHOOK_PAYLOAD)
    assert resp1.status_code == 202

    resp2 = client.post("/v1/webhooks/transactions", json=SAMPLE_WEBHOOK_PAYLOAD)
    assert resp2.status_code == 202
    assert resp2.json()["status"] == "ACCEPTED"
    assert "currently processing" in resp2.json()["message"]

    # Assert exactly 1 transaction record exists
    count = len(
        db_session.execute(
            select(Transaction).where(Transaction.transaction_id == SAMPLE_WEBHOOK_PAYLOAD["transaction_id"])
        ).scalars().all()
    )
    assert count == 1


def test_conflicting_duplicate_payload_returns_409_conflict(client: TestClient, db_session: Session):
    """Test that a duplicate transaction_id with different amount/accounts returns 409 Conflict without overwriting."""
    resp1 = client.post("/v1/webhooks/transactions", json=SAMPLE_WEBHOOK_PAYLOAD)
    assert resp1.status_code == 202

    # Conflicting payload with different amount
    conflicting_payload = dict(SAMPLE_WEBHOOK_PAYLOAD)
    conflicting_payload["amount"] = 9999

    resp2 = client.post("/v1/webhooks/transactions", json=conflicting_payload)
    assert resp2.status_code == 409
    data = resp2.json()
    assert "ConflictingTransaction" in str(data)

    # Verify original record was NOT overwritten
    saved = db_session.execute(
        select(Transaction).where(Transaction.transaction_id == SAMPLE_WEBHOOK_PAYLOAD["transaction_id"])
    ).scalar_one()
    assert saved.amount == Decimal("1500")


def test_concurrent_duplicate_requests_produce_one_record(client: TestClient, db_session: Session):
    """Test concurrent duplicate webhooks are safely resolved by database uniqueness."""
    payload = dict(SAMPLE_WEBHOOK_PAYLOAD)
    payload["transaction_id"] = "txn_concurrent_test_999"

    def send_request():
        return client.post("/v1/webhooks/transactions", json=payload)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(send_request) for _ in range(5)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    # All responses should be HTTP 202 Accepted
    for r in results:
        assert r.status_code == 202

    # Exactly one record exists in database
    records = db_session.execute(
        select(Transaction).where(Transaction.transaction_id == "txn_concurrent_test_999")
    ).scalars().all()
    assert len(records) == 1


def test_get_transaction_status_success(client: TestClient):
    """Test GET /v1/transactions/{transaction_id} returns 200 with full details for existing transaction."""
    # Ingest transaction first
    client.post("/v1/webhooks/transactions", json=SAMPLE_WEBHOOK_PAYLOAD)

    response = client.get(f"/v1/transactions/{SAMPLE_WEBHOOK_PAYLOAD['transaction_id']}")
    assert response.status_code == 200
    data = response.json()

    assert data["transaction_id"] == SAMPLE_WEBHOOK_PAYLOAD["transaction_id"]
    assert data["source_account"] == SAMPLE_WEBHOOK_PAYLOAD["source_account"]
    assert data["destination_account"] == SAMPLE_WEBHOOK_PAYLOAD["destination_account"]
    assert data["amount"] == 1500
    assert data["currency"] == "INR"
    assert data["status"] == "PROCESSING"
    assert data["processed_at"] is None
    assert "created_at" in data
    assert data["created_at"].endswith("Z")


def test_get_unknown_transaction_returns_404(client: TestClient):
    """Test GET /v1/transactions/{transaction_id} returns 404 for nonexistent transaction."""
    response = client.get("/v1/transactions/txn_nonexistent_xyz")
    assert response.status_code == 404
    data = response.json()
    assert "NotFound" in str(data)


def test_database_failure_does_not_return_false_success(client: TestClient):
    """Test that database failure during ingestion returns HTTP 500 without false success."""
    with patch("app.services.transaction_service.TransactionService.ingest_transaction") as mock_ingest:
        mock_ingest.side_effect = Exception("Database connection lost")
        response = client.post("/v1/webhooks/transactions", json=SAMPLE_WEBHOOK_PAYLOAD)
        assert response.status_code == 500
        assert "PersistenceError" in str(response.json())


def test_webhook_response_time_under_500ms(client: TestClient):
    """Test performance requirement: webhook response time must target < 500ms."""
    payload = dict(SAMPLE_WEBHOOK_PAYLOAD)
    payload["transaction_id"] = "txn_perf_measurement_001"

    start = time.perf_counter()
    response = client.post("/v1/webhooks/transactions", json=payload)
    elapsed_ms = (time.perf_counter() - start) * 1000

    assert response.status_code == 202
    assert elapsed_ms < 500, f"Webhook took {elapsed_ms}ms, expected under 500ms"
