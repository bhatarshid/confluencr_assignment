"""Transaction webhook ingestion API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.logging_config import get_logger
from app.schemas.transaction import (
    TransactionWebhookRequest,
    TransactionWebhookResponse,
)
from app.services.transaction_service import (
    ConflictingTransactionError,
    TransactionService,
    get_transaction_service,
)

logger = get_logger("api.webhooks")
router = APIRouter(prefix="/v1/webhooks", tags=["Webhooks"])


@router.post(
    "/transactions",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TransactionWebhookResponse,
    summary="Receive Transaction Webhook",
    description="Accepts and persists transaction webhooks asynchronously. Returns HTTP 202 Accepted within 500ms.",
)
def receive_transaction_webhook(
    payload: TransactionWebhookRequest,
    db: Session = Depends(get_db),
    service: TransactionService = Depends(get_transaction_service),
) -> TransactionWebhookResponse:
    """Ingest an external payment processor webhook."""
    try:
        txn, is_duplicate = service.ingest_transaction(db=db, payload=payload)
    except ConflictingTransactionError as conflict_err:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "ConflictingTransaction",
                "message": str(conflict_err),
            },
        )
    except Exception as exc:
        logger.exception("Failed to ingest transaction '%s': %s", payload.transaction_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "PersistenceError",
                "message": "Failed to persist transaction. Please retry.",
            },
        )

    msg = (
        "Transaction accepted for asynchronous processing"
        if not is_duplicate
        else "Transaction already accepted and currently processing"
    )

    return TransactionWebhookResponse(
        status="ACCEPTED",
        message=msg,
        transaction_id=txn.transaction_id,
    )
