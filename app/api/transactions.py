"""Transaction status retrieval API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.logging_config import get_logger
from app.schemas.transaction import TransactionStatusResponse
from app.services.transaction_service import (
    TransactionService,
    get_transaction_service,
)

logger = get_logger("api.transactions")
router = APIRouter(prefix="/v1/transactions", tags=["Transactions"])


@router.get(
    "/{transaction_id}",
    response_model=TransactionStatusResponse,
    summary="Get Transaction Status",
    description="Retrieves the persisted status, timestamps, and details for a given transaction ID.",
    status_code=status.HTTP_200_OK,
)
def get_transaction_status(
    transaction_id: str,
    db: Session = Depends(get_db),
    service: TransactionService = Depends(get_transaction_service),
) -> TransactionStatusResponse:
    """Lookup transaction record by external transaction ID."""
    txn = service.get_transaction_by_id(db=db, transaction_id=transaction_id)
    if txn is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "NotFound",
                "message": f"Transaction '{transaction_id}' was not found.",
            },
        )

    return TransactionStatusResponse.model_validate(txn)
