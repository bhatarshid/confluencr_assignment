"""Pydantic schemas for request validation and response serialization."""

from datetime import datetime, timezone
from decimal import Decimal
import re
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


class TransactionWebhookRequest(BaseModel):
    """Schema for POST /v1/webhooks/transactions payload."""

    model_config = ConfigDict(str_strip_whitespace=True)

    transaction_id: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Unique transaction identifier",
        examples=["txn_abc123def456"],
    )
    source_account: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Source account identifier",
        examples=["acc_user_789"],
    )
    destination_account: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Destination account identifier",
        examples=["acc_merchant_456"],
    )
    amount: Decimal = Field(
        ...,
        gt=0,
        description="Positive monetary transaction amount",
        examples=[1500],
    )
    currency: str = Field(
        ...,
        min_length=3,
        max_length=3,
        description="3-letter currency code (e.g. INR)",
        examples=["INR"],
    )

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: Decimal) -> Decimal:
        """Ensure amount is finite, positive, and has reasonable precision."""
        if not v.is_finite():
            raise ValueError("Amount must be a finite positive number.")
        if v <= 0:
            raise ValueError("Amount must be strictly greater than 0.")
        return v

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        """Ensure currency is valid 3-letter alphabetic ISO code."""
        code = v.strip().upper()
        if not re.match(r"^[A-Z]{3}$", code):
            raise ValueError("Currency must be a valid 3-letter alphabetic currency code (e.g. INR, USD).")
        return code


class TransactionWebhookResponse(BaseModel):
    """Schema for HTTP 202 Accepted webhook ingestion response."""

    status: str = Field(default="ACCEPTED", description="Processing status")
    message: str = Field(
        default="Transaction accepted for asynchronous processing",
        description="Human-readable response message",
    )
    transaction_id: str = Field(description="Ingested transaction identifier")


class TransactionStatusResponse(BaseModel):
    """Schema for GET /v1/transactions/{transaction_id} response."""

    model_config = ConfigDict(from_attributes=True)

    transaction_id: str = Field(description="Unique transaction identifier")
    source_account: str = Field(description="Debited source account")
    destination_account: str = Field(description="Credited destination account")
    amount: Decimal = Field(description="Monetary transaction amount")
    currency: str = Field(description="Transaction currency code")
    status: str = Field(description="Transaction lifecycle status (PROCESSING or PROCESSED)")
    created_at: datetime = Field(description="UTC timestamp when accepted")
    processed_at: Optional[datetime] = Field(
        default=None,
        description="UTC timestamp when processed, null while PROCESSING",
    )

    @field_serializer("amount")
    def serialize_amount(self, v: Decimal) -> float | int:
        """Serialize amount cleanly as integer if whole number or float if decimal."""
        if v % 1 == 0:
            return int(v)
        return float(v)

    @field_serializer("created_at")
    def serialize_created_at(self, dt: datetime) -> str:
        """Format created_at as standard ISO 8601 UTC timestamp."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @field_serializer("processed_at")
    def serialize_processed_at(self, dt: Optional[datetime]) -> Optional[str]:
        """Format processed_at as standard ISO 8601 UTC timestamp or null."""
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
