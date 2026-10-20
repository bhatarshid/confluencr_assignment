"""SQLAlchemy models for existing Supabase tables in confluencr_asgnmt schema."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

SCHEMA_NAME = "confluencr_asgnmt"


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""
    pass


class Transaction(Base):
    """Maps to existing confluencr_asgnmt.transactions table."""

    __tablename__ = "transactions"
    __table_args__ = (
        UniqueConstraint("transaction_id", name="uq_transactions_transaction_id"),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        Integer().with_variant(BigInteger, "postgresql"),
        primary_key=True,
        autoincrement=True,
    )
    transaction_id: Mapped[str] = mapped_column(
        String(128),
        unique=True,
        nullable=False,
        index=True,
        doc="External alphanumeric transaction reference (e.g. txn_abc123def456)",
    )
    source_account: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        doc="Identifier of the debited source account (e.g. acc_user_789)",
    )
    destination_account: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        doc="Identifier of the credited destination account (e.g. acc_merchant_456)",
    )
    amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 4),
        nullable=False,
        doc="Monetary transaction amount with fixed precision",
    )
    currency: Mapped[str] = mapped_column(
        String(3),
        nullable=False,
        doc="3-letter ISO currency code (e.g. INR)",
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="PROCESSING",
        doc="Lifecycle status: PROCESSING or PROCESSED",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        doc="Time when the transaction was accepted and recorded",
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
        doc="Time when background processing finished; null until completed",
    )

    # Relationship to background processing job
    processing_job = relationship("ProcessingJob", back_populates="transaction", uselist=False, cascade="all, delete-orphan")


class ProcessingJob(Base):
    """Maps to existing confluencr_asgnmt.processing_jobs table for Phase 3 background tasks."""

    __tablename__ = "processing_jobs"
    __table_args__ = (
        UniqueConstraint("transaction_id", name="uq_processing_jobs_transaction_id"),
        Index("idx_processing_jobs_claim", "status", "available_at"),
        {"schema": SCHEMA_NAME},
    )

    id: Mapped[int] = mapped_column(
        Integer().with_variant(BigInteger, "postgresql"),
        primary_key=True,
        autoincrement=True,
    )
    transaction_id: Mapped[int] = mapped_column(
        Integer().with_variant(BigInteger, "postgresql"),
        ForeignKey(f"{SCHEMA_NAME}.transactions.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
        index=True,
        doc="Foreign key reference to transactions.id",
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="PENDING",
        doc="Job state: PENDING, RUNNING, COMPLETED, FAILED",
    )
    attempts: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False,
        default=0,
        doc="Number of execution attempts",
    )
    available_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Earliest time this job may be claimed by a worker",
    )
    locked_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Time when worker claimed the job lock",
    )
    last_error: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Last error message if job execution failed",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    transaction = relationship("Transaction", back_populates="processing_job")
