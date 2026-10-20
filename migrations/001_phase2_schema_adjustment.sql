-- ====================================================================
-- Confluencr / Walnut Folks Group Assessment - Phase 2 Schema Adjustment
-- Target Schema: confluencr_asgnmt
-- Description: Non-destructive migration to support alphanumeric transaction
--              and account IDs and enforce database-level idempotency constraints.
-- ====================================================================

-- 1. Add transaction_id column to transactions if it doesn't exist
ALTER TABLE confluencr_asgnmt.transactions 
    ADD COLUMN IF NOT EXISTS transaction_id VARCHAR(128);

-- 2. Add unique constraint on transaction_id for reliable database idempotency
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'uq_transactions_transaction_id'
    ) THEN
        ALTER TABLE confluencr_asgnmt.transactions
            ADD CONSTRAINT uq_transactions_transaction_id UNIQUE (transaction_id);
    END IF;
END $$;

-- 3. Update account identifiers to VARCHAR(128) for strings like 'acc_user_789'
--    and amount to NUMERIC(18, 4) for exact monetary precision
ALTER TABLE confluencr_asgnmt.transactions 
    ALTER COLUMN source_account TYPE VARCHAR(128) USING source_account::text,
    ALTER COLUMN destination_account TYPE VARCHAR(128) USING destination_account::text,
    ALTER COLUMN amount TYPE NUMERIC(18, 4) USING amount::numeric;

-- 4. Add unique constraint on processing_jobs.transaction_id to prevent duplicate job creation
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'uq_processing_jobs_transaction_id'
    ) THEN
        ALTER TABLE confluencr_asgnmt.processing_jobs
            ADD CONSTRAINT uq_processing_jobs_transaction_id UNIQUE (transaction_id);
    END IF;
END $$;
