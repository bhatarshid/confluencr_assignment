-- ====================================================================
-- Confluencr / Walnut Folks Group Assessment - Phase 3 Worker Index
-- Target Schema: confluencr_asgnmt
-- Description: Composite index to optimize worker job claiming queries.
-- ====================================================================

CREATE INDEX IF NOT EXISTS idx_processing_jobs_claim 
    ON confluencr_asgnmt.processing_jobs(status, available_at);
