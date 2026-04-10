-- ============================================================================
-- Databricks Healthcare Claims Data Warehouse - Schema Definitions
-- ============================================================================
-- Creates the four-layer schema architecture for the claims data warehouse:
--   1. claims_raw       - Raw ingested data from source systems (landing zone)
--   2. claims_staging    - Cleansed, deduped, PHI-masked data ready for warehouse
--   3. claims_warehouse  - Conformed dimensions and facts (star schema)
--   4. claims_mart       - Pre-aggregated tables for analytics and reporting
--
-- Platform: Databricks / Unity Catalog
-- Delta Lake format is the default for all tables
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Schema: claims_raw
-- Purpose: Landing zone for raw claims data ingested via Auto Loader or
--          batch COPY INTO. Data here is untransformed and retains source
--          system formats. Used as the single source of truth for reprocessing.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS claims_raw
COMMENT 'Raw healthcare claims data ingested from source systems. No transformations applied.';

-- ---------------------------------------------------------------------------
-- Schema: claims_staging
-- Purpose: Cleansed and deduplicated data. PHI masking is applied at this
--          layer (before warehouse) to ensure downstream consumers never see
--          unmasked PII/PHI. ADR deduplication and current-record selection
--          happen here.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS claims_staging
COMMENT 'Staging layer with cleansed, deduplicated, and PHI-masked claims data.';

-- ---------------------------------------------------------------------------
-- Schema: claims_warehouse
-- Purpose: Conformed star schema with SCD Type 2 dimensions and fact tables.
--          Supports enterprise-wide analytics and cross-functional reporting.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS claims_warehouse
COMMENT 'Conformed star schema with dimensions and fact tables for healthcare claims analytics.';

-- ---------------------------------------------------------------------------
-- Schema: claims_mart
-- Purpose: Pre-aggregated and denormalized tables optimized for specific
--          reporting use cases: member months, claim summaries, encounter
--          summaries, and quality measures (HEDIS).
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS claims_mart
COMMENT 'Pre-aggregated mart tables for healthcare claims reporting and quality measures.';
