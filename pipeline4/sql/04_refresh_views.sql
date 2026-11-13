-- pipeline4/sql/04_refresh_views.sql
--
-- Step D2 (pipeline4_next_steps.md): refresh the seller_categories/seller_buyers
-- materialized views defined in Postgresql_schema.md Section 5.
--
-- Not scoped to impacted_sellers: REFRESH MATERIALIZED VIEW always recomputes
-- the entire view (no per-seller partial refresh exists in Postgres). It's
-- also not part of the 01/02/03 transaction in 00_run_all.sql -- CONCURRENTLY
-- cannot run inside a transaction block at all, and even without CONCURRENTLY
-- a full rebuild at 4.6M/9M rows is not something we want to hold the merge
-- transaction open for.
--
-- 00_run_all.sql runs this file right after its merge transaction COMMITs, as
-- a separate top-level step. That ordering (not a shared transaction) is what
-- makes it safe: this is a full, idempotent rebuild from current table state,
-- so it only ever reflects data that has already durably committed -- there's
-- no scenario where a rolled-back merge leaves the views half-updated. See
-- migration_plan_final.md Section 3.3 for the original cadence trade-off.
--
-- CONCURRENTLY requires the unique indexes on the views to already exist
-- (uix_sc_seller_category, uix_sb_seller_buyer -- created alongside the views
-- in Postgresql_schema.md) and lets live queries keep reading the old data
-- while the refresh runs.

REFRESH MATERIALIZED VIEW CONCURRENTLY seller_categories;
REFRESH MATERIALIZED VIEW CONCURRENTLY seller_buyers;
