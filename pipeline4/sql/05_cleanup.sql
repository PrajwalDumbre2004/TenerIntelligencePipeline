-- pipeline4/sql/05_cleanup.sql
--
-- Cleans up staging rows that have successfully completed the pipeline
-- (i.e. have been merged into the public schema). 
--
-- This strictly relies on the `merged_at` timestamp being set by 
-- 01_merge.sql. It does NOT touch un-merged rows, nor does it touch 
-- rows in `staging.bid_participations_quarantine`.

BEGIN;

DELETE FROM staging.tenders_raw
WHERE merged_at IS NOT NULL;

DELETE FROM staging.bid_participations_raw
WHERE merged_at IS NOT NULL;

COMMIT;
