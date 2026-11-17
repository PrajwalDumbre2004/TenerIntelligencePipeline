-- pipeline4/sql/05_cleanup.sql
--
-- Cleans up staging rows that have successfully completed the pipeline
-- (i.e. have been merged into the public schema). 
--
-- This strictly relies on the `merged_at` timestamp being set by
-- 01_merge.sql. The one exception is un-merged tenders rejected with
-- `no_valid_participants` (see the last statement). It does not touch rows in
-- `staging.bid_participations_quarantine`.

BEGIN;

DELETE FROM staging.tenders_raw
WHERE merged_at IS NOT NULL;

DELETE FROM staging.bid_participations_raw
WHERE merged_at IS NOT NULL;

-- Tenders rejected by validation_sql/06_require_participants.sql (no table,
-- RA page with only the parent's table, or every name masked). They are kept
-- OUT of the main DB on purpose. Deleting them here means the next re-run of
-- their month stages them again (they are not in `tenders`, so pipeline2's
-- prune keeps them) and pipeline3 scrapes them again. Any quarantined rows stay
-- in staging.bid_participations_quarantine as a record.
DELETE FROM staging.tenders_raw
WHERE merged_at IS NULL
  AND NOT validated
  AND validation_errors @> '[{"code": "no_valid_participants"}]'::jsonb;

COMMIT;
