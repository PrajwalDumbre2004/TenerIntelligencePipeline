-- pipeline4/sql/05_cleanup.sql
--
-- Cleans up staging rows that have successfully completed the pipeline
-- (i.e. have been merged into the public schema). 
--
-- This strictly relies on the `merged_at` timestamp being set by
-- 01_merge.sql. Two exceptions: un-merged tenders rejected with
-- `no_valid_participants`, and rows marked `known_at` (see the last two
-- statements). It does not touch rows in `staging.bid_participations_quarantine`.

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

-- Bids pipeline2 marked as already known (in `tenders` at the same or a higher
-- stage, see pipeline2/db.py mark_known_bid_ids). They are kept in staging only
-- so a restarted scrape sees their month as captured; nothing scrapes, validates
-- or merges them. They MUST be removed here: a month's run_id is permanent, so
-- rows left behind would make the next month's pipeline2 count the month as
-- already captured and skip it, missing every stage change.
DELETE FROM staging.tenders_raw
WHERE known_at IS NOT NULL;

COMMIT;
