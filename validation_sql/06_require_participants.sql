-- validation_sql/06_require_participants.sql
--
-- Main-DB rule: a tender is only merged into `public.tenders` if it has at
-- least ONE valid participant row. Must run AFTER 04_validate_participations.sql
-- (same transaction, see 00_run_validate.sql), because it looks at what is left
-- in staging.bid_participations_raw once 04 has quarantined the bad rows.
--
-- One check covers every "nothing usable" case:
--   - the result page had no participant table at all (common for technically
--     evaluated bids);
--   - an RA bid (/R/ in its number) whose page shows only the PARENT bid's table:
--     pipeline3/extract.py reads only ra_result for RA bids, so it derives 0 rows;
--   - every seller name was masked ("******"), so 04 quarantined all its rows.
--
-- Such a tender is set validated = FALSE with code `no_valid_participants`.
-- 01_merge.sql never merges it, and pipeline4/sql/05_cleanup.sql deletes it from
-- staging. Because it is then not in `public.tenders`, pipeline2's prune step
-- keeps it the next time its month is re-run, so it is scraped again until GeM
-- publishes a usable table.
--
-- For a stored bid whose stage moved up but whose re-scrape is unusable, this
-- means the old stored tender and participants stay as they are.
--
-- Only touches rows 03 judged valid: rows already failing (not_scraped,
-- scrape_failed, ...) keep their own codes. 03 rewrites validation_errors from
-- scratch for rejected rows on every run, so this code is never appended twice.

UPDATE staging.tenders_raw t
SET validated = FALSE,
    validation_errors = COALESCE(t.validation_errors, '[]'::jsonb)
        || '[{"code": "no_valid_participants", "severity": "hard"}]'::jsonb
WHERE t.merged_at IS NULL
  AND t.validated
  AND NOT EXISTS (
      SELECT 1
      FROM staging.bid_participations_raw bp
      WHERE bp.run_id = t.run_id AND bp.bid_id = t.bid_id
  );
