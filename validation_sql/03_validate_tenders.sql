-- validation_sql/03_validate_tenders.sql
--
-- Tender-level validation. Writes into columns pipeline4/sql/01_merge.sql
-- ALREADY reads (`validated`, `validation_errors`) -- no change needed there.
-- A tender that fails just stays `validated = FALSE` and sits in staging
-- untouched, exactly like today's "hard-fail leaves rows for inspection/retry"
-- contract.
--
-- Scope: not already merged (`merged_at IS NULL`), and either never judged
-- (`validation_errors IS NULL`) or previously rejected (`NOT validated`).
-- Rejected rows are re-judged on every run because their state can change
-- after rejection -- e.g. a tender held back as 'not_scraped' gets scraped by
-- a later pipeline3 pass and must then be allowed through. Rows that already
-- passed are not re-judged. Safe to re-run repeatedly.
--
-- Scrape-completeness checks ('not_scraped', 'scrape_failed'): a tender must
-- never reach `public.tenders` without its results. Once merged, staging is
-- cleaned up and pipeline2 prunes the bid_id as "already known", so an empty
-- merged tender would never be scraped again. pipeline3 leaves scraped_at
-- NULL on a failed scrape, so both cases simply wait here until a scrape
-- succeeds.
--
-- Each finding is tagged with a severity:
--   'hard' -- structurally broken, this tender must not merge yet.
--   'soft' -- worth a look, but not a reason to withhold an otherwise-valid
--             tender from `public.tenders` (matches the hard/soft split
--             already named in migration_plan.md's validation section).
-- `validated` is only ever FALSE because of a 'hard' finding.
--
-- Year range note: the "implausible date" checks below are deliberately NOT
-- pinned to the 2020-2026 partitions that happen to exist today in
-- `Postgresql_schema.md` -- new partitions get added over time and this
-- pipeline is meant to keep running for years, so a legitimate future tender
-- must never be rejected just because nobody has created next year's
-- partition yet. The bounds here (2000 .. current_year + 10) are a generous
-- sanity net against corrupt parses (e.g. a mis-parsed date landing in year 1
-- or year 9999), not a partition-existence check. Note this does NOT
-- guarantee a matching partition exists in `tenders` -- if a batch's year is
-- genuinely new, an operator still needs to add that year's partition to the
-- production schema before `01_merge.sql` can insert it; validation only
-- confirms the *date itself* is sane. Without this check, a single corrupt
-- date would otherwise reach `01_merge.sql`, which INSERTs into a RANGE-
-- partitioned table with no partition for that year -- Postgres raises "no
-- partition of relation found for row", which aborts 00_run_all.sql's single
-- outer transaction and rolls back the ENTIRE batch's merge, not just the
-- one bad tender.
WITH pending AS (
    SELECT run_id, bid_id, end_date, category, department, buyer_details_raw,
           scraped_at, scrape_error
    FROM staging.tenders_raw
    WHERE merged_at IS NULL
      AND (validation_errors IS NULL OR NOT validated)
),
checks AS (
    SELECT
        run_id,
        bid_id,
        ARRAY_REMOVE(ARRAY[
            CASE WHEN bid_id IS NULL
                 THEN 'missing_bid_id' END,
            CASE WHEN scraped_at IS NULL
                 THEN 'not_scraped' END,
            CASE WHEN scrape_error IS NOT NULL
                 THEN 'scrape_failed' END,
            CASE WHEN end_date IS NULL OR btrim(end_date) = ''
                 THEN 'missing_end_date' END,
            CASE WHEN end_date IS NOT NULL AND btrim(end_date) <> ''
                      AND staging.safe_to_date(end_date) IS NULL
                 THEN 'unparseable_end_date' END,
            CASE WHEN staging.safe_to_date(end_date) IS NOT NULL
                      AND EXTRACT(YEAR FROM staging.safe_to_date(end_date)) < 2000
                 THEN 'end_date_implausibly_old' END,
            CASE WHEN staging.safe_to_date(end_date) IS NOT NULL
                      AND EXTRACT(YEAR FROM staging.safe_to_date(end_date))
                          > EXTRACT(YEAR FROM now()) + 10
                 THEN 'end_date_implausibly_future' END
        ], NULL) AS hard_codes,
        ARRAY_REMOVE(ARRAY[
            CASE WHEN COALESCE(btrim(category), '') = ''
                 THEN 'empty_category' END,
            CASE WHEN COALESCE(btrim(buyer_details_raw ->> 'buyer_name'), '') = ''
                      AND COALESCE(btrim(department), '') = ''
                 THEN 'empty_buyer_name' END
        ], NULL) AS soft_codes
    FROM pending
)
UPDATE staging.tenders_raw t
SET
    validation_errors =
        COALESCE(
            (SELECT jsonb_agg(jsonb_build_object('code', code, 'severity', 'hard'))
             FROM unnest(c.hard_codes) AS code),
            '[]'::jsonb
        )
        ||
        COALESCE(
            (SELECT jsonb_agg(jsonb_build_object('code', code, 'severity', 'soft'))
             FROM unnest(c.soft_codes) AS code),
            '[]'::jsonb
        ),
    validated = (COALESCE(array_length(c.hard_codes, 1), 0) = 0)
FROM checks c
WHERE t.run_id = c.run_id AND t.bid_id = c.bid_id;
