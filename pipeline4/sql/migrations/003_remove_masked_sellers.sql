-- pipeline4/sql/migrations/003_remove_masked_sellers.sql
--
-- One-off cleanup: remove the two "sellers" that are really GeM-masked names.
--   '******'      (6 stars)  -- masked WINNER   (827 bids, 824 wins)
--   '**********'  (10 stars) -- masked bidder   (1378 bids, 0 wins)
-- seller_id is the normalised name, so every masked bidder on GeM collapsed into
-- one of these two fake sellers. Names that merely CONTAIN a '*' (e.g.
-- "7* STAR TECHNOLOGIES", "AV CONCEPT 24*7", "**** VINAY & SONS") are real and
-- are NOT touched: the target set is the two exact ids below, nothing pattern-based.
--
-- Brings stored data to the state the current validation would have produced
-- (validation_sql/04 quarantines masked rows, validation_sql/06 never merges a
-- tender left with no valid participant):
--   1. seller_competitors -- pairs (masked, X) and their mirror (X, masked)
--   2. bid_participations -- the masked rows only
--   3. tenders            -- bids whose ONLY participants were masked (no row
--                            left at all after step 2); pipeline2's prune will
--                            re-scrape them, exactly like 06 intends
--   4. sellers            -- the two profile rows
--
-- Deliberately left as is:
--   - real co-bidders' rows in the same bids (rank, price, outcome are GeM's real
--     values). Their winner_id/winner_name still say '******' where the masked
--     seller won; that is what the pipeline writes going forward too (only the
--     masked row is quarantined), and the backend uses winner_name only as a
--     display label, never in a number.
--   - real sellers' profiles (sellers row) -- 02_sellers_profile.sql builds each
--     profile only from that seller's OWN rows, so nothing in them changes.
--
-- Performance: the ids are written as literals on purpose. `IN (SELECT ... FROM
-- <temp table>)` made the planner seq-scan bid_participations (40 M rows) and
-- seller_competitors (126 M rows); literals use the seller_id indexes, and the
-- small analyzed temp tables let the mirror/tender deletes use the PKs.
-- The only unavoidable scans are the two FK checks on
-- seller_competitors.competitor_id (no index) when the sellers rows are deleted.
--
-- Safety:
--   - one transaction; every DELETE's row count is checked against counts taken
--     inside the same transaction before deleting; any mismatch aborts.
--   - the FK seller_competitors.competitor_id -> sellers makes step 4 fail
--     (and roll everything back) if any (X, masked) pair was left behind.
--   - DRY RUN BY DEFAULT: ends in ROLLBACK unless run with -v do_commit=1.
--
-- Usage (stop pipeline runs first, so no merge interleaves):
--   dry run : psql -v ON_ERROR_STOP=1 -f pipeline4/sql/migrations/003_remove_masked_sellers.sql
--   commit  : psql -v ON_ERROR_STOP=1 -v do_commit=1 -f pipeline4/sql/migrations/003_remove_masked_sellers.sql
-- then, only after a commit (cannot run inside a transaction):
--   REFRESH MATERIALIZED VIEW CONCURRENTLY seller_categories;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY seller_buyers;

\set ON_ERROR_STOP 1
\timing on

BEGIN;
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '10min';

-- Guard: both ids exist and are made of asterisks only. Protects against this
-- file ever being edited to point at a real seller.
DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n
    FROM sellers
    WHERE seller_id IN ('******', '**********') AND seller_name ~ '^\*+$';
    IF n <> 2 THEN
        RAISE EXCEPTION 'guard failed: expected 2 all-asterisk sellers, found %', n;
    END IF;
END $$;

-- Pairs the masked ids appear in (direct side, PK range scan).
CREATE TEMP TABLE masked_pairs ON COMMIT DROP AS
SELECT seller_id, competitor_id
FROM seller_competitors
WHERE seller_id IN ('******', '**********');
ANALYZE masked_pairs;

-- Bids where EVERY participant is masked -> their tender is deleted in step 3.
CREATE TEMP TABLE masked_only_bids ON COMMIT DROP AS
SELECT DISTINCT bp.bid_id, bp.participation_year
FROM bid_participations bp
WHERE bp.seller_id IN ('******', '**********')
  AND NOT EXISTS (
      SELECT 1 FROM bid_participations x
      WHERE x.bid_id = bp.bid_id
        AND x.seller_id NOT IN ('******', '**********')
  );
ANALYZE masked_only_bids;

-- Expected counts, measured inside this transaction.
CREATE TEMP TABLE expected ON COMMIT DROP AS
SELECT
    (SELECT count(*) FROM masked_pairs)                                     AS sc_direct,
    (SELECT count(*) FROM bid_participations
      WHERE seller_id IN ('******', '**********'))                          AS bp_rows,
    (SELECT count(*) FROM tenders t JOIN masked_only_bids mb
       ON t.bid_id = mb.bid_id AND t.tender_year = mb.participation_year)   AS tenders_rows;

CREATE TEMP TABLE report (step text, expected bigint, deleted bigint) ON COMMIT DROP;

-- 1a. Mirror pairs (X, masked), through the PK (seller_competitors is symmetric).
WITH d AS (
    DELETE FROM seller_competitors sc
    USING masked_pairs p
    WHERE sc.seller_id = p.competitor_id AND sc.competitor_id = p.seller_id
    RETURNING 1
)
INSERT INTO report SELECT '1a seller_competitors (X, masked)', (SELECT sc_direct FROM expected), count(*) FROM d;

-- 1b. Direct pairs (masked, X).
WITH d AS (
    DELETE FROM seller_competitors
    WHERE seller_id IN ('******', '**********')
    RETURNING 1
)
INSERT INTO report SELECT '1b seller_competitors (masked, X)', (SELECT sc_direct FROM expected), count(*) FROM d;

-- 2. Masked participation rows.
WITH d AS (
    DELETE FROM bid_participations
    WHERE seller_id IN ('******', '**********')
    RETURNING 1
)
INSERT INTO report SELECT '2  bid_participations', (SELECT bp_rows FROM expected), count(*) FROM d;

-- 3. Tenders left with no participant at all (re-checked after step 2).
WITH d AS (
    DELETE FROM tenders t
    USING masked_only_bids mb
    WHERE t.bid_id = mb.bid_id AND t.tender_year = mb.participation_year
      AND NOT EXISTS (SELECT 1 FROM bid_participations x WHERE x.bid_id = t.bid_id)
    RETURNING 1
)
INSERT INTO report SELECT '3  tenders (masked-only bids)', (SELECT tenders_rows FROM expected), count(*) FROM d;

-- 4. The two profile rows (FK checks here catch any leftover reference).
WITH d AS (
    DELETE FROM sellers WHERE seller_id IN ('******', '**********')
    RETURNING 1
)
INSERT INTO report SELECT '4  sellers', 2, count(*) FROM d;

SELECT * FROM report;

DO $$
DECLARE bad text;
BEGIN
    SELECT string_agg(step || ' expected ' || expected || ' got ' || deleted, '; ')
      INTO bad FROM report WHERE expected <> deleted;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'count mismatch, rolling back: %', bad;
    END IF;
    IF EXISTS (SELECT 1 FROM bid_participations WHERE seller_id IN ('******', '**********')) THEN
        RAISE EXCEPTION 'masked rows still present in bid_participations';
    END IF;
END $$;

\if :{?do_commit}
    COMMIT;
    \echo '>>> COMMITTED. Now refresh seller_categories and seller_buyers.'
\else
    ROLLBACK;
    \echo '>>> DRY RUN: rolled back, nothing changed. Re-run with -v do_commit=1 to apply.'
\endif
