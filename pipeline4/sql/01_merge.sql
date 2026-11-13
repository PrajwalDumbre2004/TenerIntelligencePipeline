-- pipeline4/sql/01_merge.sql
--
-- Step C (pipeline4_next_steps.md): copy validated staging rows into the
-- production `tenders` / `bid_participations` tables. Pure INSERT ... SELECT --
-- pipeline3 already flattened participants into staging.bid_participations_raw,
-- so there is no Python-side transformation left to do here.
--
-- Idempotent / safe to re-run: only ever touches rows still flagged
-- merged_at IS NULL, so a batch that already merged is a no-op on retry.
--
-- Leaves two session-scoped TEMP TABLES behind for 02_sellers_profile.sql and
-- 03_seller_competitors.sql to consume in the same psql session:
--   batch_tenders     -- (run_id, bid_id) merged by this run
--   impacted_sellers  -- distinct seller_id written to bid_participations by this run
--
-- No BEGIN/COMMIT here: 00_run_all.sql wraps this file together with
-- 02_sellers_profile.sql and 03_seller_competitors.sql in a single outer
-- transaction, so all three either commit together or roll back together.
-- Do not add a BEGIN/COMMIT pair back into this file -- a COMMIT here would
-- commit the outer transaction early instead of just this step.

-- staging.bid_participations_raw has its merged_at column added in
-- validation_sql/02_schema.sql so the validation indexes can safely use it.

DROP TABLE IF EXISTS batch_tenders;
CREATE TEMP TABLE batch_tenders AS
SELECT run_id, bid_id
FROM staging.tenders_raw
WHERE merged_at IS NULL AND validated;

-- 1a. Stub any brand-new sellers first -- bid_participations.seller_id has a
-- FK to sellers(seller_id), checked per-statement, so the row must exist
-- before the participations INSERT below runs. Sellers that already have a
-- row are left completely untouched here (ON CONFLICT DO NOTHING) -- their
-- real profile numbers get (re)computed in 02_sellers_profile.sql.
INSERT INTO sellers (seller_id, seller_name, mse_status)
SELECT DISTINCT bp.seller_id, bp.seller_name, bp.mse_status
FROM staging.bid_participations_raw bp
JOIN batch_tenders bt USING (run_id, bid_id)
WHERE bp.merged_at IS NULL
ON CONFLICT (seller_id) DO NOTHING;

-- 1b. Tender metadata: staging.tenders_raw -> tenders (one row per bid_id).
-- buyer_name/buyer_department aren't plain staging columns -- they live inside
-- buyer_details_raw (pipeline3/extract.py's own logic for pulling them out).
-- is_ra_bid is likewise re-derived from bid_number, same regex pipeline3 uses
-- to choose bid_result vs ra_result.
INSERT INTO tenders (
    bid_id, bid_id_parent, bid_number, bid_number_parent, ra_number, tender_year,
    category, department, ministry, month, end_date, buyer_name, buyer_department,
    is_ra_bid, scraped_at, bid_result_raw, ra_result_raw, buyer_details_raw
)
SELECT
    bid_id,
    bid_id_parent,
    COALESCE(bid_number, ''),
    COALESCE(bid_number_parent, ''),
    COALESCE(ra_number, ''),
    EXTRACT(YEAR FROM end_date::date)::smallint,
    COALESCE(category, ''),
    COALESCE(department, ''),
    COALESCE(ministry, ''),
    COALESCE(month, ''),
    end_date::date,
    COALESCE(buyer_details_raw ->> 'buyer_name', ''),
    COALESCE(buyer_details_raw ->> 'buyer_department', department, ''),
    COALESCE(bid_number, '') ~ '/(R|RA)/',
    scraped_at,
    bid_result_raw,
    ra_result_raw,
    buyer_details_raw
FROM staging.tenders_raw
WHERE merged_at IS NULL AND validated
ON CONFLICT (bid_id, tender_year) DO UPDATE SET
    bid_id_parent      = EXCLUDED.bid_id_parent,
    bid_number         = EXCLUDED.bid_number,
    bid_number_parent  = EXCLUDED.bid_number_parent,
    ra_number          = EXCLUDED.ra_number,
    category           = EXCLUDED.category,
    department         = EXCLUDED.department,
    ministry           = EXCLUDED.ministry,
    month              = EXCLUDED.month,
    end_date           = EXCLUDED.end_date,
    buyer_name         = EXCLUDED.buyer_name,
    buyer_department   = EXCLUDED.buyer_department,
    is_ra_bid          = EXCLUDED.is_ra_bid,
    scraped_at         = EXCLUDED.scraped_at,
    bid_result_raw     = EXCLUDED.bid_result_raw,
    ra_result_raw      = EXCLUDED.ra_result_raw,
    buyer_details_raw  = EXCLUDED.buyer_details_raw;

-- 1c. Participation rows: staging.bid_participations_raw -> bid_participations.
-- Straight column copy -- category/buyer_name/etc. are NOT duplicated here,
-- they stay on `tenders` and are reached via JOIN (Postgresql_schema.md Section 4).
INSERT INTO bid_participations (
    seller_id, bid_id, participation_year, rank, rank_num, total_price, is_winner,
    winner_id, winner_name, winner_price, technical_status, outcome, seller_name,
    mse_status, end_date
)
SELECT
    bp.seller_id, bp.bid_id,
    EXTRACT(YEAR FROM bp.end_date::date)::smallint,
    bp.rank, bp.rank_num, bp.total_price, bp.is_winner,
    bp.winner_id, bp.winner_name, bp.winner_price,
    bp.technical_status, bp.outcome, bp.seller_name, bp.mse_status,
    bp.end_date::date
FROM staging.bid_participations_raw bp
JOIN batch_tenders bt USING (run_id, bid_id)
WHERE bp.merged_at IS NULL
ON CONFLICT (seller_id, bid_id, participation_year) DO UPDATE SET
    rank              = EXCLUDED.rank,
    rank_num          = EXCLUDED.rank_num,
    total_price       = EXCLUDED.total_price,
    is_winner         = EXCLUDED.is_winner,
    winner_id         = EXCLUDED.winner_id,
    winner_name       = EXCLUDED.winner_name,
    winner_price      = EXCLUDED.winner_price,
    technical_status  = EXCLUDED.technical_status,
    outcome           = EXCLUDED.outcome,
    seller_name       = EXCLUDED.seller_name,
    mse_status        = EXCLUDED.mse_status,
    end_date          = EXCLUDED.end_date;

-- 1d. Capture impacted sellers BEFORE flagging merged_at -- this is the set
-- 02_sellers_profile.sql and 03_seller_competitors.sql scope their recompute to.
DROP TABLE IF EXISTS impacted_sellers;
CREATE TEMP TABLE impacted_sellers AS
SELECT DISTINCT bp.seller_id
FROM staging.bid_participations_raw bp
JOIN batch_tenders bt USING (run_id, bid_id)
WHERE bp.merged_at IS NULL;

-- 1e. Mark this batch merged so a re-run of this script is a no-op for these rows.
UPDATE staging.bid_participations_raw bp
SET merged_at = now()
FROM batch_tenders bt
WHERE bp.run_id = bt.run_id AND bp.bid_id = bt.bid_id AND bp.merged_at IS NULL;

UPDATE staging.tenders_raw t
SET merged_at = now()
FROM batch_tenders bt
WHERE t.run_id = bt.run_id AND t.bid_id = bt.bid_id
  AND t.merged_at IS NULL AND t.validated;
