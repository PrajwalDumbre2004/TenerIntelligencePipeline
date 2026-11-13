-- validation_sql/05_post_merge_checks.sql
--
-- Read-only post-merge verification. Run this AFTER pipeline4/sql/00_run_all.sql
-- has completed (merge + sellers profile + competitors + MV refresh), to check
-- that what landed in `public` is actually correct -- not just that the merge
-- ran without a SQL error.
--
-- Nothing in this file writes to `public.*`, `staging.*`, or any materialized
-- view. It is safe to run any number of times, at any time, against a live
-- database -- it only ever SELECTs.
--
-- Every check below is a "this should return zero rows / an expected count"
-- query. A non-empty result under a "should be empty" heading is a real
-- finding worth investigating before trusting the batch; it does not mean
-- this script mutates anything to fix it.
--
-- Usage:
--   psql "$POSTGRES_DSN" -f validation_sql/05_post_merge_checks.sql
--
-- CHECK 6b picks a sample seller automatically (highest total_tenders); edit
-- its WHERE clause if you want to target a specific seller_id instead. CHECK 8
-- is a scaffold -- fill in real bid_ids from your batch before it's useful.

\pset pager off

-- ═══════════════════════════════════════════════════════════════════════════
-- CHECK 1 -- Merge completeness: anything that should have merged but didn't.
--
-- Meaningful only BEFORE pipeline4/sql/05_cleanup.sql has deleted the merged
-- staging rows for this batch -- once cleanup runs, merged_at IS NOT NULL rows
-- are gone from staging entirely, so this will just show whatever the NEXT
-- unmerged batch happens to be (not evidence of a problem with the batch you
-- just ran). Run this check before cleanup, or immediately after merge in the
-- same session, if you want it to mean anything for THIS batch.
-- ═══════════════════════════════════════════════════════════════════════════
\echo '--- CHECK 1a: validated tenders never merged (expect 0) ---'
SELECT count(*) AS stuck_validated_tenders
FROM staging.tenders_raw
WHERE validated AND merged_at IS NULL;

\echo '--- CHECK 1b: participation rows never merged and not quarantined (expect 0) ---'
SELECT count(*) AS stuck_participations
FROM staging.bid_participations_raw
WHERE merged_at IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- CHECK 2 -- Quarantine review: what got pulled out, and why. Not "expect 0"
-- -- some quarantine volume is normal (masked names, bad prices from GeM's
-- source data). This is for a human to skim and confirm nothing that looks
-- like a real, valid tender got quarantined by mistake.
-- ═══════════════════════════════════════════════════════════════════════════
\echo '--- CHECK 2: quarantine reasons breakdown ---'
SELECT
    (jsonb_array_elements(validation_errors) ->> 'code') AS reason,
    count(*)
FROM staging.bid_participations_quarantine
GROUP BY 1
ORDER BY 2 DESC;

-- ═══════════════════════════════════════════════════════════════════════════
-- CHECK 3 -- Row-count sanity. Compare against Postgresql_schema.md's scale
-- estimates (tenders ~3.1M, bid_participations ~40M, sellers ~306K,
-- seller_categories ~2-6M, seller_buyers ~5-15M, seller_competitors ~30-100M).
-- A batch that moves these wildly outside expectation is worth a second look
-- before trusting it.
-- ═══════════════════════════════════════════════════════════════════════════
\echo '--- CHECK 3a: tenders per partition year ---'
SELECT tender_year, count(*) FROM tenders GROUP BY 1 ORDER BY 1;

\echo '--- CHECK 3b: bid_participations per partition year ---'
SELECT participation_year, count(*) FROM bid_participations GROUP BY 1 ORDER BY 1;

\echo '--- CHECK 3c: table totals ---'
SELECT 'tenders' AS table_name, count(*) FROM tenders
UNION ALL
SELECT 'bid_participations', count(*) FROM bid_participations
UNION ALL
SELECT 'sellers', count(*) FROM sellers
UNION ALL
SELECT 'seller_categories', count(*) FROM seller_categories
UNION ALL
SELECT 'seller_buyers', count(*) FROM seller_buyers
UNION ALL
SELECT 'seller_competitors', count(*) FROM seller_competitors;

-- ═══════════════════════════════════════════════════════════════════════════
-- CHECK 4 -- Structural / referential integrity.
-- ═══════════════════════════════════════════════════════════════════════════
\echo '--- CHECK 4a: bid_participations with no matching tender (expect 0) ---'
SELECT count(*) AS orphan_participations
FROM bid_participations bp
LEFT JOIN tenders t USING (bid_id)
WHERE t.bid_id IS NULL;

\echo '--- CHECK 4b: tenders whose tender_year disagrees with end_date (expect 0) ---'
SELECT count(*) AS tender_year_mismatch
FROM tenders
WHERE end_date IS NOT NULL
  AND tender_year <> EXTRACT(YEAR FROM end_date)::smallint;

\echo '--- CHECK 4c: bid_participations whose participation_year disagrees with end_date (expect 0) ---'
SELECT count(*) AS participation_year_mismatch
FROM bid_participations
WHERE end_date IS NOT NULL
  AND participation_year <> EXTRACT(YEAR FROM end_date)::smallint;

\echo '--- CHECK 4d: duplicate bid_id across tender partitions (expect 0 rows) ---'
SELECT bid_id, count(*) AS copies
FROM tenders
GROUP BY bid_id
HAVING count(*) > 1;

\echo '--- CHECK 4e: bid_participations rows referencing a seller not in sellers (expect 0; FK should prevent this) ---'
SELECT count(*) AS orphan_seller_refs
FROM bid_participations bp
LEFT JOIN sellers s ON s.seller_id = bp.seller_id
WHERE s.seller_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- CHECK 5 -- `sellers` aggregate correctness. Recomputes the same aggregate
-- pipeline4/sql/02_sellers_profile.sql computes, straight off bid_participations
-- + tenders, and diffs it against what's actually stored. Any row returned
-- here means the stored profile has drifted from the source-of-truth tables.
-- ═══════════════════════════════════════════════════════════════════════════
\echo '--- CHECK 5: sellers.total_tenders / total_wins vs live recompute (expect 0 rows) ---'
WITH recomputed AS (
    SELECT
        bp.seller_id,
        count(DISTINCT bp.bid_id)                              AS total_tenders,
        count(DISTINCT bp.bid_id) FILTER (WHERE bp.is_winner)  AS total_wins,
        COALESCE(SUM(bp.total_price) FILTER (WHERE bp.is_winner AND bp.total_price > 0), 0)
                                                                AS total_contract_value
    FROM bid_participations bp
    GROUP BY bp.seller_id
)
SELECT
    s.seller_id,
    s.total_tenders  AS stored_total_tenders,  r.total_tenders  AS recomputed_total_tenders,
    s.total_wins     AS stored_total_wins,     r.total_wins     AS recomputed_total_wins,
    s.total_contract_value AS stored_value,    r.total_contract_value AS recomputed_value
FROM sellers s
JOIN recomputed r USING (seller_id)
WHERE s.total_tenders <> r.total_tenders
   OR s.total_wins <> r.total_wins
   OR s.total_contract_value <> r.total_contract_value
LIMIT 100;

\echo '--- CHECK 5b: sellers with a row in bid_participations but missing from sellers entirely (expect 0) ---'
SELECT count(DISTINCT bp.seller_id) AS missing_seller_profiles
FROM bid_participations bp
LEFT JOIN sellers s USING (seller_id)
WHERE s.seller_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- CHECK 6 -- Materialized view freshness. If these timestamps are older than
-- your last pipeline4 run, 04_refresh_views.sql either didn't run or failed
-- silently (e.g. hit a lock) -- the views are stale relative to public.tenders
-- / public.bid_participations even though the base tables are correct.
-- ═══════════════════════════════════════════════════════════════════════════
\echo '--- CHECK 6a: materialized view last-refreshed timestamps ---'
SELECT 'seller_categories' AS view_name, max(updated_at) AS last_refreshed FROM seller_categories
UNION ALL
SELECT 'seller_buyers', max(updated_at) FROM seller_buyers;

\echo '--- CHECK 6b: sample seller -- seller_categories vs a live GROUP BY (expect identical rows) ---'
\echo '(picks the seller with the most tenders by default -- edit the WHERE clauses below to target a specific seller_id instead)'
DROP TABLE IF EXISTS _sample_seller;
CREATE TEMP TABLE _sample_seller AS
SELECT seller_id FROM sellers ORDER BY total_tenders DESC LIMIT 1;

SELECT category, tenders_participated, tenders_won, win_rate
FROM seller_categories
WHERE seller_id = (SELECT seller_id FROM _sample_seller)
ORDER BY tenders_participated DESC;

SELECT t.category,
       count(*)                             AS tenders_participated_live,
       count(*) FILTER (WHERE bp.is_winner) AS tenders_won_live,
       ROUND(count(*) FILTER (WHERE bp.is_winner) * 100.0 / count(*), 1) AS win_rate_live
FROM bid_participations bp
JOIN tenders t USING (bid_id)
WHERE bp.seller_id = (SELECT seller_id FROM _sample_seller)
  AND t.category <> ''
GROUP BY t.category
ORDER BY tenders_participated_live DESC;

DROP TABLE IF EXISTS _sample_seller;

-- ═══════════════════════════════════════════════════════════════════════════
-- CHECK 7 -- seller_competitors symmetry. shared_tenders(A, B) must equal
-- shared_tenders(B, A) -- it counts the same set of shared bids either way.
-- A mismatch means Pass 1 / Pass 2 in 03_seller_competitors.sql diverged for
-- that pair (e.g. one side's batch ran, the other's was interrupted).
-- ═══════════════════════════════════════════════════════════════════════════
\echo '--- CHECK 7: seller_competitors asymmetric pairs (expect 0 rows) ---'
SELECT a.seller_id, a.competitor_id, a.shared_tenders AS a_to_b, b.shared_tenders AS b_to_a
FROM seller_competitors a
JOIN seller_competitors b
  ON b.seller_id = a.competitor_id AND b.competitor_id = a.seller_id
WHERE a.shared_tenders <> b.shared_tenders
LIMIT 100;

\echo '--- CHECK 7b: seller_competitors rows missing their mirror row entirely (expect 0) ---'
SELECT count(*) AS unmirrored_pairs
FROM seller_competitors a
LEFT JOIN seller_competitors b
  ON b.seller_id = a.competitor_id AND b.competitor_id = a.seller_id
WHERE b.seller_id IS NULL;

-- ═══════════════════════════════════════════════════════════════════════════
-- CHECK 8 -- Spot-check scaffold. Fill in real bid_ids you can independently
-- verify (e.g. against the original GeM portal / MongoDB source) to eyeball
-- full end-to-end correctness for a handful of records.
-- ═══════════════════════════════════════════════════════════════════════════
\echo '--- CHECK 8: spot-check tender + participants for specific bid_ids ---'
\echo '(replace the bid_id list below with real IDs from this migration batch)'
SELECT * FROM tenders WHERE bid_id IN (0) ORDER BY bid_id;
SELECT * FROM bid_participations WHERE bid_id IN (0) ORDER BY bid_id, seller_id;
