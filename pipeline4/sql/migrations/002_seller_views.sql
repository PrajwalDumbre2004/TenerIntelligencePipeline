-- pipeline4/sql/migrations/002_seller_views.sql
--
-- One-time migration. Run AFTER 001 (needs tenders.stage).
--
-- Makes win_rate in the seller_categories / seller_buyers materialized views
-- use the same rule as sellers.win_rate (02_sellers_profile.sql):
--
--     win_rate = wins / participations in AWARDED bids
--
-- Bids at technical or financial evaluation (stage 1/2) have no winner yet, so
-- counting them in the denominator would drag every seller's win rate down.
-- tenders_participated still counts ALL participations. A new column,
-- awarded_participated, holds the denominator. tenders.stage NULL (rows from
-- before stages existed, all awarded) counts as awarded.
--
-- The definitions below are the LIVE ones read from the database with
-- pg_get_viewdef (seller_categories differs from Postgresql_schema.md), changed
-- only for awarded_participated / win_rate. Nothing depends on these views, so
-- they can be dropped and recreated. Both happen in one transaction, so readers
-- never see a missing view; they wait on the lock while the views are rebuilt
-- (a few minutes at ~40M participation rows).
--
-- 04_refresh_views.sql needs no change: REFRESH ... CONCURRENTLY keeps working
-- because the same unique indexes are recreated here.
--
-- Usage:
--   psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/migrations/002_seller_views.sql

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS seller_categories;
DROP MATERIALIZED VIEW IF EXISTS seller_buyers;

CREATE MATERIALIZED VIEW seller_categories AS
WITH category_stats AS (
    SELECT bp.seller_id,
           t.category,
           count(*)                                                AS tenders_participated,
           count(*) FILTER (WHERE COALESCE(t.stage, 3) = 3)        AS awarded_participated,
           count(*) FILTER (WHERE bp.is_winner)                    AS tenders_won,
           COALESCE(sum(bp.total_price) FILTER (WHERE bp.is_winner), 0::numeric) AS total_value_won,
           avg(bp.total_price) FILTER (WHERE bp.total_price IS NOT NULL AND bp.total_price > 0::numeric) AS avg_price
    FROM bid_participations bp
    JOIN tenders t USING (bid_id)
    WHERE t.category <> ''::text
    GROUP BY bp.seller_id, t.category
), monthly_stats AS (
    SELECT bp.seller_id,
           t.category,
           t.month,
           count(*)                             AS participated,
           count(*) FILTER (WHERE bp.is_winner) AS won
    FROM bid_participations bp
    JOIN tenders t USING (bid_id)
    WHERE t.category <> ''::text AND t.month <> ''::text
    GROUP BY bp.seller_id, t.category, t.month
), monthly_agg AS (
    SELECT monthly_stats.seller_id,
           monthly_stats.category,
           jsonb_agg(jsonb_build_object('month', monthly_stats.month,
                                        'participated', monthly_stats.participated,
                                        'won', monthly_stats.won)
                     ORDER BY monthly_stats.month) AS monthly
    FROM monthly_stats
    GROUP BY monthly_stats.seller_id, monthly_stats.category
)
SELECT cs.seller_id,
       cs.category,
       cs.tenders_participated,
       cs.tenders_won,
       CASE WHEN cs.awarded_participated > 0
            THEN round(cs.tenders_won::numeric * 100.0 / cs.awarded_participated::numeric, 1)
            ELSE 0::numeric END                                    AS win_rate,
       cs.total_value_won,
       round(cs.avg_price, 2)                                      AS avg_price,
       ma.monthly,
       now()                                                       AS updated_at,
       cs.awarded_participated
FROM category_stats cs
LEFT JOIN monthly_agg ma USING (seller_id, category);

CREATE UNIQUE INDEX uix_sc_seller_category ON public.seller_categories USING btree (seller_id, category);
CREATE INDEX ix_sc_seller_participated     ON public.seller_categories USING btree (seller_id, tenders_participated DESC);

CREATE MATERIALIZED VIEW seller_buyers AS
SELECT bp.seller_id,
       t.buyer_name,
       t.buyer_name                                                AS buyer_id,
       max(t.buyer_department)                                     AS buyer_department,
       count(*)                                                    AS tenders_participated,
       count(*) FILTER (WHERE bp.is_winner)                        AS tenders_won,
       CASE WHEN count(*) FILTER (WHERE COALESCE(t.stage, 3) = 3) > 0
            THEN round(count(*) FILTER (WHERE bp.is_winner)::numeric * 100.0
                       / (count(*) FILTER (WHERE COALESCE(t.stage, 3) = 3))::numeric, 1)
            ELSE 0::numeric END                                    AS win_rate,
       COALESCE(sum(bp.total_price) FILTER (WHERE bp.is_winner), 0::numeric) AS total_value_won,
       array_agg(DISTINCT t.category) FILTER (WHERE t.category <> ''::text) AS categories,
       max(t.month) FILTER (WHERE t.month <> ''::text)             AS last_participated,
       max(t.month) FILTER (WHERE bp.is_winner AND t.month <> ''::text) AS last_won,
       now()                                                       AS updated_at,
       count(*) FILTER (WHERE COALESCE(t.stage, 3) = 3)            AS awarded_participated
FROM bid_participations bp
JOIN tenders t USING (bid_id)
WHERE t.buyer_name <> ''::text
GROUP BY bp.seller_id, t.buyer_name;

CREATE UNIQUE INDEX uix_sb_seller_buyer ON public.seller_buyers USING btree (seller_id, buyer_id);
CREATE INDEX ix_sb_seller_participated  ON public.seller_buyers USING btree (seller_id, tenders_participated DESC);

COMMIT;
