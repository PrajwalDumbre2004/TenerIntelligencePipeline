-- pipeline4/sql/02_sellers_profile.sql
--
-- Step D1 (pipeline4_next_steps.md): recompute the `sellers` profile card for
-- every seller in `impacted_sellers` (populated by 01_merge.sql, same session).
--
-- Full recompute, not an incremental patch: for each impacted seller, this
-- reads their ENTIRE bid_participations/tenders history -- not just this
-- batch's new rows -- and overwrites the row via ON CONFLICT DO UPDATE. This
-- mirrors the original pipeline4 design choice (pipeline4/orchestrator.py's
-- docstring: "recompute ALL aggregates from full ... history ... so aggregates
-- are always correct, not just the delta"). Cheap here because it's scoped to
-- one seller at a time via ix_bp_seller_date, not a scan of the whole table.

WITH scope AS (
    SELECT seller_id FROM impacted_sellers
),
base AS (
    SELECT bp.seller_id, bp.bid_id, bp.is_winner, bp.total_price, bp.seller_name,
           bp.mse_status, t.category, t.buyer_name, t.month
    FROM bid_participations bp
    JOIN tenders t USING (bid_id)
    WHERE bp.seller_id IN (SELECT seller_id FROM scope)
),
-- Shortest raw seller_name wins the tie-break -- SQL equivalent of
-- builders.py's `min(names, key=len)`.
name_pick AS (
    SELECT DISTINCT ON (seller_id) seller_id, seller_name
    FROM base
    WHERE seller_name <> ''
    ORDER BY seller_id, length(seller_name) ASC
),
-- First non-empty mse_status encountered, same as builders.py's fallback loop.
mse_pick AS (
    SELECT DISTINCT ON (seller_id) seller_id, mse_status
    FROM base
    WHERE mse_status <> ''
    ORDER BY seller_id
),
cat_counts AS (
    SELECT seller_id, category,
           COUNT(*)                         AS cnt,
           COUNT(*) FILTER (WHERE is_winner) AS wins
    FROM base
    WHERE category <> ''
    GROUP BY seller_id, category
),
cat_ranked AS (
    SELECT seller_id,
           jsonb_build_object('category', category, 'count', cnt, 'wins', wins) AS entry,
           ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY cnt DESC) AS rn
    FROM cat_counts
),
top_categories AS (
    SELECT seller_id, jsonb_agg(entry ORDER BY rn) AS top_categories
    FROM cat_ranked
    WHERE rn <= 10
    GROUP BY seller_id
),
buyer_counts AS (
    SELECT seller_id, buyer_name,
           COUNT(*)                         AS cnt,
           COUNT(*) FILTER (WHERE is_winner) AS wins
    FROM base
    WHERE buyer_name <> ''
    GROUP BY seller_id, buyer_name
),
buyer_ranked AS (
    SELECT seller_id,
           jsonb_build_object('buyer_name', buyer_name, 'count', cnt, 'wins', wins) AS entry,
           ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY cnt DESC) AS rn
    FROM buyer_counts
),
top_buyers AS (
    SELECT seller_id, jsonb_agg(entry ORDER BY rn) AS top_buyers
    FROM buyer_ranked
    WHERE rn <= 10
    GROUP BY seller_id
),
-- win_price_count/total_contract_value only count wins with a positive price,
-- matching builders.py's win_prices list (total_price > 0 filter) --
-- avg_contract_value is total_contract_value / win_price_count, not
-- total_contract_value / total_wins.
profile AS (
    SELECT
        b.seller_id,
        COUNT(DISTINCT b.bid_id)                                                          AS total_tenders,
        COUNT(DISTINCT b.bid_id) FILTER (WHERE b.is_winner)                               AS total_wins,
        COALESCE(SUM(b.total_price) FILTER (WHERE b.is_winner AND b.total_price > 0), 0)  AS total_contract_value,
        COUNT(*) FILTER (WHERE b.is_winner AND b.total_price > 0)                         AS win_price_count,
        MIN(b.month) FILTER (WHERE b.month <> '')                                         AS first_seen,
        MAX(b.month) FILTER (WHERE b.month <> '')                                         AS last_seen
    FROM base b
    GROUP BY b.seller_id
)
INSERT INTO sellers (
    seller_id, seller_name, seller_name_norm, mse_status,
    total_tenders, total_wins, win_rate, total_contract_value, avg_contract_value,
    top_categories, top_buyers, first_seen, last_seen, updated_at
)
SELECT
    p.seller_id,
    COALESCE(n.seller_name, p.seller_id),
    p.seller_id,
    COALESCE(m.mse_status, ''),
    p.total_tenders,
    p.total_wins,
    ROUND(p.total_wins * 100.0 / p.total_tenders, 1),
    p.total_contract_value,
    CASE WHEN p.win_price_count > 0
         THEN ROUND(p.total_contract_value / p.win_price_count, 2)
         ELSE 0 END,
    COALESCE(tc.top_categories, '[]'::jsonb),
    COALESCE(tb.top_buyers, '[]'::jsonb),
    COALESCE(p.first_seen, ''),
    COALESCE(p.last_seen, ''),
    now()
FROM profile p
LEFT JOIN name_pick      n  ON n.seller_id = p.seller_id
LEFT JOIN mse_pick       m  ON m.seller_id = p.seller_id
LEFT JOIN top_categories tc ON tc.seller_id = p.seller_id
LEFT JOIN top_buyers     tb ON tb.seller_id = p.seller_id
ON CONFLICT (seller_id) DO UPDATE SET
    seller_name          = EXCLUDED.seller_name,
    seller_name_norm     = EXCLUDED.seller_name_norm,
    mse_status           = EXCLUDED.mse_status,
    total_tenders        = EXCLUDED.total_tenders,
    total_wins           = EXCLUDED.total_wins,
    win_rate             = EXCLUDED.win_rate,
    total_contract_value = EXCLUDED.total_contract_value,
    avg_contract_value   = EXCLUDED.avg_contract_value,
    top_categories       = EXCLUDED.top_categories,
    top_buyers           = EXCLUDED.top_buyers,
    first_seen           = EXCLUDED.first_seen,
    last_seen            = EXCLUDED.last_seen,
    updated_at           = EXCLUDED.updated_at;
