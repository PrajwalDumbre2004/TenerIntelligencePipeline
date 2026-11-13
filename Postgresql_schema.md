# TenderGenie Intelligence — PostgreSQL Schema Design (v2)
### Migration from MongoDB → PostgreSQL | Scale-Optimised for Millions of Tenders

---

## Clarifications Before the Schema

### Q1 — "`bid_participations` looks exactly like `seller_tenders`. Isn't it the same 40M row problem?"

**Yes, 40M rows is correct. No, it is not the same problem.**

The 40M rows is the **irreducible fact volume** — it cannot be collapsed without losing information. Every row represents a distinct real-world event: *seller X participated in tender Y with price Z and rank L2*. That fact exists 40M times in your data.

What changes is **how** PostgreSQL stores and queries those 40M rows vs MongoDB:

| | MongoDB `seller_tenders` | PostgreSQL `bid_participations` |
|---|---|---|
| **Storage overhead** | Full tender metadata duplicated per seller (category, buyer, dates stored 13× per tender) | Tender metadata lives ONCE in `tenders` (3.1M rows). `bid_participations` stores ONLY per-participant columns |
| **Join capability** | No real joins. Pre-duplication was the workaround | `JOIN tenders USING (bid_id)` — single-hop, index-backed, sub-ms |
| **Aggregate queries** | Must pre-compute `seller_buyers`, `seller_categories` ETL jobs | `GROUP BY` on 40M rows with covering indexes → done in milliseconds |
| **RAM issue** | SQLite buffer needed because all 40M docs couldn't fit in RAM | PostgreSQL hash-joins/sort-spill to disk natively. No workaround needed |
| **Competitor analysis** | Batch scan of seller_tenders → Python loop → ThreadPoolExecutor | Single self-JOIN on `bid_participations` with two indexes |

**In MongoDB the duplication was a necessity. In PostgreSQL it becomes a design choice — and we choose NOT to duplicate.**

---

### Q2 — "Where are `seller_categories` and `seller_buyers` derived from?"

Both are derived from `bid_participations` (joined with `tenders` for category/buyer metadata). In the current MongoDB pipeline (`derived_claude.py`):

```python
# seller_categories: for each seller, group their seller_tenders by category
for t in data["tenders"]:            # = their rows in seller_tenders
    category = t.get("category")
    stats[category]["tenders_participated"] += 1
    if t["is_winner"]: stats[category]["tenders_won"] += 1
    ...

# seller_buyers: same, grouped by buyer_name
for t in data["tenders"]:
    buyer_id = t.get("buyer_name").upper()
    stats[buyer_id]["tenders_participated"] += 1
    ...
```

**The source is always `seller_tenders` (participation rows), not the raw tenders.**

In PostgreSQL, these become **Materialized Views** — `CREATE MATERIALIZED VIEW` + `REFRESH MATERIALIZED VIEW CONCURRENTLY` after each ETL run. The GROUP BY query replaces the Python loop entirely.

---

## Scale Numbers

```
Current data:
  tenders (raw)           →  3,100,000  rows   (3.1M unique bid_ids)
  sellers                 →    306,000  rows   (3.06 lakh unique sellers)
  bid_participations      → 40,000,000  rows   (unavoidable, ~13 participants/tender avg)
  seller_categories (MV)  →  ~2-6M      rows   (306K sellers × avg ~10-20 unique categories each)
  seller_buyers (MV)      →  ~5-15M     rows   (306K sellers × avg ~20-50 unique buyers each)
  seller_competitors      →  ~30-100M   rows   (if storing all pairs) OR kept as JSONB doc
```

---

## Full DDL

### 1. Auth & Multi-Tenancy

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- tg_organizations
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE tg_organizations (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    external_org_id     TEXT        NOT NULL,
    name                TEXT        NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_tg_organizations_external_org_id UNIQUE (external_org_id)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- app_users
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE app_users (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    email               TEXT        NOT NULL,
    first_name          TEXT        NOT NULL DEFAULT '',
    last_name           TEXT        NOT NULL DEFAULT '',
    full_name           TEXT        NOT NULL DEFAULT '',
    password_hash       TEXT,
    signup_source       TEXT        NOT NULL DEFAULT 'direct',   -- 'direct' | 'tg_handoff'
    org_id              UUID        REFERENCES tg_organizations(id) ON DELETE SET NULL,
    external_user_id    TEXT,
    is_active           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_app_users_email UNIQUE (email)
);

-- Partial unique index: mirrors MongoDB sparse unique index (NULL ≠ unique violation)
CREATE UNIQUE INDEX uix_app_users_external_user_id
    ON app_users (external_user_id)
    WHERE external_user_id IS NOT NULL;

-- ─────────────────────────────────────────────────────────────────────────────
-- password_reset_tokens
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE password_reset_tokens (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID        NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    token_hash  TEXT        NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    used        BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_password_reset_tokens_hash UNIQUE (token_hash)
);

CREATE INDEX ix_password_reset_tokens_user_id ON password_reset_tokens (user_id);
```

---

### 2. Raw Tenders (3.1M rows, partitioned by year)

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- tenders  — ONE ROW PER UNIQUE BID (3.1M rows)
--
-- Replaces: TenderIntelligence.tenders_YYYY (year-sharded MongoDB collections)
--
-- KEY DESIGN DECISION:
--   All tender metadata (category, buyer_name, buyer_department, ministry,
--   month, end_date, bid_result) lives HERE and only here.
--
--   bid_participations stores ONLY the per-seller result (rank, price, outcome).
--   This eliminates the 13× duplication of category/buyer/date that exists
--   in the current MongoDB seller_tenders collection.
--
-- PARTITIONING: RANGE by tender_year
--   3.1M tenders split across ~7 year partitions ≈ ~440K rows/partition.
--   Queries with end_date filters get automatic partition pruning.
--   Each partition is independently B-tree indexed and VACUUM'd.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE tenders (
    id                  BIGSERIAL,
    bid_id              BIGINT      NOT NULL,
    bid_id_parent       BIGINT,
    bid_number          TEXT        NOT NULL DEFAULT '',
    bid_number_parent   TEXT        NOT NULL DEFAULT '',
    ra_number           TEXT        NOT NULL DEFAULT '',
    tender_year         SMALLINT    NOT NULL,               -- partition key
    category            TEXT        NOT NULL DEFAULT '',
    department          TEXT        NOT NULL DEFAULT '',
    ministry            TEXT        NOT NULL DEFAULT '',
    month               TEXT        NOT NULL DEFAULT '',    -- "YYYY-MM"
    end_date            DATE,
    buyer_name          TEXT        NOT NULL DEFAULT '',
    buyer_department    TEXT        NOT NULL DEFAULT '',
    is_ra_bid           BOOLEAN     NOT NULL DEFAULT FALSE,
    scraped_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Raw evaluation blobs (for detail views only; not used in analytics queries)
    bid_result_raw      JSONB,
    ra_result_raw       JSONB,
    buyer_details_raw   JSONB,

    PRIMARY KEY (id, tender_year)
) PARTITION BY RANGE (tender_year);

-- Year partitions
CREATE TABLE tenders_2020 PARTITION OF tenders FOR VALUES FROM (2020) TO (2021);
CREATE TABLE tenders_2021 PARTITION OF tenders FOR VALUES FROM (2021) TO (2022);
CREATE TABLE tenders_2022 PARTITION OF tenders FOR VALUES FROM (2022) TO (2023);
CREATE TABLE tenders_2023 PARTITION OF tenders FOR VALUES FROM (2023) TO (2024);
CREATE TABLE tenders_2024 PARTITION OF tenders FOR VALUES FROM (2024) TO (2025);
CREATE TABLE tenders_2025 PARTITION OF tenders FOR VALUES FROM (2025) TO (2026);
CREATE TABLE tenders_2026 PARTITION OF tenders FOR VALUES FROM (2026) TO (2027);

-- Unique constraint: one tender per bid_id per year
CREATE UNIQUE INDEX uix_tenders_bid_id_year    ON tenders (bid_id, tender_year);
-- Lookup by bid_id alone (cross-partition, used when year is unknown)
CREATE INDEX ix_tenders_bid_id                 ON tenders (bid_id);
-- Covering index for bid category
CREATE INDEX ix_tenders_bid_category           ON tenders (bid_id) INCLUDE (category);
-- For category/buyer aggregate queries (GROUP BY on tenders side of JOIN)
CREATE INDEX ix_tenders_category               ON tenders (category);
CREATE INDEX ix_tenders_buyer_name             ON tenders (buyer_name);
-- Incremental ETL watermark
CREATE INDEX ix_tenders_scraped_at             ON tenders (scraped_at);
```

---

### 3. Sellers (306K rows)

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- sellers  — ONE ROW PER UNIQUE SELLER (306K rows)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE sellers (
    seller_id               TEXT        PRIMARY KEY,
    seller_name             TEXT        NOT NULL DEFAULT '',
    seller_name_norm        TEXT        NOT NULL DEFAULT '',
    mse_status              TEXT        NOT NULL DEFAULT '',

    -- Pre-computed summary counters (updated by ETL, not by the API)
    total_tenders           INT         NOT NULL DEFAULT 0,
    total_wins              INT         NOT NULL DEFAULT 0,
    win_rate                NUMERIC(5,2) NOT NULL DEFAULT 0,
    total_contract_value    NUMERIC(20,2) NOT NULL DEFAULT 0,
    avg_contract_value      NUMERIC(20,2) NOT NULL DEFAULT 0,

    -- Top-N embedded JSON arrays (lightweight, avoids extra table joins for profile cards)
    -- Format: [{category, count, wins}, ...]  — rebuilt by ETL GROUP BY
    top_categories          JSONB       NOT NULL DEFAULT '[]',
    -- Format: [{buyer_name, count, wins}, ...]
    top_buyers              JSONB       NOT NULL DEFAULT '[]',

    first_seen              TEXT        NOT NULL DEFAULT '',   -- "YYYY-MM"
    last_seen               TEXT        NOT NULL DEFAULT '',
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Covering index for fast profile queries without heap lookup
CREATE INDEX ix_sellers_covering ON sellers (seller_id) INCLUDE (seller_name, mse_status, total_tenders);

-- Prefix search on seller name (replaces MongoDB $regex ^{search})
-- text_pattern_ops is REQUIRED for LIKE 'prefix%' to use the index
CREATE INDEX ix_sellers_name_norm_prefix ON sellers (seller_name_norm text_pattern_ops);

-- Full-text search (optional upgrade path for fuzzy matching)
CREATE INDEX ix_sellers_name_fts ON sellers
    USING GIN (to_tsvector('simple', seller_name_norm));

-- Sort support for search results (sort by total_tenders DESC)
CREATE INDEX ix_sellers_total_tenders ON sellers (total_tenders DESC);
```

---

### 4. Bid Participations — The Junction Table (40M rows, partitioned)

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- bid_participations  — ONE ROW PER SELLER × TENDER PARTICIPATION (40M rows)
--
-- This is the junction table between sellers (306K) and tenders (3.1M).
-- It stores ONLY columns that are per-participant:
--   rank, total_price, is_winner, outcome, technical_status, mse_status
--
-- Columns NOT stored here (they live in tenders, accessed via JOIN):
--   category, buyer_name, buyer_department, ministry, month
--
-- EXCEPTION — end_date is intentionally DUPLICATED here:
--   The most common API filter is "seller's tenders in date range". Without
--   end_date in this table, every such query requires a JOIN to tenders.
--   At 40M rows, this denormalization saves a JOIN on the hot path.
--   This is a deliberate trade-off, not an oversight.
--
-- WHY 40M ROWS IS NOT A PROBLEM IN POSTGRESQL:
--   • Partitioned into ~7 year partitions → ~5.7M rows per partition
--   • All hot queries hit at most 1-2 partitions (date range filter)
--   • JOINs to tenders (3.1M rows) are hash-join or merge-join,
--     both disk-spilling by design — no RAM OOM risk
--   • Index-only scans avoid heap reads for many common queries
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE bid_participations (
    id                  BIGSERIAL,
    seller_id           TEXT        NOT NULL REFERENCES sellers(seller_id),
    bid_id              BIGINT      NOT NULL,               -- FK to tenders.bid_id
    participation_year  SMALLINT    NOT NULL,               -- partition key (= year of end_date)

    -- PER-PARTICIPANT RESULT COLUMNS (not duplicated from tenders)
    rank                TEXT        NOT NULL DEFAULT '',    -- "L1", "L2", etc.
    rank_num            SMALLINT,                           -- integer equivalent of rank
    total_price         NUMERIC(20,2),
    is_winner           BOOLEAN     NOT NULL DEFAULT FALSE,
    winner_id           TEXT        NOT NULL DEFAULT '',    -- seller_id of the winner in this bid
    winner_name         TEXT        NOT NULL DEFAULT '',
    winner_price        NUMERIC(20,2),
    technical_status    TEXT        NOT NULL DEFAULT '',
    outcome             TEXT        NOT NULL DEFAULT 'no_result',   -- 'won'|'lost'|'disqualified'|'no_result'
    seller_name         TEXT        NOT NULL DEFAULT '',    -- display name at time of participation
    mse_status          TEXT        NOT NULL DEFAULT '',

    -- INTENTIONAL DENORMALIZATION: end_date copied from tenders
    -- Reason: avoids JOIN to tenders on the primary query path (seller tenders list with date filter)
    end_date            DATE,

    PRIMARY KEY (id, participation_year)
) PARTITION BY RANGE (participation_year);

-- Year partitions (mirror tenders)
CREATE TABLE bid_participations_2020 PARTITION OF bid_participations FOR VALUES FROM (2020) TO (2021);
CREATE TABLE bid_participations_2021 PARTITION OF bid_participations FOR VALUES FROM (2021) TO (2022);
CREATE TABLE bid_participations_2022 PARTITION OF bid_participations FOR VALUES FROM (2022) TO (2023);
CREATE TABLE bid_participations_2023 PARTITION OF bid_participations FOR VALUES FROM (2023) TO (2024);
CREATE TABLE bid_participations_2024 PARTITION OF bid_participations FOR VALUES FROM (2024) TO (2025);
CREATE TABLE bid_participations_2025 PARTITION OF bid_participations FOR VALUES FROM (2025) TO (2026);
CREATE TABLE bid_participations_2026 PARTITION OF bid_participations FOR VALUES FROM (2026) TO (2027);

-- ── INDEXES ──────────────────────────────────────────────────────────────────

-- UPSERT KEY (replaces MongoDB ReplaceOne keyed on seller_id+bid_id)
-- MUST include participation_year: PostgreSQL requires every unique index on a
-- partitioned table to include the partition key column, or CREATE UNIQUE INDEX
-- fails outright. ETL upserts must target ON CONFLICT (seller_id, bid_id, participation_year).
CREATE UNIQUE INDEX uix_bp_seller_bid
    ON bid_participations (seller_id, bid_id, participation_year);

-- PRIMARY QUERY: seller's tenders with optional date filter
-- Covers: GET /sellers/{id}/tenders  (no join to tenders needed)
CREATE INDEX ix_bp_seller_date
    ON bid_participations (seller_id, end_date DESC);

-- OUTCOME FILTER: same endpoint, outcome param
CREATE INDEX ix_bp_seller_outcome_date
    ON bid_participations (seller_id, outcome, end_date DESC);

-- SORT BY PRICE variant
CREATE INDEX ix_bp_seller_price
    ON bid_participations (seller_id, total_price DESC NULLS LAST);

-- PARTICIPANTS LOOKUP: all sellers in a bid — GET /tenders/{bid_id}/participants
CREATE INDEX ix_bp_bid_id
    ON bid_participations (bid_id);

-- COMPETITOR OVERLAP: the most critical competitor query index
-- Finds shared bids between two sellers: WHERE bid_id = ANY(...) AND seller_id = X
CREATE INDEX ix_bp_bid_seller
    ON bid_participations (bid_id, seller_id);

-- WINNER-ONLY PARTIAL INDEX: fast win-rate stats without scanning losers
CREATE INDEX ix_bp_seller_winner
    ON bid_participations (seller_id, end_date DESC)
    WHERE is_winner = TRUE;
```

---

### 5. Materialized Views — `seller_categories` and `seller_buyers`

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- seller_categories  — ONE ROW PER SELLER × CATEGORY
--
-- Derived from: bid_participations JOIN tenders USING (bid_id)
-- Source in MongoDB: derived_claude.py build_seller_categories()
--   → iterates seller_tenders rows, groups by category, counts participations/wins
--
-- In PostgreSQL: the exact same aggregation runs as a GROUP BY query.
--   MATERIALIZED VIEW is refreshed after each ETL run (typically daily/weekly).
--   REFRESH MATERIALIZED VIEW CONCURRENTLY means live queries are not blocked.
--
-- Estimated size: 306K sellers × avg ~15 categories = ~4.6M rows
-- ─────────────────────────────────────────────────────────────────────────────
-- NOTE ON `monthly`: the previous draft joined a per-row LATERAL subquery before
-- the outer GROUP BY, so it re-ran once per participation row and jsonb_agg
-- collected N duplicate copies of the month breakdown instead of one clean
-- array (and it bucketed by raw end_date, i.e. per-day, not per-month).
-- Fixed by pre-aggregating into the `monthly` CTE ONCE per (seller_id, category,
-- month) using the existing tenders.month ("YYYY-MM") column, then pulling each
-- group's slice via a correlated subquery that runs once per output row, not
-- once per input row.
CREATE MATERIALIZED VIEW seller_categories AS
WITH monthly AS (
    SELECT
        bp.seller_id,
        t.category,
        t.month,
        COUNT(*)                                     AS participated,
        COUNT(*) FILTER (WHERE bp.is_winner)         AS won
    FROM bid_participations bp
    JOIN tenders t USING (bid_id)
    WHERE t.category <> '' AND t.month <> ''
    GROUP BY bp.seller_id, t.category, t.month
)
SELECT
    bp.seller_id,
    t.category,

    COUNT(*)                                                    AS tenders_participated,
    COUNT(*) FILTER (WHERE bp.is_winner)                        AS tenders_won,
    ROUND(
        COUNT(*) FILTER (WHERE bp.is_winner) * 100.0 / COUNT(*),
    1)                                                          AS win_rate,

    -- total_value_won: sum of winner's price (matches build_seller_categories total_value logic)
    COALESCE(SUM(bp.total_price) FILTER (WHERE bp.is_winner), 0) AS total_value_won,

    -- avg_price: average across ALL participations in this category (won or lost)
    ROUND(AVG(bp.total_price) FILTER (WHERE bp.total_price IS NOT NULL AND bp.total_price > 0), 2)
                                                                AS avg_price,

    -- monthly breakdown stored as JSONB array: [{month, participated, won}, ...]
    -- Matches: build_seller_categories stats["monthly"] structure
    (
        SELECT jsonb_agg(
            jsonb_build_object('month', m.month, 'participated', m.participated, 'won', m.won)
            ORDER BY m.month
        )
        FROM monthly m
        WHERE m.seller_id = bp.seller_id AND m.category = t.category
    )                                                           AS monthly,

    now()                                                       AS updated_at

FROM bid_participations bp
JOIN tenders t USING (bid_id)
WHERE t.category <> ''
GROUP BY bp.seller_id, t.category;

-- Indexes on the materialized view
CREATE UNIQUE INDEX uix_sc_seller_category ON seller_categories (seller_id, category);
CREATE INDEX ix_sc_seller_participated ON seller_categories (seller_id, tenders_participated DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- seller_buyers  — ONE ROW PER SELLER × BUYER
--
-- Derived from: bid_participations JOIN tenders USING (bid_id)
-- Source in MongoDB: derived_claude.py build_seller_buyers()
--   → iterates seller_tenders rows, groups by buyer_name, counts participations/wins
--
-- Estimated size: 306K sellers × avg ~30 buyers = ~9M rows
-- ─────────────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW seller_buyers AS
SELECT
    bp.seller_id,
    t.buyer_name,
    t.buyer_name                                                AS buyer_id,   -- normalized key
    -- buyer_department is descriptive metadata, NOT part of the grouping key.
    -- buyer_id (= buyer_name) is what uix_sb_seller_buyer treats as one buyer;
    -- if grouped alongside buyer_name, two department spellings for the same
    -- buyer_name would produce two rows sharing one buyer_id and break that
    -- unique index. Aggregate it instead of grouping on it.
    MAX(t.buyer_department)                                     AS buyer_department,

    COUNT(*)                                                    AS tenders_participated,
    COUNT(*) FILTER (WHERE bp.is_winner)                        AS tenders_won,
    ROUND(
        COUNT(*) FILTER (WHERE bp.is_winner) * 100.0 / COUNT(*),
    1)                                                          AS win_rate,

    COALESCE(SUM(bp.total_price) FILTER (WHERE bp.is_winner), 0) AS total_value_won,

    -- categories this seller has bid for with this buyer
    -- Matches: build_seller_buyers stats["categories"] = sorted(set of categories)
    array_agg(DISTINCT t.category) FILTER (WHERE t.category <> '') AS categories,

    MAX(t.month) FILTER (WHERE t.month <> '')                   AS last_participated,
    MAX(t.month) FILTER (WHERE bp.is_winner AND t.month <> '')  AS last_won,

    now()                                                       AS updated_at

FROM bid_participations bp
JOIN tenders t USING (bid_id)
WHERE t.buyer_name <> ''
GROUP BY bp.seller_id, t.buyer_name;

-- Indexes on the materialized view
CREATE UNIQUE INDEX uix_sb_seller_buyer     ON seller_buyers (seller_id, buyer_id);
CREATE INDEX ix_sb_seller_participated      ON seller_buyers (seller_id, tenders_participated DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- HOW TO REFRESH (after each ETL run)
-- ─────────────────────────────────────────────────────────────────────────────
-- CONCURRENTLY: live queries continue reading old data while refresh runs
-- Requires the unique index (uix_sc_seller_category, uix_sb_seller_buyer) to exist first.
--
--   REFRESH MATERIALIZED VIEW CONCURRENTLY seller_categories;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY seller_buyers;
--
-- Estimated refresh time at 40M participation rows: 5-20 minutes (single pass GROUP BY).
-- Much faster than the current multi-hour ETL with Python loops + MongoDB writes.
```

---

### 6. Pre-Computed Competitor Cache (Optional)

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- seller_competitors  — precomputed all-time competitor rankings
--
-- WHY KEEP IT:
--   The /competitors/summary endpoint has a "no date filter" fast path.
--   A self-JOIN on 40M bid_participations rows (finding all sellers who shared
--   bids with seller X) is fast with indexes but can take 1-5 seconds for
--   sellers with many tenders. The cache gives <50ms.
--
-- WHY NOT A MATERIALIZED VIEW:
--   A full self-join materialized view would be:
--     SELECT DISTINCT bp1.seller_id, bp2.seller_id, COUNT(*)
--     FROM bid_participations bp1 JOIN bid_participations bp2 USING (bid_id)
--     WHERE bp1.seller_id <> bp2.seller_id
--     GROUP BY bp1.seller_id, bp2.seller_id
--   At 40M rows this produces potentially BILLIONS of pairs (O(n²) join).
--   Instead, reuse the current pattern: compute per-seller and store rows.
--
-- Estimated size: 306K sellers × avg ~100 competitors each = ~30M rows
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE seller_competitors (
    seller_id           TEXT        NOT NULL REFERENCES sellers(seller_id),
    competitor_id       TEXT        NOT NULL REFERENCES sellers(seller_id),
    shared_tenders      INT         NOT NULL DEFAULT 0,
    refreshed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (seller_id, competitor_id)
);

CREATE INDEX ix_sc_seller_count ON seller_competitors (seller_id, shared_tenders DESC);
```

---

### 7. Supporting Tables

```sql
-- ─────────────────────────────────────────────────────────────────────────────
-- watched_sellers
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE watched_sellers (
    seller_id   TEXT        PRIMARY KEY,
    is_active   BOOLEAN     NOT NULL DEFAULT TRUE,
    added_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- pipeline_refresh_state  — incremental ETL watermark
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE pipeline_refresh_state (
    source_collection   TEXT        PRIMARY KEY,
    last_processed_at   TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Data Flow: Where Everything Comes From

```
RAW SOURCE (MongoDB → migrated once)
    TenderIntelligence.tenders_YYYY
           │
           ▼
    tenders (3.1M rows, PostgreSQL)
    [bid_id, category, buyer_name, buyer_dept, end_date, month, bid_result_raw, ...]
           │
           │  ETL: extract_participants() logic applied per tender
           │  → 13 participation rows per tender on average
           ▼
    bid_participations (40M rows, PostgreSQL)
    [seller_id, bid_id, rank, price, is_winner, outcome, end_date (copy)]
           │
           ├──── REFRESH MATERIALIZED VIEW seller_categories
           │     GROUP BY seller_id, category   → ~4.6M rows
           │
           ├──── REFRESH MATERIALIZED VIEW seller_buyers
           │     GROUP BY seller_id, buyer_name → ~9M rows
           │
           ├──── Aggregate query → sellers (306K rows)
           │     total_tenders, win_rate, top_categories (top 10 via JSONB)
           │
           └──── Per-seller self-join loop → seller_competitors (~30M rows)
                 (same pattern as current build_competitors.py, in PostgreSQL)
```

---

## Reference SQL for Every Endpoint

### `GET /sellers` (search)
```sql
SELECT seller_id, seller_name, mse_status, total_tenders
FROM sellers
WHERE seller_name_norm LIKE $1 || '%'
ORDER BY total_tenders DESC
LIMIT $2;
-- Uses: ix_sellers_name_norm_prefix (text_pattern_ops) + ix_sellers_total_tenders
```

### `GET /sellers/{seller_id}`
```sql
SELECT * FROM sellers WHERE seller_id = $1;
-- Uses: PRIMARY KEY — single row lookup, ~1ms
```

### `GET /sellers/{seller_id}/tenders` (listing with filters)
```sql
SELECT
    bp.bid_id, t.bid_number, t.bid_number_parent, t.ra_number,
    t.category, t.buyer_name, t.buyer_department, t.month,
    bp.end_date, bp.rank, bp.total_price,
    bp.is_winner, bp.technical_status, bp.outcome
FROM bid_participations bp
JOIN tenders t USING (bid_id)
WHERE bp.seller_id = $1
  AND ($2::DATE IS NULL OR bp.end_date >= $2)       -- uses ix_bp_seller_date
  AND ($3::DATE IS NULL OR bp.end_date <= $3)
  AND ($4::TEXT[] IS NULL OR bp.outcome = ANY($4))  -- uses ix_bp_seller_outcome_date
ORDER BY bp.end_date DESC, bp.bid_id DESC            -- or total_price DESC
OFFSET $5 LIMIT $6;
```

> **Why not query `tenders` directly for date filter?**
> `end_date` is copied into `bid_participations` so this query doesn't need a JOIN for the WHERE clause — only the SELECT columns need the `tenders` JOIN, which PostgreSQL resolves efficiently with a nested loop on the indexed primary key after filtering.

### `GET /tenders/{bid_id}/participants`
```sql
SELECT
    bp.seller_id, bp.seller_name, bp.mse_status,
    bp.rank, bp.total_price, bp.is_winner,
    bp.technical_status, bp.outcome
FROM bid_participations bp
WHERE bp.bid_id = $1
ORDER BY
    CASE technical_status
        WHEN 'QUALIFIED' THEN 0
        WHEN ''          THEN 1
        ELSE 2 END,
    rank_num ASC NULLS LAST,
    total_price ASC NULLS LAST;
-- Uses: ix_bp_bid_id → single bid_id, ~13 rows returned
```

### `GET /sellers/{seller_id}/analytics`
```sql
-- Summary
SELECT
    COUNT(*)                            AS total_tenders,
    COUNT(*) FILTER (WHERE is_winner)   AS total_wins
FROM bid_participations
WHERE seller_id = $1
  AND ($2::DATE IS NULL OR end_date >= $2)
  AND ($3::DATE IS NULL OR end_date <= $3);

-- Monthly breakdown
SELECT t.month,
       COUNT(*)                             AS tenders,
       COUNT(*) FILTER (WHERE bp.is_winner) AS wins
FROM bid_participations bp
JOIN tenders t USING (bid_id)
WHERE bp.seller_id = $1
  AND ($2::DATE IS NULL OR bp.end_date >= $2)
  AND ($3::DATE IS NULL OR bp.end_date <= $3)
GROUP BY t.month ORDER BY t.month;

-- Category breakdown
SELECT t.category,
       COUNT(*)                             AS tenders,
       COUNT(*) FILTER (WHERE bp.is_winner) AS wins
FROM bid_participations bp
JOIN tenders t USING (bid_id)
WHERE bp.seller_id = $1
  AND ($2::DATE IS NULL OR bp.end_date >= $2)
  AND ($3::DATE IS NULL OR bp.end_date <= $3)
GROUP BY t.category ORDER BY tenders DESC;
```

### `GET /sellers/{seller_id}/competitors/summary`
```sql
-- Fast path: no date filter → read from seller_competitors cache
SELECT
    sc.competitor_id,
    s.seller_name,
    s.mse_status,
    s.total_tenders,
    sc.shared_tenders
FROM seller_competitors sc
JOIN sellers s ON s.seller_id = sc.competitor_id
WHERE sc.seller_id = $1
ORDER BY sc.shared_tenders DESC;
-- Uses: ix_sc_seller_count → single document lookup ~1ms

-- Date-filtered path → dynamic query
SELECT
    bp2.seller_id           AS competitor_id,
    s.seller_name,
    s.mse_status,
    s.total_tenders,
    COUNT(*)                AS shared_tenders
FROM bid_participations bp1
JOIN bid_participations bp2 USING (bid_id)   -- Uses: ix_bp_bid_seller on bp2 side
LEFT JOIN sellers s ON s.seller_id = bp2.seller_id
WHERE bp1.seller_id = $1
  AND bp2.seller_id <> $1
  AND bp1.end_date BETWEEN $2 AND $3          -- Uses: ix_bp_seller_date on bp1 side
GROUP BY bp2.seller_id, s.seller_name, s.mse_status, s.total_tenders
ORDER BY shared_tenders DESC;
```

### `GET /sellers/{my_id}/vs/{comp_id}` (one-vs-one)
```sql
SELECT
    bp1.bid_id,
    t.bid_number,
    t.end_date,
    t.category,
    t.buyer_department,
    t.month,
    bp1.rank          AS my_rank,
    bp2.rank          AS comp_rank,
    bp1.total_price   AS my_price,
    bp2.total_price   AS comp_price,
    bp1.is_winner     AS i_won,
    bp2.is_winner     AS they_won,
    bp1.technical_status AS my_tech_status,
    bp2.technical_status AS comp_tech_status
FROM bid_participations bp1
JOIN bid_participations bp2 USING (bid_id)    -- Uses: ix_bp_bid_seller on bp2
JOIN tenders t USING (bid_id)
WHERE bp1.seller_id = $1
  AND bp2.seller_id = $2
ORDER BY t.end_date DESC;
-- All data for scoreboard, monthly_trend, category_overlap, dept_overlap
-- from a single query. No Python-level loops needed.
```

---

## Index Summary

| Table | Index | Columns | Supports |
|---|---|---|---|
| `tg_organizations` | `uq_external_org_id` | `external_org_id` | SSO org lookup |
| `app_users` | `uq_email` | `email` | login |
| `app_users` | `uix_external_user_id` | `external_user_id` WHERE NOT NULL | SSO user lookup |
| `password_reset_tokens` | `uq_token_hash` | `token_hash` | reset flow |
| `password_reset_tokens` | `ix_user_id` | `user_id` | user's tokens |
| `tenders` | `uix_bid_id_year` | `(bid_id, tender_year)` | ETL dedup |
| `tenders` | `ix_bid_id` | `bid_id` | cross-year bid lookup |
| `tenders` | `ix_tenders_bid_category` | `bid_id INCLUDE (category)` | covering index |
| `tenders` | `ix_category` | `category` | JOIN + GROUP BY |
| `tenders` | `ix_buyer_name` | `buyer_name` | JOIN + GROUP BY |
| `tenders` | `ix_scraped_at` | `scraped_at` | incremental ETL |
| `sellers` | (PK) | `seller_id` | exact lookup |
| `sellers` | `ix_sellers_covering` | `seller_id INCLUDE (seller_name, mse_status, total_tenders)` | covering index |
| `sellers` | `ix_name_norm_prefix` | `seller_name_norm text_pattern_ops` | **prefix search** |
| `sellers` | `ix_name_fts` GIN | `to_tsvector(seller_name_norm)` | fuzzy search |
| `sellers` | `ix_total_tenders` | `total_tenders DESC` | search ranking |
| `bid_participations` | `uix_seller_bid` | `(seller_id, bid_id, participation_year)` | **upsert dedup key** |
| `bid_participations` | `ix_seller_date` | `(seller_id, end_date DESC)` | **primary hot query** |
| `bid_participations` | `ix_seller_outcome_date` | `(seller_id, outcome, end_date DESC)` | outcome filter |
| `bid_participations` | `ix_seller_price` | `(seller_id, total_price DESC)` | price sort |
| `bid_participations` | `ix_bid_id` | `bid_id` | participants lookup |
| `bid_participations` | `ix_bid_seller` | `(bid_id, seller_id)` | **competitor overlap JOIN** |
| `bid_participations` | `ix_seller_winner` (partial) | `(seller_id, end_date) WHERE is_winner` | win stats |
| `seller_categories` (MV) | `uix_sc_seller_category` | `(seller_id, category)` | lookup |
| `seller_buyers` (MV) | `uix_sb_seller_buyer` | `(seller_id, buyer_id)` | lookup |
| `seller_competitors` | (PK) | `(seller_id, competitor_id)` | cache key |
| `seller_competitors` | `ix_sc_seller_count` | `(seller_id, shared_tenders DESC)` | top-N |

---

## Migration Phases

| Phase | Action | Rows |
|---|---|---|
| 1 | Create schema (DDL above) | — |
| 2 | Migrate `tg_organizations`, `app_users`, `password_reset_tokens` | Small |
| 3 | Bulk-load `tenders` from MongoDB (one partition per year) | 3.1M |
| 4 | Run `extract_participants()` → `INSERT INTO bid_participations` per year | 40M |
| 5 | `REFRESH MATERIALIZED VIEW seller_categories` | ~4.6M |
| 6 | `REFRESH MATERIALIZED VIEW seller_buyers` | ~9M |
| 7 | Aggregate `bid_participations` → `INSERT INTO sellers` | 306K |
| 8 | Run competitor build per-seller → `seller_competitors` | ~30M |
| 9 | Run verification queries; compare with MongoDB counts |  |
| 10 | Switch FastAPI (`database.py`) to `asyncpg` / `psycopg3` |  |
| 11 | Decommission MongoDB | |

---

> [!IMPORTANT]
> **`end_date` duplication is intentional.** Only `end_date` is duplicated into `bid_participations` from `tenders`. Category, buyer_name, buyer_department, month, ministry are NOT duplicated — they are always fetched via `JOIN tenders USING (bid_id)`. This single duplication eliminates the JOIN on the primary hot query path (seller tenders list with date filter).

> [!WARNING]
> **`seller_competitors` is a table, not a materialized view**, because a full self-JOIN on 40M bid_participations rows produces an O(n²) result set. Use the per-seller loop pattern from `build_competitors.py` but targeting PostgreSQL with `COPY` or batch `INSERT ... ON CONFLICT DO UPDATE`.

> [!TIP]
> **`REFRESH MATERIALIZED VIEW CONCURRENTLY`** lets the app read stale-but-valid data from `seller_categories` and `seller_buyers` while the refresh runs. This requires the `UNIQUE INDEX` to exist on the view before the first `CONCURRENTLY` refresh. The first ever refresh must be done **without** `CONCURRENTLY`.
