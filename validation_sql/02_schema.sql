-- validation_sql/02_schema.sql
--
-- Additive-only schema setup for the validation phase. Nothing here alters
-- the meaning of an existing column. `staging.tenders_raw` already has
-- `validated` / `validation_errors` from pipeline2/db.py; the only thing
-- ensured on it here is `scrape_error` (normally added by pipeline3/db.py),
-- with ADD COLUMN IF NOT EXISTS, so 03_validate_tenders.sql can always read it.
--
-- staging.bid_participations_raw gets ONE new, inert column:
--   validated_at TIMESTAMPTZ -- marks "this row has already been checked and
--   passed" so 04_validate_participations.sql only re-scans genuinely new
--   rows on every run (mirrors the run/re-run cheapness of
--   pipeline4/sql/01_merge.sql's own `merged_at IS NULL` scoping). Rows that
--   fail are moved to the quarantine table below instead, so they don't need
--   a "failed" state on this column at all.
--
-- This is safe to add without touching pipeline3/pipeline4 code: neither
-- `pipeline3/db.py`'s bulk_insert_participations() nor
-- `pipeline4/sql/01_merge.sql`'s participations INSERT uses `SELECT *` --
-- both name their columns explicitly -- so a new column here is invisible to
-- them until/unless something is deliberately written to reference it.
ALTER TABLE staging.bid_participations_raw
    ADD COLUMN IF NOT EXISTS validated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS merged_at TIMESTAMPTZ;

-- 03_validate_tenders.sql reads scrape_error. pipeline3/db.py normally adds it,
-- but validation must not depend on pipeline3 having run first on this DB.
-- No-op when the column already exists.
ALTER TABLE staging.tenders_raw
    ADD COLUMN IF NOT EXISTS scrape_error TEXT;

-- 03_validate_tenders.sql and pipeline4/sql/05_cleanup.sql read known_at
-- (pipeline2/db.py adds it; same reasoning as scrape_error above).
ALTER TABLE staging.tenders_raw
    ADD COLUMN IF NOT EXISTS known_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_staging_bid_participations_raw_pending_validate
    ON staging.bid_participations_raw (run_id, batch_key)
    WHERE merged_at IS NULL AND validated_at IS NULL;

-- ─────────────────────────────────────────────────────────────────────────
-- staging.bid_participations_quarantine -- where a bad participation row
-- goes instead of `public.bid_participations`. Same column shape as
-- staging.bid_participations_raw, plus why it was pulled and when.
--
-- Rows land here via a physical move (INSERT here, DELETE from
-- staging.bid_participations_raw) inside 04_validate_participations.sql, not
-- a soft flag -- that's what lets pipeline4/sql/01_merge.sql merge the rest
-- of that bid's (valid) participants and the tender itself without any
-- change on its side: the bad row simply isn't in
-- staging.bid_participations_raw anymore for it to find.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.bid_participations_quarantine (
    run_id              UUID        NOT NULL,
    batch_key           TEXT        NOT NULL,
    bid_id              BIGINT      NOT NULL,
    seller_id           TEXT        NOT NULL,
    seller_name         TEXT        NOT NULL DEFAULT '',
    mse_status          TEXT        NOT NULL DEFAULT '',
    rank                TEXT        NOT NULL DEFAULT '',
    rank_num            SMALLINT,
    total_price         NUMERIC(20,2),
    is_winner           BOOLEAN     NOT NULL DEFAULT FALSE,
    winner_id           TEXT        NOT NULL DEFAULT '',
    winner_name         TEXT        NOT NULL DEFAULT '',
    winner_price        NUMERIC(20,2),
    technical_status    TEXT        NOT NULL DEFAULT '',
    outcome             TEXT        NOT NULL DEFAULT 'no_result',
    end_date            TEXT,
    derived_at          TIMESTAMPTZ,
    validation_errors   JSONB       NOT NULL,
    quarantined_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (run_id, bid_id, seller_id)
);

CREATE INDEX IF NOT EXISTS ix_staging_bp_quarantine_batch
    ON staging.bid_participations_quarantine (run_id, batch_key);
