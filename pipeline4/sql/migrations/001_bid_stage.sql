-- pipeline4/sql/migrations/001_bid_stage.sql
--
-- One-time, additive migration for fetching technically / financially evaluated
-- bids alongside awarded ones.
--
--   stage              1 = technical evaluation, 2 = financial evaluation,
--                      3 = awarded. Taken from the GeM `byStatus` list the bid
--                      was found in.
--   is_cancelled       TRUE when GeM's b_status is 3 or 5 (cancelled at that stage).
--   bid_status         GeM's raw b_status code, kept for audit.
--   status_updated_at  when stage / is_cancelled / bid_status were last written.
--
-- Every row already in `tenders` was fetched from GeM's awarded list, so it
-- must read stage = 3. That is done WITHOUT rewriting any row:
--   ADD COLUMN ... DEFAULT 3 with a constant default is metadata-only in
--   PostgreSQL 11+ -- the value is stored once per partition as the column's
--   "missing value" (pg_attribute.attmissingval) and every existing row reads
--   as 3. Dropping the default right after only affects rows inserted LATER
--   (they start NULL; the pipeline always writes its own stage); existing rows
--   keep reading 3. Takes well under a second on ~3M rows, no table rewrite,
--   no dead tuples, no VACUUM needed.
-- is_cancelled / bid_status / status_updated_at stay NULL on old rows until
-- their month is re-run.
--
-- Run with run_001.py (sets a lock_timeout and verifies), or:
--   psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/migrations/001_bid_stage.sql
-- Re-running is a no-op.

BEGIN;
SET LOCAL lock_timeout = '10s';   -- never queue behind long app queries; retry instead

ALTER TABLE tenders ADD COLUMN IF NOT EXISTS stage SMALLINT DEFAULT 3;
ALTER TABLE tenders ALTER COLUMN stage DROP DEFAULT;

ALTER TABLE tenders
    ADD COLUMN IF NOT EXISTS is_cancelled      BOOLEAN,
    ADD COLUMN IF NOT EXISTS bid_status        SMALLINT,
    ADD COLUMN IF NOT EXISTS status_updated_at TIMESTAMPTZ;

COMMIT;
