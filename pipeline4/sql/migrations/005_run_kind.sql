-- pipeline4/sql/migrations/005_run_kind.sql
--
-- One-time migration (after 004): tells monthly runs and backfill batches apart
-- in ops.pipeline_runs. pipeline_main/spot_run.py writes it:
--   scheduled  the monthly run (window M-6 .. M-1)
--   backfill   one batch of the one-time backfill (spot_run.py --backfill)
-- The monthly "this month's run is already done" check counts only scheduled
-- runs, so backfill batches never make the monthly schedule skip a month.
--
-- Existing rows become 'scheduled'. Adding a column with a constant default is
-- instant (no table rewrite). Re-running is a no-op.
--
-- Usage:  psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/migrations/005_run_kind.sql

ALTER TABLE ops.pipeline_runs
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'scheduled'
    CHECK (kind IN ('scheduled', 'backfill'));
