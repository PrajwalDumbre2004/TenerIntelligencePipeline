-- pipeline4/sql/00_run_all.sql
--
-- Runs the per-batch pipeline4 sequence end to end: validate (Step B), merge
-- (Step C), then the two impacted-seller-scoped aggregate steps (Step D1,
-- D3).
--
-- STEP 0 -- VALIDATE, ALWAYS FIRST, ITS OWN TRANSACTION:
--   ../../validation_sql/00_run_validate.sql runs (and fully commits) before
--   anything below even starts. It flips `staging.tenders_raw.validated` to a
--   real answer (it's `FALSE` until this runs) and physically removes bad
--   `staging.bid_participations_raw` rows into
--   `staging.bid_participations_quarantine` -- see that file's header for the
--   full rationale. This is a SEPARATE transaction from the merge below (its
--   own file has its own BEGIN/COMMIT), not folded into the one outer
--   transaction that wraps 01/02/03 -- deliberately: validation's decisions
--   should survive even if the merge transaction below later fails and rolls
--   back for an unrelated reason (e.g. a bug in 02/03), so a retry of this
--   script doesn't have to redo work that already succeeded. Because
--   `-v ON_ERROR_STOP=1` (see Usage below) aborts the ENTIRE psql invocation
--   the instant any statement anywhere errors, a broken validation run
--   never falls through to the merge transaction that follows it -- this is
--   what guarantees `01_merge.sql` never reads a half-computed
--   `validated` column, and never merges a tender or participation this
--   batch hasn't actually finished judging.
--
-- STEP 1 -- MERGE + AGGREGATES, ONE TRANSACTION:
--   01/02/03 are wrapped in ONE transaction here (BEGIN ... COMMIT spanning
--   all three \ir'd files) so they commit or roll back as a single unit --
--   e.g. if 03_seller_competitors.sql fails, 01_merge.sql's inserts and
--   02_sellers_profile.sql's upserts are rolled back too, not just the
--   failing step. None of the three files may contain their own
--   BEGIN/COMMIT: a nested COMMIT would commit this outer transaction early
--   instead of just its step. `01_merge.sql` only ever reads rows validation
--   marked `validated = TRUE` (tenders) or left behind in
--   `staging.bid_participations_raw` (participations, because the bad ones
--   were already physically removed above) -- so nothing this transaction
--   inserts into `public.tenders` / `public.bid_participations` can be data
--   validation flagged as wrong. No new condition needed here to enforce
--   that; the input set validation hands off is already clean.
--
-- 04_refresh_views.sql runs AFTER the COMMIT, as a separate, non-transactional
-- step: REFRESH MATERIALIZED VIEW CONCURRENTLY is not allowed inside a
-- transaction block, and it's also a full rebuild we don't want blocking (or
-- being rolled back by) the merge transaction above. It only runs if the
-- COMMIT above succeeded -- see that file's header for why it's still safe to
-- treat as "automatic" here even though it isn't part of the same transaction.
--
-- Usage:
--   psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/00_run_all.sql
--
-- (-v ON_ERROR_STOP=1 makes psql abort the whole script on the first SQL
-- error instead of plowing ahead with a half-applied batch. Because nothing
-- commits until the COMMIT below, aborting mid-way -- for any of 01/02/03 --
-- leaves the transaction open and unfinished; psql exiting on that error then
-- causes Postgres to roll the whole thing back automatically. The same flag
-- is what stops a failed validation step from ever reaching this file's own
-- BEGIN; below.)

\ir ../../validation_sql/00_run_validate.sql

BEGIN;

\ir 01_merge.sql
\ir 02_sellers_profile.sql
\ir 03_seller_competitors.sql

COMMIT;

\ir 04_refresh_views.sql
\ir 05_cleanup.sql
