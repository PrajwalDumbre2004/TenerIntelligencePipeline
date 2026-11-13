-- validation_sql/00_run_validate.sql
--
-- Standalone validation phase for staged data. Run this BEFORE
-- pipeline4/sql/00_run_all.sql, as a separate psql invocation, on the same
-- database. It never touches `public.tenders` / `public.bid_participations`
-- (read-only against `public`, if it reads it at all) and it never edits any
-- file under pipeline2/, pipeline3/, or pipeline4/ -- it only reads and writes
-- `staging.tenders_raw` / `staging.bid_participations_raw` (both already
-- written by pipeline2/pipeline3 today) plus one brand-new table this folder
-- owns (`staging.bid_participations_quarantine`).
--
-- Why it works with zero changes to pipeline4:
--   `staging.tenders_raw.validated` / `.validation_errors` already exist
--   (added by pipeline2/db.py) and `pipeline4/sql/01_merge.sql` already reads
--   them (`WHERE merged_at IS NULL AND validated`). This script is the first
--   thing that actually populates those columns with a real answer instead of
--   the pipeline's current `validated = TRUE` stub. Tenders that fail simply
--   stay `validated = FALSE` and are skipped by the merge, same as today's
--   contract -- no change needed on the merge side.
--
--   `staging.bid_participations_raw` has no `validated` column and
--   `01_merge.sql` never checks one -- it merges every participation row
--   belonging to a validated tender. Rather than adding a column pipeline4
--   would need to be taught to read (which means touching pipeline4), a bad
--   participation row (e.g. a masked "*****" seller name) is physically moved
--   out of `staging.bid_participations_raw` into
--   `staging.bid_participations_quarantine` before merge ever runs. The row
--   simply isn't there for `01_merge.sql`'s `JOIN staging.bid_participations_raw`
--   to pick up -- no new condition for it to check, no file to edit. The
--   tender itself, and its other (valid) participants, are untouched and
--   merge normally.
--
-- Usage:
--   `pipeline4/sql/00_run_all.sql` already `\ir`s this file, as its very
--   first step, before it opens its own merge transaction -- so a plain
--   `psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f pipeline4/sql/00_run_all.sql`
--   always validates this batch first automatically. You do not need to run
--   this file separately as part of the normal per-batch flow.
--
--   It's still fully runnable standalone -- e.g. to re-validate after fixing
--   upstream scraping/derivation bugs and re-deriving a batch, without also
--   running the merge in the same breath:
--     psql "$POSTGRES_DSN" -v ON_ERROR_STOP=1 -f validation_sql/00_run_validate.sql
--
-- All four steps run in ONE transaction: either this batch's validation
-- decisions (tenders flagged, bad participations quarantined) all land
-- together, or none of them do. 03/04 must not contain their own
-- BEGIN/COMMIT -- see pipeline4/sql/00_run_all.sql for why a nested COMMIT
-- would be wrong here too.
--
-- This transaction is deliberately SEPARATE from -- and always commits
-- before -- 00_run_all.sql's own merge transaction. If it commits and the
-- merge transaction that runs afterward later fails for an unrelated reason,
-- these validation decisions are not lost: re-running 00_run_all.sql skips
-- already-judged rows (03/04 both scope to "not yet judged") and retries only
-- the merge. And because `-v ON_ERROR_STOP=1` aborts the whole psql
-- invocation on the first error, a failure IN this file stops
-- 00_run_all.sql before it ever reaches its own `BEGIN;` -- the merge simply
-- never runs against a batch whose validation didn't finish cleanly.

BEGIN;

\ir 01_helpers.sql
\ir 02_schema.sql
\ir 03_validate_tenders.sql
\ir 04_validate_participations.sql

COMMIT;
