-- validation_sql/01_helpers.sql
--
-- staging.tenders_raw.end_date / staging.bid_participations_raw.end_date are
-- both TEXT (pipeline2 and pipeline3 store the raw scraped string, matching
-- pipeline4_next_steps.md's note that the TEXT -> DATE cast happens later).
-- A plain `end_date::date` cast throws for any row where the string isn't a
-- valid date -- and because validation runs as one set-based UPDATE over many
-- rows at once, a single bad row would abort the ENTIRE statement, not just
-- fail that one row. staging.safe_to_date() swallows the cast error and
-- returns NULL instead, so a garbage date shows up as "this row's date is
-- unparseable" (one flagged row) rather than crashing validation for the
-- whole batch.
--
-- IMMUTABLE is safe here: the function's output depends only on its input,
-- not on session state, so the planner is free to reuse/inline it.
CREATE OR REPLACE FUNCTION staging.safe_to_date(raw TEXT)
RETURNS DATE
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN raw::date;
EXCEPTION WHEN others THEN
    RETURN NULL;
END;
$$;
