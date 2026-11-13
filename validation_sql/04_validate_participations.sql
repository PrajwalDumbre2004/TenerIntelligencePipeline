-- validation_sql/04_validate_participations.sql
--
-- Participation-level validation. Must run AFTER 03_validate_tenders.sql in
-- the same transaction -- one of its checks (`no_validated_parent_tender`)
-- depends on that step's `staging.tenders_raw.validated` already being set
-- for this batch.
--
-- Unlike tenders, a bad participation row is NOT left in place with a flag
-- for pipeline4 to notice -- pipeline4/sql/01_merge.sql was never taught to
-- check one, and this project isn't touching that file. Instead a bad row is
-- physically moved: inserted into staging.bid_participations_quarantine, then
-- deleted from staging.bid_participations_raw. Once it's gone from
-- bid_participations_raw, 01_merge.sql's `JOIN staging.bid_participations_raw`
-- simply never sees it -- the tender and its other (valid) participants merge
-- normally, untouched.
--
-- Scope: rows not yet merged and not yet judged (`validated_at IS NULL`).
-- Assumes -- as pipeline3/derive.py's derivation model guarantees -- that all
-- participants for one bid_id are bulk-inserted together in a single pass, so
-- the first validation run after derivation sees a bid's complete participant
-- set at once. This matters for the cross-row checks below (one bad-winner
-- row invalidates the group, not the bid).

DROP TABLE IF EXISTS bad_participations;
CREATE TEMP TABLE bad_participations AS
WITH pending AS (
    SELECT *
    FROM staging.bid_participations_raw
    WHERE merged_at IS NULL AND validated_at IS NULL
),
-- winner_id/winner_name/winner_price are stamped onto EVERY row of a bid by
-- extract_participants() (pipeline3/extract.py) -- they identify who the
-- bid's actual winner is, and should agree across every row of that bid.
-- Checked against real staged data before picking this shape: an earlier
-- draft flagged "more than one is_winner = TRUE row per bid" as corruption,
-- but on this project's real GeM data that fires on ~80% of rows -- GeM's
-- financial_evaluation payload marks every seller who was L1 on ANY line
-- item of a multi-item bid as is_winner = TRUE, while winner_id/winner_name
-- only ever capture the FIRST such seller extract_participants() encounters.
-- That's expected multi-item behavior, not a data defect, so it must not be
-- quarantined. The actual defect is narrower and verified against real
-- data (19,638 rows on this dataset): is_winner = TRUE on a row whose OWN
-- seller_id does not match the bid's recorded winner_id -- i.e. this
-- specific row's winner flag disagrees with who the bid says actually won.
checks AS (
    SELECT
        p.run_id, p.batch_key, p.bid_id, p.seller_id,
        ARRAY_REMOVE(ARRAY[
            -- The rule this pipeline was built around: GeM sometimes renders
            -- a seller's name as a run of masking characters (most commonly
            -- asterisks) instead of the real name. A name may legitimately
            -- START with a special/punctuation character (e.g. "(P) ABC
            -- TRADERS", "M/S. XYZ") -- the defect is a name with NO
            -- alphanumeric content anywhere in it, not merely an unusual
            -- first character.
            CASE
                WHEN btrim(p.seller_name) = ''            THEN 'empty_seller_name'
                WHEN btrim(p.seller_name) ~ '^\*+$'        THEN 'masked_seller_name_asterisks'
                WHEN btrim(p.seller_name) !~ '[A-Za-z0-9]' THEN 'masked_seller_name_no_alnum'
            END,
            CASE WHEN p.total_price IS NOT NULL AND p.total_price < 0
                 THEN 'negative_price' END,
            CASE WHEN p.is_winner AND (p.total_price IS NULL OR p.total_price <= 0)
                 THEN 'winner_missing_price' END,
            -- Only cast the digits out of `rank` once the regex has already
            -- confirmed they ARE digits -- CASE evaluates branches in order
            -- and only the matched THEN, so this cast never runs on a rank
            -- string that wouldn't survive it.
            CASE WHEN p.rank ~ '^L[0-9]+$'
                      AND p.rank_num IS DISTINCT FROM substring(p.rank FROM 2)::int
                 THEN 'rank_num_mismatch' END,
            CASE WHEN p.outcome NOT IN ('won', 'lost', 'disqualified', 'no_result')
                 THEN 'invalid_outcome' END,
            -- Row-level: this seller is flagged is_winner but isn't who the
            -- bid recorded as its winner. p.winner_id is '' (not NULL) when
            -- no winner was determined at all, so a real winner_id is
            -- required before treating a mismatch as a defect.
            CASE WHEN p.is_winner AND p.winner_id <> '' AND p.seller_id <> p.winner_id
                 THEN 'winner_flag_seller_mismatch' END,
            -- This bid_id's tender either isn't in this batch at all or
            -- failed 03_validate_tenders.sql -- this row could never be
            -- merged by 01_merge.sql anyway (it joins on
            -- `batch_tenders` = validated tenders only). Quarantining it here
            -- gives it a paper trail instead of leaving it stuck in
            -- staging.bid_participations_raw with merged_at forever NULL and
            -- no record of why.
            CASE WHEN NOT EXISTS (
                     SELECT 1 FROM staging.tenders_raw t
                     WHERE t.run_id = p.run_id AND t.bid_id = p.bid_id AND t.validated
                 )
                 THEN 'no_validated_parent_tender' END
        ], NULL) AS bad_codes
    FROM pending p
)
SELECT run_id, batch_key, bid_id, seller_id,
       (SELECT jsonb_agg(jsonb_build_object('code', code, 'severity', 'hard'))
        FROM unnest(bad_codes) AS code) AS validation_errors
FROM checks
WHERE array_length(bad_codes, 1) > 0;

-- Move quarantined rows out (insert first, then delete -- both scoped to the
-- exact (run_id, bid_id, seller_id) keys just computed above, so nothing
-- outside this batch's pending set is touched).
INSERT INTO staging.bid_participations_quarantine (
    run_id, batch_key, bid_id, seller_id, seller_name, mse_status, rank, rank_num,
    total_price, is_winner, winner_id, winner_name, winner_price, technical_status,
    outcome, end_date, derived_at, validation_errors, quarantined_at
)
SELECT
    bp.run_id, bp.batch_key, bp.bid_id, bp.seller_id, bp.seller_name, bp.mse_status,
    bp.rank, bp.rank_num, bp.total_price, bp.is_winner, bp.winner_id, bp.winner_name,
    bp.winner_price, bp.technical_status, bp.outcome, bp.end_date, bp.derived_at,
    bad.validation_errors, now()
FROM staging.bid_participations_raw bp
JOIN bad_participations bad
    ON bad.run_id = bp.run_id AND bad.bid_id = bp.bid_id AND bad.seller_id = bp.seller_id
ON CONFLICT (run_id, bid_id, seller_id) DO NOTHING;

DELETE FROM staging.bid_participations_raw bp
USING bad_participations bad
WHERE bp.run_id = bad.run_id AND bp.bid_id = bad.bid_id AND bp.seller_id = bad.seller_id;

-- Everything still pending at this point passed -- the bad rows are already
-- gone. Stamp them so the next run doesn't re-check them.
UPDATE staging.bid_participations_raw
SET validated_at = now()
WHERE merged_at IS NULL AND validated_at IS NULL;

DROP TABLE IF EXISTS bad_participations;
