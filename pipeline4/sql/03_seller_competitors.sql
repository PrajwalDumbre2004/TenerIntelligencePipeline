-- pipeline4/sql/03_seller_competitors.sql
--
-- Step D3 (pipeline4_next_steps.md): recompute `seller_competitors` for every
-- seller in `impacted_sellers` (populated by 01_merge.sql, same session), then
-- propagate the change to their competitors -- the "second pass" from
-- migration_plan_final.md Section 3.
--
-- Pass 1 -- direct impacted sellers:
--   Full recompute (entire bid history, not just this batch) of every impacted
--   seller's competitor list via a self-join on shared bid_id. If both sides
--   of a pair are impacted, this pass alone produces both directions
--   correctly (it runs once with bp1.seller_id = A, once with bp1.seller_id = B).
--
-- Pass 2 -- their competitors who were NOT themselves impacted:
--   shared_tenders(A, B) is symmetric -- "how many bids did A and B both
--   participate in" doesn't depend on which one you count from. Pass 1 already
--   computed that number for every (impacted, competitor) pair, so Pass 2 just
--   mirrors those rows (swaps seller_id/competitor_id) into the reverse
--   direction for whichever competitor wasn't itself impacted -- no second
--   self-join over bid_participations needed.
--   This only ever writes to seller_competitors, never back into `sellers` --
--   a non-impacted competitor's own tender history didn't change, so their
--   profile card in 02_sellers_profile.sql has nothing to recompute.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY THIS FILE IS CHUNKED (and why the naive one-statement version cannot run)
-- ─────────────────────────────────────────────────────────────────────────────
-- The self-join fans out to sum(n * (n-1)) rows over every bid an impacted
-- seller touched. Measured on current production data:
--
--     bid_participations            39.8 M rows / 3.08 M bids / 307 K sellers
--     impacted sellers per batch    ~11.6 K  (3.8% of sellers, but 55% of all
--                                             participation rows -- these are
--                                             the mega-active accounts)
--     pass-1 intermediate rows      ~5.5 BILLION
--     planner tuple width           52 B  ->  ~374 GB of hash-aggregate spill
--
-- As a single statement that spills ~374 GB into base/pgsql_tmp and dies with
-- "No space left on device". Chunking works because Postgres releases a
-- statement's temp spill files at END OF STATEMENT: 200 sellers at a time caps
-- peak spill at the worst single chunk instead of the whole job.
--
-- Measured worst-chunk fan-out across candidate chunk sizes:
--
--     chunk  chunks   worst rows      mean rows     skew   worst spill
--        25     458   46,700,813     12,014,601    3.89x       ~3.2 GB
--        50     229   69,496,464     24,029,203    2.89x       ~4.7 GB
--       100     115   95,882,864     47,849,457    2.00x       ~6.5 GB
--       200      58  154,715,585     94,873,924    1.63x      ~10.5 GB  <- chosen
--       500      23  317,801,903    239,247,287    1.33x      ~21.6 GB
--
-- Peak does NOT scale linearly with chunk size, because skew rises as chunks
-- shrink. Per-seller fan-out is savagely skewed (median 7.9 K rows, p99 6.8 M,
-- worst single seller 20 M), and a big chunk dilutes the whales while a small
-- one cannot. So 200 -> 50 is a 4x cut in sellers but only a 2.2x cut in peak.
-- The hard floor is that worst single seller: 20 M rows / ~1.4 GB, unreachable
-- at any chunk size.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY 200 AND NOT SMALLER -- smaller chunks cost WALL-CLOCK, not just rounds
-- ─────────────────────────────────────────────────────────────────────────────
-- 50 was tried and reverted. Total ROW work is identical at any chunk size --
-- chunking partitions sellers, it never duplicates them, so all 5.5 B
-- intermediate rows get computed exactly once either way. What changes is fixed
-- per-chunk overhead (planning, executor + bitmap setup, HashAggregate spin-up),
-- and at 229 rounds instead of 58 that overhead dominates. Timed on chunk 0
-- with the real bp_narrow in place:
--
--     chunk  join plan chosen              chunk-0    pairs out    throughput
--        50  Nested Loop + Bitmap Heap      187.9s      289,216   1,539 pair/s
--       200  Merge Join                     222.1s    1,016,686   4,577 pair/s
--
--     extrapolated full job:  50 -> ~717 min      200 -> ~215 min
--
-- ~3x worse throughput at 50, to save ~5.8 GB of peak spill. Not worth it: 200's
-- ~10.5 GB is already a 38x cut from the ~374 GB that failed. Drop to 100
-- (~6.5 GB) only if a cloud instance genuinely cannot spare 10.5 GB of temp
-- headroom -- and time it first, the same way, rather than assuming it lands
-- between the two.
--
-- Note the plan flips with chunk size: 50 nested-loops through the
-- (seller_id, bid_id) index, 200 merge-joins on bid_id. BOTH are index-driven
-- and fine. The failure mode to actually watch for is a Hash Join over a SEQ
-- SCAN of bp_narrow -- that would re-scan ~35 M rows per chunk (58 x 35 M) and
-- is what the two indexes below exist to prevent. Worth an EXPLAIN if this step
-- ever slows down sharply after a stats drift or a chunk-size change.
--
-- `bp_narrow` exists because chunking ALONE would make things worse. The
-- original plan seq-scans all 39.8 M rows of bid_participations (Append over 7
-- wide partitions) to build the probe side. Run that per chunk and you pay it
-- 58 times. So we project (bid_id, seller_id) ONCE and let every chunk probe
-- that instead.
--
-- Be clear about what bp_narrow does and does not buy. It does NOT prune many
-- rows: the impacted sellers are the mega-active accounts and collectively
-- touch nearly every bid, so bp_narrow measures 35,485,905 rows of the 39.8 M
-- source -- barely a reduction -- and 4.5 GB on disk including its two indexes,
-- taking ~130 s to build. What it buys is a 2-column tuple instead of a
-- 34-byte one, and -- the real point -- the
-- two indexes below, which let each chunk reach its rows by index instead of
-- scanning the lot.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY THE OUTPUT IS BYTE-IDENTICAL TO THE UNCHUNKED VERSION
-- ─────────────────────────────────────────────────────────────────────────────
-- 1. No group is ever split. The GROUP BY key is (bp1.seller_id, competitor_id)
--    and chunking partitions on bp1.seller_id, so every group lands entirely
--    inside exactly one chunk. There are no partial aggregates to reconcile --
--    this is the whole reason we chunk by SELLER and not by bid.
--
-- 2. bp_narrow loses no join input. bp1 rows need seller_id impacted, and every
--    such row's bid_id is in touched_bids by construction. bp2 rows need to
--    share a bid with a bp1 row, and touched_bids keeps ALL participants of
--    every touched bid. Rows on untouched bids could never have matched.
--
-- 3. bp_narrow does NOT de-duplicate. It is a plain projection, no DISTINCT --
--    row multiplicity is preserved exactly, so COUNT(*) is unchanged. See the
--    note at touched_bids below: this deliberately preserves an existing
--    double-count edge case rather than silently "fixing" it here.
--
-- 4. Pass 1 and Pass 2 write to disjoint key spaces, so running them per chunk
--    is equivalent to running them once at the end:
--        Pass 1 writes (A, B) where A IS impacted
--        Pass 2 writes (B, A) where B is NOT impacted
--    No pass-1 row can collide with a pass-2 row, and each pair is produced by
--    exactly one chunk (the one holding its impacted seller). Pass 2 still
--    filters against the FULL impacted_sellers set, never the current chunk.
--
-- 5. refreshed_at is identical across chunks. now() is the TRANSACTION
--    timestamp, not a per-statement clock, and 00_run_all.sql wraps 01/02/03 in
--    one transaction -- so every row gets the same value it would have before.
--
-- 6. ON CONFLICT stays safe. Postgres rejects an INSERT whose own rows conflict
--    twice on the same key. Pass 1 keys come straight off a GROUP BY on exactly
--    those two columns, and Pass 2 is that same set with the columns swapped --
--    both unique within a chunk, exactly as in the unchunked version.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- No BEGIN/COMMIT here -- 00_run_all.sql wraps this file together with
-- 01_merge.sql and 02_sellers_profile.sql in a single outer transaction. A
-- COMMIT here would commit that outer transaction early. This also means the
-- ~59 chunks all run inside ONE transaction: chunking bounds temp SPILL, but it
-- cannot release WAL, which is pinned until the outer COMMIT. If pg_wal becomes
-- the new bottleneck on a managed instance, the fix is to let this step commit
-- per chunk -- safe in principle because it is a pure recompute from
-- already-committed data and re-running it over the full impacted set restores
-- correctness -- but that requires promoting impacted_sellers out of a TEMP
-- table (temp tables die with the session, taking the resume point with them)
-- and giving up the all-or-nothing guarantee documented in 00_run_all.sql.

-- Tunable. 4MB (the default here) makes each chunk's HashAggregate re-partition
-- recursively; a modest bump is the difference between 59 fast chunks and an
-- all-night run. Keep it well within the instance's RAM -- with
-- max_parallel_workers_per_gather = 2, budget 3x this value per chunk.
SET LOCAL work_mem = '64MB';

-- Guardrail, sized ~2x the 10.5 GB worst measured chunk. Without this a
-- pathological future batch fills the data volume and takes the instance down;
-- with it, the statement fails cleanly on "temporary file size exceeds
-- temp_file_limit" and rolls the batch back. Re-size it if you change the
-- chunk size -- see the table in the header.
SET LOCAL temp_file_limit = '20GB';

-- ── Narrow scan table ────────────────────────────────────────────────────────
-- Bids that at least one impacted seller participated in. Everything else in
-- bid_participations is unreachable from this batch's join and is dropped here
-- once rather than re-scanned 58 times.
DROP TABLE IF EXISTS touched_bids;
CREATE TEMP TABLE touched_bids AS
SELECT DISTINCT bp.bid_id
FROM bid_participations bp
JOIN impacted_sellers i USING (seller_id);

CREATE INDEX ON touched_bids (bid_id);
ANALYZE touched_bids;

-- The projection both sides of the self-join read from. Deliberately NOT
-- DISTINCT: bid_participations' unique key is
-- (seller_id, bid_id, participation_year), so one seller CAN hold the same
-- bid_id under two participation_years, and the unchunked version -- which
-- joins USING (bid_id) only -- counts that pair twice. Preserving the exact
-- old output means preserving that behaviour. If those duplicates turn out to
-- be real data errors, fix them in validation, not here, or this step will
-- start disagreeing with the version it replaced.
DROP TABLE IF EXISTS bp_narrow;
CREATE TEMP TABLE bp_narrow AS
SELECT bp.bid_id, bp.seller_id
FROM bid_participations bp
JOIN touched_bids tb USING (bid_id);

-- Two indexes, one per side of the self-join. Both are built once and used by
-- all 58 chunks, so the ~130 s build cost amortises immediately:
--   (bid_id)             -- bp2, the probe side: fetch every participant of a bid
--   (seller_id, bid_id)  -- bp1, the driving side: fetch one chunk's 200 sellers
-- They are load-bearing, not an optimisation -- see the plan note in the header
-- for the seq-scan failure mode they prevent.
CREATE INDEX ON bp_narrow (bid_id);
CREATE INDEX ON bp_narrow (seller_id, bid_id);
ANALYZE bp_narrow;

-- ── Chunk assignment ─────────────────────────────────────────────────────────
-- The 200 below is THE tuning knob for this file: it trades peak temp spill
-- against iteration count, and iteration count turned out to dominate runtime.
-- See the two tables in the header for measured spill AND measured throughput
-- at 25/50/100/200/500 before changing it, and re-size temp_file_limit above to
-- match. Lowering it is NOT free -- 50 was ~3x slower.
--
-- ORDER BY seller_id is arbitrary but deterministic, which is what makes a
-- failed run reproducible. It also keeps the skew as flat as it can be at this
-- size: seller_id order is uncorrelated with fan-out, so each chunk gets a
-- representative mix of whales and long tail rather than clustering the whales.
-- Do not "improve" this by ordering on fan-out -- that would pack the whales
-- into the same chunks and spike the peak this file exists to bound.
DROP TABLE IF EXISTS impacted_chunked;
CREATE TEMP TABLE impacted_chunked AS
SELECT seller_id,
       ((row_number() OVER (ORDER BY seller_id)) - 1) / 200 AS chunk_no
FROM impacted_sellers;

CREATE INDEX ON impacted_chunked (chunk_no);
ANALYZE impacted_chunked;

-- Created once and TRUNCATEd per chunk rather than DROP/CREATEd. TRUNCATE keeps
-- the relation OID stable, so plpgsql's cached plans below stay valid (a
-- drop/recreate cycle invalidates them and throws "cache lookup failed for
-- relation"), while still handing the space back each iteration.
DROP TABLE IF EXISTS pass1_pairs;
CREATE TEMP TABLE pass1_pairs (
    seller_id       TEXT,
    competitor_id   TEXT,
    shared_tenders  BIGINT   -- COUNT(*) is bigint; narrows to INT on insert,
                             -- same implicit cast the CTAS version relied on.
);

-- ── Chunked recompute ────────────────────────────────────────────────────────
DO $$
DECLARE
    max_chunk INT;
    c         INT;
BEGIN
    SELECT max(chunk_no) INTO max_chunk FROM impacted_chunked;

    IF max_chunk IS NULL THEN
        RAISE NOTICE 'seller_competitors: no impacted sellers, nothing to do';
        RETURN;
    END IF;

    FOR c IN 0..max_chunk LOOP
        TRUNCATE pass1_pairs;

        INSERT INTO pass1_pairs (seller_id, competitor_id, shared_tenders)
        SELECT bp1.seller_id, bp2.seller_id, COUNT(*)
        FROM bp_narrow bp1
        JOIN bp_narrow bp2 USING (bid_id)
        WHERE bp1.seller_id IN (
                  SELECT seller_id FROM impacted_chunked WHERE chunk_no = c)
          AND bp2.seller_id <> bp1.seller_id
        GROUP BY bp1.seller_id, bp2.seller_id;

        -- Pass 1: this chunk's impacted sellers' full competitor lists.
        INSERT INTO seller_competitors (seller_id, competitor_id, shared_tenders, refreshed_at)
        SELECT seller_id, competitor_id, shared_tenders, now()
        FROM pass1_pairs
        ON CONFLICT (seller_id, competitor_id) DO UPDATE SET
            shared_tenders = EXCLUDED.shared_tenders,
            refreshed_at   = EXCLUDED.refreshed_at;

        -- Pass 2: mirror the reverse-direction row for competitors who weren't
        -- themselves impacted this batch. Filters against the FULL impacted set
        -- (impacted_sellers), NOT this chunk -- a competitor sitting in a later
        -- chunk gets its own correct row from that chunk's Pass 1, and must not
        -- be mirrored into here.
        INSERT INTO seller_competitors (seller_id, competitor_id, shared_tenders, refreshed_at)
        SELECT competitor_id, seller_id, shared_tenders, now()
        FROM pass1_pairs
        WHERE competitor_id NOT IN (SELECT seller_id FROM impacted_sellers)
        ON CONFLICT (seller_id, competitor_id) DO UPDATE SET
            shared_tenders = EXCLUDED.shared_tenders,
            refreshed_at   = EXCLUDED.refreshed_at;

        RAISE NOTICE 'seller_competitors: chunk %/% done', c, max_chunk;
    END LOOP;
END $$;

-- Hand back the scratch space before leaving. bp_narrow plus its two indexes
-- measures 4.5 GB at current volumes, and the very next step (04_refresh_views.sql)
-- does a CONCURRENTLY rebuild of two multi-million-row materialized views, which
-- wants that room. These are TEMP tables so they would die with the psql session
-- anyway -- but that is only after 04 has run, which is too late to help it.
DROP TABLE IF EXISTS pass1_pairs;
DROP TABLE IF EXISTS bp_narrow;
DROP TABLE IF EXISTS touched_bids;
DROP TABLE IF EXISTS impacted_chunked;
