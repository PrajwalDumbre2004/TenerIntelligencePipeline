# Pipeline4 — Remaining Work (Step C & Step D)

## Context

`pipeline3` now flattens every scraped tender into per-seller participation
rows immediately after scraping (see `pipeline3/extract.py`, `pipeline3/derive.py`),
writing them to `staging.bid_participations_raw`. That was **Step A/B** and is
already implemented.

What's left is making `pipeline4` consume that pre-flattened data purely at
the DB level — no Python-side transformation of tender/participant data
anywhere in pipeline4. That's **Step C** (merge) and **Step D** (aggregates),
described below. Neither has been implemented yet.

---

## Step C — Merge becomes a plain `INSERT ... SELECT`

**Files affected:** `pipeline4/merge.py` (new), `pipeline4/orchestrator.py`,
`pipeline4/extract.py` (retire), `pipeline4/db.py` (Mongo-era read/write helpers
to retire).

Today (per `migration_plan_final.md` Step 4), the plan still has
`extract_participants()` running *inside* pipeline4's merge transaction. That
should no longer be true — the flattening already happened in pipeline3.
`pipeline4/merge.py` should now just copy already-normalized rows into the
production tables, inside one transaction, scoped to `(run_id, batch_key)`:

```sql
BEGIN;

  -- 1. Tender metadata: staging.tenders_raw -> tenders (unchanged shape)
  INSERT INTO tenders (bid_id, bid_id_parent, bid_number, bid_number_parent,
                        ra_number, tender_year, category, department, ministry,
                        month, end_date, buyer_name, buyer_department, is_ra_bid,
                        scraped_at, bid_result_raw, ra_result_raw, buyer_details_raw)
  SELECT ...
  FROM staging.tenders_raw
  WHERE run_id = $1 AND batch_key = $2 AND validated
  ON CONFLICT (bid_id, tender_year) DO UPDATE SET ...;

  -- 2. Participations: staging.bid_participations_raw -> bid_participations
  --    Straight column copy. No Python, no extract_participants() call here.
  INSERT INTO bid_participations (seller_id, bid_id, participation_year,
                                   rank, rank_num, total_price, is_winner,
                                   winner_id, winner_name, winner_price,
                                   technical_status, outcome, seller_name,
                                   mse_status, end_date)
  SELECT seller_id, bid_id, EXTRACT(YEAR FROM end_date::date),
         rank, rank_num, total_price, is_winner,
         winner_id, winner_name, winner_price,
         technical_status, outcome, seller_name,
         mse_status, end_date::date
  FROM staging.bid_participations_raw
  WHERE run_id = $1 AND batch_key = $2
  ON CONFLICT (seller_id, bid_id, participation_year) DO UPDATE SET ...;

  -- 3. sellers table: ensure every seller_id seen has a row (profile fields
  --    get filled in properly by Step D's aggregate query)
  INSERT INTO sellers (seller_id, seller_name, mse_status)
  SELECT DISTINCT seller_id, seller_name, mse_status
  FROM staging.bid_participations_raw
  WHERE run_id = $1 AND batch_key = $2
  ON CONFLICT (seller_id) DO NOTHING;

  UPDATE staging.tenders_raw SET merged_at = now()
  WHERE run_id = $1 AND batch_key = $2 AND validated;

COMMIT;
```

Notes / open decisions to resolve when implementing:

- **`end_date` cast**: `staging.bid_participations_raw.end_date` is `TEXT`
  (mirrors `staging.tenders_raw.end_date`, which pipeline2 also stores as
  `TEXT`). The `tenders`/`bid_participations` schema wants `DATE`. Decide
  where the `TEXT -> DATE` cast happens — either in this merge query
  (`end_date::date`, shown above) or earlier in pipeline3's derive step. Doing
  it here keeps pipeline3 simpler and keeps date-parsing logic in one place
  (the merge step already needs to know the target schema's types).
- **`participation_year`**: not present in staging at all today — it's
  derived from `end_date`'s year at merge time (`EXTRACT(YEAR FROM ...)`).
  Confirm this matches how `tender_year` is derived for the `tenders` insert
  (should be the same source column, same logic, for consistency).
- **Return value**: `merge.py`'s function should still return the distinct
  `seller_id`s written in this batch (`impacted_sellers`), same contract as
  today's `pipeline4/orchestrator.py:process_new_tenders()` — Step D depends
  on this set to scope its recompute.
- Once `merge.py` is verified working, **retire** `pipeline4/extract.py` and
  the Mongo-era `pipeline4/db.py` functions it's no longer needed for
  (`discover_tender_collections`, `get_underived_tenders`, etc., if not
  already dead from an earlier Mongo decommission pass).

---

## Step D — Aggregates become real SQL, not Python `Counter` logic

**Files affected:** `pipeline4/builders.py` (retire `build_seller_profile`'s
Python computation), `pipeline4/orchestrator.py` (`recompute_seller_aggregates`,
`recompute_competitors`), `pipeline4/db.py`, new `pipeline4/refresh_views.py`.

Three things currently still run as Python loops and need to become SQL,
scoped to `impacted_sellers` from Step C where the schema allows it:

### D1 — `sellers` profile (replaces `build_seller_profile`)

Today this reads a seller's full history into a Python list and computes
win-rate / top-10 categories / top-10 buyers with `collections.Counter`. This
becomes one aggregate query per impacted seller (or one query with
`WHERE seller_id = ANY($impacted_sellers)` grouped by `seller_id`):

```sql
INSERT INTO sellers (seller_id, seller_name, mse_status, total_tenders,
                      total_wins, win_rate, total_contract_value,
                      avg_contract_value, top_categories, top_buyers,
                      first_seen, last_seen, updated_at)
SELECT
    bp.seller_id,
    MIN(bp.seller_name)  -- or "shortest name" tie-break, see note below
    ...
    COUNT(DISTINCT bp.bid_id)                       AS total_tenders,
    COUNT(DISTINCT bp.bid_id) FILTER (WHERE bp.is_winner) AS total_wins,
    ROUND(COUNT(*) FILTER (WHERE bp.is_winner) * 100.0 / COUNT(*), 1) AS win_rate,
    COALESCE(SUM(bp.total_price) FILTER (WHERE bp.is_winner), 0)      AS total_contract_value,
    ...
    -- top_categories / top_buyers: jsonb_agg with a per-seller top-10 filter,
    -- via a LATERAL subquery or ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY count DESC)
FROM bid_participations bp
JOIN tenders t USING (bid_id)
WHERE bp.seller_id = ANY($impacted_sellers)
GROUP BY bp.seller_id
ON CONFLICT (seller_id) DO UPDATE SET ...;
```

Open decision: `build_seller_profile`'s "shortest raw name wins" tie-break
for `seller_name` (see `pipeline4/builders.py:90-93`) has no single SQL
aggregate equivalent — needs `MIN(seller_name) OVER (ORDER BY length(seller_name))`
style logic, a `DISTINCT ON`, or just accepting `MIN(seller_name)` as a
close-enough approximation. Decide which before implementing; this is a
behavior change worth confirming against real data, not just a syntax swap.

### D2 — `seller_categories` / `seller_buyers` materialized views

Already fully designed in `Postgresql_schema.md` Section 5 — just needs
wiring up: create `pipeline4/refresh_views.py` with
`REFRESH MATERIALIZED VIEW CONCURRENTLY seller_categories;` and the same for
`seller_buyers`, called once per month-merge (or on a decoupled schedule —
see the trade-off already noted in `migration_plan_final.md` Section 3.3,
since a full-view refresh isn't scoped to `impacted_sellers`).

### D3 — `seller_competitors`

Replace `get_seller_bids_for_competitors` + `get_co_participants_for_bids` +
the Python per-seller loop in `pipeline4/orchestrator.py:recompute_competitors()`
with the two-pass self-join SQL already written in `migration_plan_final.md`
Section 3.2:

```sql
SELECT bp1.seller_id, bp2.seller_id AS competitor_id, COUNT(*) AS shared_tenders
FROM bid_participations bp1
JOIN bid_participations bp2 USING (bid_id)
WHERE bp1.seller_id = ANY($impacted_sellers) AND bp2.seller_id <> bp1.seller_id
GROUP BY bp1.seller_id, bp2.seller_id;
```

Pass 2 (competitors discovered here who aren't themselves in
`impacted_sellers`) still needs to run so both sides of every affected pair
stay consistent — same shape as today, just via this query instead of the
chunked loop.

---

## Definition of done for "pipeline4 is DB-only"

Both Step C and Step D need to land before that claim is actually true:
- C alone removes Python transformation from the *merge* path but leaves it in
  aggregates (D1's `Counter` logic).
- D alone has nothing to consume yet without C (there'd be no
  `staging.bid_participations_raw` → `bid_participations` copy happening).

They can be implemented in either order but both must land, and should be
verified end-to-end against a real staged month before `pipeline4/extract.py`
and the Mongo-era `pipeline4/db.py` helpers are deleted.
