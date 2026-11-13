# Migration Plan (Final) — Incremental, In-Place Implementation

## Scope & Approach

This supersedes the "brand-new module tree" framing in `migration_plan.md` (`common/`, `validate/`, `merge/`, `aggregates/`, `cleanup/` as new top-level packages). The actual implementation will **modify the existing `pipeline2/`, `pipeline3/`, `pipeline4/`, `pipeline_main/` packages in place**, rolled out **step by step**, not built as a parallel pipeline that later replaces the current one.

The only genuinely new shared infrastructure is a small `common/` module for the Postgres connection and the run/step logging tables — everything else (staging writes, validation, merge, aggregates, cleanup) is added as new files *inside* the existing packages, or as edits to existing files there.

The rest of this document describes:
1. The exact per-month sequence that the pipeline will follow, and which existing file implements each step.
2. The rule that this entire sequence runs to completion for one month before the next month is even fetched.
3. How impacted-seller recomputation (today's pipeline4 Phase 2/3 behavior) fits into this sequence.
4. The step-by-step rollout order for implementing the changes.

---

## 1. The Per-Month Sequence

Each month (`batch_key`, e.g. `"2024-03"`) goes through the following steps, in order, driven by `pipeline_main/run.py`:

### Step 1 — Start the run
```
pipeline_main/run.py picks a month → batch_key = "2024-03", calls start_run(batch_key) → gets a run_id.
```
- `start_run(batch_key)` lives in `common/logging_db.py` (new). It inserts a row into `pipeline_runs` and commits immediately — the record of "this batch was attempted" exists even if everything after it fails.
- `run_id` (UUID) is threaded through every subsequent step for this month.

### Step 2 — Extract
```
Extract: pipeline2/3 write/enrich rows in staging.tenders_raw tagged (run_id, batch_key) —
scoped to just that month's GeM date range, same as today.
```
- **Files modified**: `pipeline2/db.py`, `pipeline3/db.py` (and their `config.py`s for the Postgres DSN).
- Scraping/enrichment logic itself is **unchanged** — same multi-pass convergence in pipeline2, same result/RA/contract fetching in pipeline3. Only the write target changes: `bulk_upsert_tenders()` and `update_tender_results()` now write to `staging.tenders_raw` keyed on `(run_id, bid_id)`, tagged with `batch_key`, instead of Mongo's `tenders_YYYY`.
- Extract is complete for the month when: pipeline2's numFound convergence check passes AND pipeline3's `scraped_at IS NOT NULL` count matches the batch total — identical completion logic to today, just checked against `staging.tenders_raw WHERE run_id = $1`.

### Step 3 — Validate
```
Validate: validate_batch(run_id, batch_key) only touches rows matching that pair.
```
- **File added**: `pipeline4/validate.py` (new file inside the existing `pipeline4` package).
- `validate_batch(run_id, batch_key) -> ValidationReport` runs checks scoped strictly to `staging.tenders_raw WHERE run_id = $1 AND batch_key = $2` — never touches other months' or other runs' staging rows.
- Writes per-row `validation_errors` and flips `validated = TRUE` on rows that pass. Today this is a stub that marks everything `validated = TRUE`; real rules get added later without touching anything downstream.
- If the batch fails validation outright (hard-fail policy), the run stops here: `finish_run(run_id, "failed")`, staging rows are left in place for inspection/retry, nothing is merged.

### Step 4 — Merge (atomic)
```
Merge: the transaction's WHERE run_id = $1 AND batch_key = $2 AND validated clause is what makes it
a bounded atomic operation — one month's worth of rows, not the whole table.
```
- **File added**: `pipeline4/merge.py` (new file inside `pipeline4`, takes over the write responsibilities currently split between `pipeline4/db.py` and the inline block in `pipeline4/orchestrator.py`).
- One transaction:
  ```sql
  BEGIN;
    INSERT INTO tenders (...)
    SELECT ... FROM staging.tenders_raw
    WHERE run_id = $1 AND batch_key = $2 AND validated
    ON CONFLICT (bid_id, tender_year) DO UPDATE SET ...;

    -- extract_participants() per tender (pipeline4/extract.py, unchanged), then:
    INSERT INTO bid_participations (...)
    VALUES ...
    ON CONFLICT (seller_id, bid_id, participation_year) DO UPDATE SET ...;

    UPDATE staging.tenders_raw SET merged_at = now()
    WHERE run_id = $1 AND batch_key = $2 AND validated;
  COMMIT;
  ```
- The `WHERE run_id = $1 AND batch_key = $2` clause is what bounds this transaction to one month's rows — not a fresh scan of the whole `staging.tenders_raw` table, and not a transaction spanning the entire multi-year history. If anything in this block fails, Postgres rolls back the whole thing; staging rows for this run stay untouched (`merged_at IS NULL`) and the batch can simply be retried.
- Returns the set of `bid_id`s merged and the set of distinct `seller_id`s written into `bid_participations` — this becomes `impacted_sellers` for the next step.

### Step 5 — Aggregates
```
Aggregates: impacted_sellers = only sellers found in this batch's new bid_participations rows.
```
- **Files modified**: `pipeline4/orchestrator.py` (drives this step), `pipeline4/builders.py` (keeps `build_seller_profile`, drops `build_seller_buyers_docs`/`build_seller_categories_docs`), `pipeline4/db.py` (read/write helpers for `sellers`/`seller_competitors`), plus a new `pipeline4/refresh_views.py` for the two materialized views.
- Scoped strictly to `impacted_sellers` from Step 4 — never all ~306K sellers. See Section 3 below for the full recompute logic.

### Step 6 — Cleanup
```
Cleanup: DELETE FROM staging.tenders_raw WHERE run_id = $1 — scoped to the same run,
leaving other months' (or in-flight) staging rows untouched.
```
- **File added**: `pipeline4/cleanup.py` (or a function in `pipeline_main/run.py` — small enough either way).
- Deletes rows, never drops the `staging` schema or table — the table persists across every run.
- Runs immediately after the merge transaction (Step 4) commits — it does **not** wait for Step 5 (aggregates) to finish, since aggregates only ever reads from `bid_participations`/`tenders` in `public`, never from staging. A slow or failing aggregates step must not hold staging cleanup hostage.
- (Optional hardening, not required for correctness: keep merged rows for a short retention window before hard-deleting, as a replay/audit buffer, purged by a separate scheduled job.)

---

## 2. One Month Fully, Then the Next

**The entire sequence above (Start → Extract → Validate → Merge → Aggregates → Cleanup) runs to completion for a given month before the next month is even fetched.** This is not a new constraint — it's exactly how `pipeline_main/run.py` already behaves today (it currently runs pipeline2 → pipeline3 → pipeline4 fully for one month before moving to the next in its `for i, month in enumerate(months)` loop). The only change is that the loop body now drives six steps instead of three, with validation and an atomic merge inserted between extraction and the analytics recompute.

Concretely, `pipeline_main/run.py`'s per-month block becomes:

```
for month in months:
    run_id = start_run(month)
    try:
        with step(run_id, "extract"):   pipeline2 + pipeline3 write to staging (scoped to run_id, month)
        with step(run_id, "validate"):
            report = pipeline4.validate.validate_batch(run_id, month)
            if not report.passed: finish_run(run_id, "failed"); continue   # next month is NOT started early
        with step(run_id, "merge"):      result = pipeline4.merge.run_merge(run_id, month)
        with step(run_id, "aggregates"): pipeline4.orchestrator.run_aggregates(result.impacted_sellers)
        with step(run_id, "cleanup"):    pipeline4.cleanup.purge_run(run_id)
        finish_run(run_id, "succeeded")
    except Exception as e:
        finish_run(run_id, "failed")
        # month is retried on next invocation; later months are still not started early
```

A month is never partially started while an earlier month is still in flight, and a later month never begins before the current one reaches `succeeded` or `failed` — matching the current pipeline's strictly sequential month processing (a deliberate trade-off already documented in `DataExtractionPipeline.md`: "Processing order: Sequential month-by-month... chosen for stability").

---

## 3. Recomputing Impacted Sellers

`impacted_sellers` (from Step 4's merge) drives Step 5 end-to-end. This preserves today's pipeline4 Phase 2/3 behavior — recompute only what changed, not the whole seller universe — just re-pointed at the new tables.

1. **`sellers` profile table** — for each seller in `impacted_sellers`:
   ```sql
   SELECT bp.*, t.category, t.buyer_name, t.month
   FROM bid_participations bp
   JOIN tenders t USING (bid_id)
   WHERE bp.seller_id = ANY($impacted_sellers)
   ```
   Feed the result into the existing `build_seller_profile()` logic (unchanged `Counter`-based top-10/win-rate/first-seen/last-seen computation) — only the fetch query changes, from a Mongo `find()` to this join, since `category`/`buyer_name`/`month` no longer live on the participation row itself.

2. **`seller_competitors` — two-pass, same shape as today's Phase 3**:
   - **Pass 1**: for every seller in `impacted_sellers`, recompute their full competitor list via a self-join over their *entire* participation history (not just this batch):
     ```sql
     SELECT bp1.seller_id, bp2.seller_id AS competitor_id, COUNT(*) AS shared_tenders
     FROM bid_participations bp1
     JOIN bid_participations bp2 USING (bid_id)
     WHERE bp1.seller_id = ANY($impacted_sellers) AND bp2.seller_id <> bp1.seller_id
     GROUP BY bp1.seller_id, bp2.seller_id
     ```
     This one query replaces today's chunked Mongo `$in` loop (`get_seller_bids_for_competitors` + `get_co_participants_for_bids`) entirely, and can run once for the whole impacted set instead of per-seller.
   - **Pass 2**: the competitors discovered in Pass 1 who are *not themselves* in `impacted_sellers` also get recomputed, so both sides of every affected `(seller_id, competitor_id)` pair stay consistent — identical to today's co-participant expansion in `pipeline4/orchestrator.py`'s `recompute_competitors()`.
   - Both passes upsert into `seller_competitors(seller_id, competitor_id, shared_tenders)`.

3. **Materialized views (`seller_categories`, `seller_buyers`) — not scoped to `impacted_sellers` at all**:
   - `REFRESH MATERIALIZED VIEW CONCURRENTLY` always recomputes the *entire* view — there is no per-seller partial refresh. The incremental-recompute optimization used for `sellers`/`seller_competitors` does not apply here.
   - Options: refresh after every month's merge (simplest, matches today's cadence, but a full recompute over 4.6M/9M rows on every single month may be wasteful), or decouple the refresh cadence from individual batch merges (e.g. once per `pipeline_main` invocation, or on a periodic schedule) — a tuning decision, not a correctness one.

4. **Ordering**: `sellers → seller_competitors → materialized views`, run sequentially. None of the three depends on another's output (all three read only from the already-committed `bid_participations`/`tenders`), so this is a chosen simplicity/predictability trade-off, not a requirement.

---

## 4. Step-by-Step Rollout Order

Since the current pipeline is being modified in place rather than replaced wholesale, implement in this order so the system stays runnable at each step:

1. Create `staging` schema + `staging.tenders_raw` table, and `pipeline_runs`/`pipeline_step_log` in `public`.
2. Add `common/db.py` (Postgres connection) and `common/logging_db.py` (`start_run`/`log_step`/`finish_run`).
3. Rewrite `pipeline2/db.py` + `pipeline2/config.py` to write into `staging.tenders_raw` — verify pipeline2 alone still runs correctly against staging.
4. Rewrite `pipeline3/db.py` + `pipeline3/config.py` to read/update the same staging rows — verify pipeline2 → pipeline3 together populate a fully-enriched staging batch.
5. Add `pipeline4/validate.py` as a pass-through stub (`validated = TRUE` for everything) — establishes the seam without blocking anything yet.
6. Add `pipeline4/merge.py` (the atomic transaction) — verify a full month merges correctly into `tenders` + `bid_participations` and staging rows get `merged_at` set.
7. Update `pipeline4/orchestrator.py` + `builders.py` + new `refresh_views.py` to run the aggregates step scoped to `impacted_sellers` from the merge result.
8. Add `pipeline4/cleanup.py` and wire it in right after merge commits (not after aggregates).
9. Update `pipeline_main/run.py` to drive the six-step per-month sequence from Section 1, keeping the existing strictly-sequential month loop.
10. Only after the above is verified end-to-end on a real month: decommission the old Mongo-writing code paths.
</content>
